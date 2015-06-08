import os

import pytest

from wzdat.manifest import Manifest, RecursiveReference
from wzdat.util import get_notebook_dir, find_hdf_notebook_path,\
    get_notebook_manifest_path, iter_notebooks, iter_notebook_manifests,\
    get_data_dir, dataframe_checksum, HDF
from wzdat.ipynb_runner import update_notebook_by_run


@pytest.yield_fixture
def fxsoldir():
    sol_dir = os.environ['WZDAT_SOL_DIR']
    os.chdir(sol_dir)
    yield sol_dir


@pytest.yield_fixture
def fxhdftest2():
    with HDF('haje01') as hdf:
        test = hdf.store['test']
        if 'test2' not in hdf.store:
            hdf.store['test2'] = test
    yield None


@pytest.yield_fixture
def fxnewfile():
    # new file
    newfile = os.path.join(get_data_dir(), 'kr/node-3/log',
                           'game_2014-03-05 04.log')
    if os.path.isfile(newfile):
        os.unlink(newfile)

    yield newfile

    if os.path.isfile(newfile):
        os.unlink(newfile)
        # reload file infor for following tests
        from ws_mysol.myprj import log as l
        l.load_info()


def test_notebook_run():
    path = os.path.join(get_notebook_dir(),
                        '[* * * * *@Test]test-notebook.ipynb')
    assert os.path.isfile(path)
    before = os.stat(path).st_mtime
    update_notebook_by_run(path)
    assert os.stat(path).st_mtime > before


def test_notebook_util():
    nbdir = get_notebook_dir()
    nbs = [nb for nb in iter_notebooks(nbdir)]
    assert len(nbs) == 12
    nbms = [(nb, mf) for nb, mf in iter_notebook_manifests(nbdir)]
    assert len(nbms) == 5
    path = os.path.join(get_notebook_dir(), 'test-notebook3.ipynb')
    assert path == find_hdf_notebook_path('haje01', 'test')


def test_notebook_manifest(fxsoldir):
    path = os.path.join(get_notebook_dir(), 'test-notebook3.ipynb')
    assert os.path.isfile(path)
    mpath = get_notebook_manifest_path(path)
    assert os.path.isfile(mpath)

    # check manifest being written
    before = os.stat(mpath).st_mtime
    update_notebook_by_run(path)
    assert os.stat(mpath).st_mtime > before

    # check hdf store
    from wzdat.util import HDF
    with HDF('haje01') as hdf:
        df = hdf.store.select('test')
        assert len(df) == 7560

    #
    # check manifest checksum
    #
    import json
    with open(mpath, 'r') as f:
        data = json.loads(f.read())
    ws = data['worksheets'][0]
    assert len(ws['cells']) == 2
    chksums = ws['cells'][1]['input']
    assert 'WARNING' in chksums[0]
    # check depends checksum
    assert 'depends' in chksums[3]
    assert '8875249185536240278' in chksums[4]
    # check output checksum
    assert 'output' in chksums[6]
    assert '5917511075693791499' in chksums[7]

    path = os.path.join(get_notebook_dir(), 'test-notebook3.ipynb')
    assert os.path.isfile(path)
    mpath = get_notebook_manifest_path(path)
    assert os.path.isfile(mpath)
    manifest = Manifest(False, False, path)
    assert manifest._out_hdf_chksum is None


def test_notebook_manifest2(fxsoldir, fxhdftest2):
    # multiple files & hdfs dependency test
    path = os.path.join(get_notebook_dir(), 'test-notebook5.ipynb')
    assert os.path.isfile(path)
    mpath = get_notebook_manifest_path(path)
    assert os.path.isfile(mpath)
    update_notebook_by_run(path)
    manifest = Manifest(False, True, path)
    assert len(manifest.depends.files) == 2
    assert len(manifest.depends.hdf) == 2
    assert len(manifest._dep_files_chksum) == 2
    assert len(manifest._dep_hdf_chksum) == 2
    assert manifest._out_hdf_chksum is None

    path = os.path.join(get_notebook_dir(), 'test-notebook6.ipynb')
    with pytest.raises(RecursiveReference):
        Manifest(False, False, path)


def test_notebook_dependency(fxsoldir, fxnewfile):
    # run notebook first
    path = os.path.join(get_notebook_dir(), 'test-notebook3.ipynb')
    assert os.path.isfile(path)

    with HDF('haje01') as hdf:
        if 'test' in hdf.store:
            del hdf.store['test']

    update_notebook_by_run(path)
    manifest = Manifest(False, True, path)
    assert manifest._prev_files_chksum == manifest._dep_files_chksum
    with HDF('haje01') as hdf:
        prev_hdf_chksum = dataframe_checksum(hdf.store['test'])
        print "prev_hdf_chksum {}".format(prev_hdf_chksum)
        print len(hdf.store['test'])

    # add new file
    with open(fxnewfile, 'w') as f:
        f.write('2014-03-05 23:30 [ERROR] - Async\n')

    manifest = Manifest(False, False, path)
    assert manifest._depend_files_changed
    assert manifest._prev_files_chksum != manifest._dep_files_chksum

    # run notebok again
    update_notebook_by_run(path)
    with HDF('haje01') as hdf:
        new_hdf_chksum = dataframe_checksum(hdf.store['test'])
        print "new_hdf_chksum {}".format(new_hdf_chksum)
        print len(hdf.store['test'])

    # check check
    assert prev_hdf_chksum != new_hdf_chksum


def test_notebook_depresolv(fxsoldir):
    from wzdat.nbdependresolv import DependencyTree
    skip_nbs = [os.path.join(get_notebook_dir(), 'test-notebook6.ipynb')]
    dt = DependencyTree(get_notebook_dir(), skip_nbs)
    nb3 = dt.get_notebook_by_fname('test-notebook3')
    nb4 = dt.get_notebook_by_fname('test-notebook4')
    nb5 = dt.get_notebook_by_fname('test-notebook5')
    assert nb4.is_depend(nb3)
    assert nb5.is_depend(nb3)
    assert nb5.is_depend(nb4)
    dt.resolve()
