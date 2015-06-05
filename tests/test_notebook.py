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
    nbs = [nb for nb in iter_notebooks()]
    assert len(nbs) == 10
    nbms = [(nb, mf) for nb, mf in iter_notebook_manifests()]
    assert len(nbms) == 4
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
        assert len(df) == 4320

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
    assert 'depends' in chksums[2]
    assert '6533060286654657065' in chksums[3]
    # check output checksum
    assert 'output' in chksums[5]
    assert '-9210520864853562061' in chksums[6]


def test_notebook_manifest2(fxsoldir):
    path = os.path.join(get_notebook_dir(), 'test-notebook5.ipynb')
    assert os.path.isfile(path)
    mpath = get_notebook_manifest_path(path)
    assert os.path.isfile(mpath)

    # run notebook
    update_notebook_by_run(path)

    path = os.path.join(get_notebook_dir(), 'test-notebook6.ipynb')
    with pytest.raises(RecursiveReference):
        Manifest(False, path)


def test_notebook_dependency(fxsoldir, fxnewfile):
    # run notebook first
    path = os.path.join(get_notebook_dir(), 'test-notebook3.ipynb')
    assert os.path.isfile(path)

    with HDF('haje01') as hdf:
        if 'test' in hdf.store:
            del hdf.store['test']

    update_notebook_by_run(path)
    manifest = Manifest(False, path)
    assert manifest._prev_files_chksum == manifest._files_chksum
    with HDF('haje01') as hdf:
        prev_hdf_chksum = dataframe_checksum(hdf.store['test'])
        print "prev_hdf_chksum {}".format(prev_hdf_chksum)
        print len(hdf.store['test'])

    # add new file
    with open(fxnewfile, 'w') as f:
        f.write('2014-03-05 23:30 [ERROR] - Async\n')

    manifest = Manifest(False, path)
    assert manifest._depend_files_changed
    assert manifest._prev_files_chksum != manifest._files_chksum

    # run notebok again
    update_notebook_by_run(path)
    with HDF('haje01') as hdf:
        new_hdf_chksum = dataframe_checksum(hdf.store['test'])
        print "new_hdf_chksum {}".format(new_hdf_chksum)
        print len(hdf.store['test'])

    # check check
    assert prev_hdf_chksum != new_hdf_chksum
