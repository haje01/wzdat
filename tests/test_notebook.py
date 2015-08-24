import os

import pytest
from datetime import datetime

from wzdat.manifest import Manifest, RecursiveReference
from wzdat.util import get_notebook_dir, find_hdf_notebook_path,\
    get_notebook_manifest_path, iter_notebooks, iter_notebook_manifest_input,\
    get_data_dir, dataframe_checksum, HDF, iter_dashboard_notebook, \
    iter_scheduled_notebook, touch
from wzdat.ipynb_runner import update_notebook_by_run, notebook_outputs_to_html
from wzdat.rundb import check_notebook_error_and_changed, iter_run_info
from wzdat.nbdependresolv import update_all_notebooks


@pytest.yield_fixture
def fxsoldir():
    sol_dir = os.environ['WZDAT_SOL_DIR']
    os.chdir(sol_dir)
    yield sol_dir


@pytest.yield_fixture
def fxhdftest2():
    with HDF('haje01') as hdf:
        if 'test' not in hdf.store:
            # check hdf notebook has run
            path = find_hdf_notebook_path('haje01', 'test')
            assert path is not None
            update_notebook_by_run(path)
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
    path = os.path.join(get_notebook_dir(), 'test-notebook.ipynb')
    assert os.path.isfile(path)
    before = os.stat(path).st_mtime
    update_notebook_by_run(path)
    assert os.stat(path).st_mtime > before
    runnbs = [ri[0] for ri in iter_run_info()]
    assert path in runnbs


def test_notebook_error():
    path = os.path.join(get_notebook_dir(), 'test-notebook-error.ipynb')
    assert os.path.isfile(path)
    try:
        update_notebook_by_run(path)
    except ValueError:
        pass
    assert check_notebook_error_and_changed(path) == (True, False)
    touch(path)
    assert check_notebook_error_and_changed(path) == (True, True)


def test_notebook_util():
    nbdir = get_notebook_dir()
    nbs = [nb for nb in iter_notebooks(nbdir)]
    assert len(nbs) == 11
    nbms = [(nb, mi) for nb, mi in iter_notebook_manifest_input(nbdir)]
    assert len(nbms) == 9
    path = os.path.join(nbdir, 'test-notebook3.ipynb')
    assert path == find_hdf_notebook_path('haje01', 'test')


def test_notebook_manifest1(fxsoldir):
    nbdir = get_notebook_dir()
    path = os.path.join(nbdir, 'test-notebook3.ipynb')
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
    assert 'last_run' in chksums[2]
    # check max mem
    assert 'max_memory' in chksums[3]
    # check depends checksum
    assert 'depends' in chksums[4]
    assert '8875249185536240278' in chksums[5]
    # check output checksum
    assert 'output' in chksums[7]
    assert '5917511075693791499' in chksums[8]

    manifest = Manifest(False, False, path)
    assert type(manifest.last_run) is datetime
    assert manifest._out_hdf_chksum is None


def test_notebook_manifest2(fxsoldir, fxhdftest2):
    # multiple files & hdfs dependency test
    nbdir = get_notebook_dir()
    path = os.path.join(nbdir, 'test-notebook5.ipynb')
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

    path = os.path.join(nbdir, 'test-notebook6.ipynb')
    mpath = get_notebook_manifest_path(path)
    with pytest.raises(RecursiveReference):
        Manifest(False, False, path)


def test_notebook_dependency(fxsoldir, fxnewfile):
    # run notebook first
    nbdir = get_notebook_dir()
    path = os.path.join(nbdir, 'test-notebook3.ipynb')
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
    nbdir = get_notebook_dir()
    from wzdat.nbdependresolv import DependencyTree
    skip_nbs = [os.path.join(nbdir, 'test-notebook6.ipynb')]
    dt = DependencyTree(nbdir, skip_nbs)
    nb3 = dt.get_notebook_by_fname('test-notebook3')
    nb4 = dt.get_notebook_by_fname('test-notebook4')
    nb5 = dt.get_notebook_by_fname('test-notebook5')
    assert nb4.is_depend(nb3)
    assert nb5.is_depend(nb3)
    assert nb5.is_depend(nb4)
    resolved, _ = dt.resolve()
    sched_nbs = set([snb for snb, scd in iter_scheduled_notebook(nbdir)])
    resolved_nbs = set([nb.path for nb in resolved])
    assert len(sched_nbs & resolved_nbs) == 0


def test_notebook_dashboard(fxsoldir):
    nbdir = get_notebook_dir()
    dnbs = [nbpath for nbpath in iter_dashboard_notebook(nbdir)]
    assert len(dnbs) == 3


def test_notebook_cron(fxsoldir):
    from wzdat.jobs import register_cron
    register_cron()
    from crontab import CronTab
    cron = CronTab()
    assert len(cron.crons) == 3


def test_notebook_resolve(fxsoldir, fxnewfile):
    _, runs = update_all_notebooks()

    _, runs = update_all_notebooks()
    assert len(runs) == 0

    # add new file
    with open(fxnewfile, 'w') as f:
        f.write('2014-03-05 23:30 [ERROR] - Async\n')

    _, runs = update_all_notebooks()
    assert len(runs) == 4


def test_notebook_nodata():
    nbdir = get_notebook_dir()
    path = os.path.join(nbdir, 'test-notebook-nodata.ipynb')
    assert os.path.isfile(path)
    update_notebook_by_run(path)
    rv = notebook_outputs_to_html(path)
    assert 'NoDataFound' in rv
