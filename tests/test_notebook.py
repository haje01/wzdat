import os

from wzdat.util import get_notebook_dir, find_hdf_notebook_path,\
    get_notebook_manifest_path, iter_notebooks, iter_notebook_manifests
from wzdat.ipynb_runner import update_notebook_by_run


def test_notebook_run():
    path = os.path.join(get_notebook_dir(),
                        '[* * * * *@Test]test-notebook.ipynb')
    assert os.path.isfile(path)
    before = os.stat(path).st_mtime
    update_notebook_by_run(path)
    assert os.stat(path).st_mtime > before


def test_notebook_util():
    nbs = [nb for nb in iter_notebooks()]
    assert len(nbs) == 6
    nbms = [(nb, mf) for nb, mf in iter_notebook_manifests()]
    assert len(nbms) == 2
    path = os.path.join(get_notebook_dir(), 'test-notebook3.ipynb')
    assert path == find_hdf_notebook_path('haje01', 'test')


def test_notebook_manifest():
    path = os.path.join(get_notebook_dir(), 'test-notebook3.ipynb')
    assert os.path.isfile(path)
    mpath = get_notebook_manifest_path(path)
    assert os.path.isfile(mpath)

    # check manifest being written
    before = os.stat(mpath).st_mtime
    sol_dir = os.environ['WZDAT_SOL_DIR']
    os.chdir(sol_dir)
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
    assert '827178658943898453' in chksums[3]
    # check output checksum
    assert 'output' in chksums[5]
    assert '-9210520864853562061' in chksums[6]


def test_notebook_manifest2():
    path = os.path.join(get_notebook_dir(), 'test-notebook5.ipynb')
    assert os.path.isfile(path)
    mpath = get_notebook_manifest_path(path)
    assert os.path.isfile(mpath)

    # run notebook
    sol_dir = os.environ['WZDAT_SOL_DIR']
    os.chdir(sol_dir)
    update_notebook_by_run(path)
