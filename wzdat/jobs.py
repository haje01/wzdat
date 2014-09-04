import os
import argh

from wzdat.make_config import make_config
from wzdat.ipynb_runner import update_notebook_by_run
from wzdat.rundb import update_cache_info
from wzdat.const import BASE_DIR
from wzdat.util import gen_dummydata as _gen_dummydata


cfg = make_config()


def cache_files():
    prj = os.environ['WZDAT_PRJ']
    print "Caching files for: %s" % prj
    datadir = '/logdata'
    pkg = os.environ["WZDAT_SOL_PKG"]
    pcfg = make_config(prj)
    for ftype in pcfg["FILE_TYPES"]:
        cmd = ['from %s.%s.%s import find_files_and_save; '
               'find_files_and_save("%s")' % (pkg, prj, ftype, datadir)]
        cmd = ' '.join(cmd)
        exec(cmd)
    update_cache_info()


def register_cron():
    from wzdat.util import get_notebook_dir
    from wzdat.ipynb_runner import find_cron_notebooks, register_cron_notebooks
    nb_dir = get_notebook_dir()
    paths, scheds, _, _ = find_cron_notebooks(nb_dir)
    register_cron_notebooks(paths, scheds)


@argh.arg('path', help="notebook path")
def run_notebook(path):
    update_notebook_by_run(path)


@argh.arg('-d', '--dir', help="target directory where dummy data will be"
          "written into. if skipped, $WZDAT_DIR/tests/dummydata/ will be"
          "chosen.")
def gen_dummydata(**kwargs):
    td = kwargs['dir']
    if td is None:
        td = os.path.join(BASE_DIR, '..', 'tests', 'dummydata')
    return _gen_dummydata(td)


if __name__ == "__main__":
    argh.dispatch_commands([cache_files, register_cron, run_notebook, gen_dummydata])
