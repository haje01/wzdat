import os
import argh
import imp

from wzdat.make_config import make_config
from wzdat.ipynb_runner import update_notebook_by_run
from wzdat.rundb import update_cache_info, update_finder_info
from wzdat.util import gen_dummydata as _gen_dummydata, get_data_dir


cfg = make_config()


def cache_all():
    cache_files()
    cache_finder()


def cache_files():
    # prevent using cache
    os.environ['WZDAT_NO_CACHE'] = 'True'

    prj = os.environ['WZDAT_PRJ']
    print "Caching files for: %s" % prj
    datadir = get_data_dir()
    pkg = os.environ["WZDAT_SOL_PKG"]
    pcfg = make_config(prj)
    for ftype in pcfg["FILE_TYPES"]:
        cmd = ['from %s.%s.%s import find_files_and_save; '
               'find_files_and_save("%s")' % (pkg, prj, ftype, datadir)]
        cmd = ' '.join(cmd)
        exec(cmd)
    update_cache_info()


def cache_finder():
    ret = []
    if ret is None or len(ret) == 0:
        pkg = os.environ["WZDAT_SOL_PKG"]
        prj = os.environ['WZDAT_PRJ']
        pcfg = make_config(prj)
        ftypes = pcfg["FILE_TYPES"]
        os.chdir('/solution')
        ret = []
        for ft in ftypes:
            mpath = '%s/%s/%s.py' % (pkg, prj, ft)
            mod = imp.load_source('%s' % ft,  mpath)
            dates = [str(date) for date in mod.dates[:-15:-1]]
            kinds = [str(kind) for kind in mod.kinds.group()]
            nodes = [str(node) for node in mod.nodes]
            info = ft, dates, kinds, nodes
            ret.append(info)
        update_finder_info(ret)
    return ret


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
    return _gen_dummydata(td)


if __name__ == "__main__":
    argh.dispatch_commands([cache_files, register_cron, run_notebook,
                            gen_dummydata])
