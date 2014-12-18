import os
import logging
import time

import argh
import imp

from wzdat.make_config import make_config, ChangeDir
from wzdat.ipynb_runner import update_notebook_by_run
from wzdat.rundb import update_cache_info, update_finder_info, save_cron, \
    get_cron_notebooks
from wzdat.util import gen_dummydata as _gen_dummydata


cfg = make_config()


def cache_all():
    logging.debug('cache_all')
    cache_files()
    cache_finder()


def cache_files():
    logging.debug('cache_files')
    with ChangeDir(cfg['sol_dir']):
        # prevent using cache
        if 'file_types' not in cfg:
            logging.warning('no file_types in cfg. exit')
            return
        old_use_cache = cfg['use_cache']
        prj = cfg['prj']
        print "Caching files for: %s" % prj
        datadir = cfg['data_dir']
        pkg = cfg['sol_pkg']
        ftypes = cfg['file_types']
        for ftype in ftypes:
            cmd = ['from %s.%s.%s import find_files_and_save; '
                   'find_files_and_save("%s")' % (pkg, prj, ftype, datadir)]
            cmd = ' '.join(cmd)
            exec(cmd)
        update_cache_info()
        cfg['use_cache'] = old_use_cache


def cache_finder():
    # Make cache for file finder.
    logging.debug('cache_finder')
    with ChangeDir(cfg['sol_dir']):
        if 'file_types' not in cfg:
            logging.warning('no file_types in cfg. exit')
            return
        ret = []
        if ret is None or len(ret) == 0:
            pkg = cfg['sol_pkg']
            prj = cfg['prj']
            ftypes = cfg["file_types"]
            sol_dir = cfg['sol_dir']
            os.chdir(sol_dir)
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
    logging.debug("register_cron")
    register_cron_notebooks(paths, scheds)
    save_cron(paths, scheds)


def run_all_cron_notebooks():
    logging.debug('run_all_cron_notebooks')
    for nbpath in get_cron_notebooks():
        run_notebook(nbpath)


@argh.arg('path', help="notebook path")
def run_notebook(path):
    path = path.decode('utf-8') if type(path) == str else path
    logging.debug(u'run_notebook {}'.format(path))
    st = time.time()
    update_notebook_by_run(path)


@argh.arg('-d', '--dir', help="target directory where dummy data will be"
          "written into. if skipped, cfg['data_dir'] will be chosen.")
def gen_dummydata(**kwargs):
    td = kwargs['dir']
    if td is None:
        td = cfg['data_dir']
    return _gen_dummydata(td)


if __name__ == "__main__":
    argh.dispatch_commands([cache_all, register_cron, run_notebook,
                            gen_dummydata, run_all_cron_notebooks])
