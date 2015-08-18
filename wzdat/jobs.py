import os
import logging

import argh

from wzdat.rundb import flush_unhandled_events, unhandled_events, iter_run_info
from wzdat.const import FORWARDER_LOG_PREFIX
from wzdat.make_config import make_config
from wzdat.ipynb_runner import update_notebook_by_run, NoDataFound
from wzdat.util import gen_dummydata as _gen_dummydata, cache_files,\
    cache_finder, get_notebook_dir, OfflineNBPath

cfg = make_config()


def _remove_forwarder_file(es):
    return [e for e in es if FORWARDER_LOG_PREFIX not in e[3]]


def check_cache():
    # TODO: remove when file-wise caching done
    es = _remove_forwarder_file(unhandled_events())
    if len(es) > 0:
        cache_all()
        flush_unhandled_events()


def update_notebooks():
    '''Check notebook's dependency and run for dashboard if needed.'''
    logging.debug('update_notebooks start')
    nbdir = get_notebook_dir()
    from wzdat.nbdependresolv import DependencyTree
    skip_nbs = [os.path.join(nbdir, 'test-notebook6.ipynb')]
    dt = DependencyTree(nbdir, skip_nbs)
    ret = dt.resolve(True)
    logging.debug('update_notebooks done')
    return ret


def cache_all():
    logging.debug('cache_all')
    cache_files()
    cache_finder()


def register_cron():
    from wzdat.util import register_cron_notebooks
    logging.debug("register_cron")
    paths, scheds = register_cron_notebooks()


@argh.arg('path', help="notebook path")
def run_notebook(path):
    path = path.decode('utf-8') if type(path) == str else path
    logging.debug(u'run_notebook {}'.format(path))
    with OfflineNBPath(path):
        try:
            update_notebook_by_run(path)
        except NoDataFound, e:
            logging.debug(unicode(e))


@argh.arg('-d', '--dir', help="target directory where dummy data will be"
          "written into. if skipped, cfg['data_dir'] will be chosen.")
def gen_dummydata(**kwargs):
    td = kwargs['dir']
    if td is None:
        td = cfg['data_dir']
    return _gen_dummydata(td)


@argh.arg('-e', '--type', help="event type")
@argh.arg('-i', '--info', help="event info")
def register_event(**kwargs):
    etype = kwargs['type']
    einfo = kwargs['info']
    print etype, einfo


def run_info():
    for ri in iter_run_info():
        print ri


if __name__ == "__main__":
    argh.dispatch_commands([cache_all, register_cron, run_notebook,
                            gen_dummydata, register_event, check_cache,
                            update_notebooks, run_info])
