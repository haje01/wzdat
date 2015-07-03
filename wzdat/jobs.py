import os
import logging

import argh

from wzdat.const import FORWARDER_LOG_PREFIX
from wzdat.make_config import make_config
from wzdat.ipynb_runner import update_notebook_by_run
from wzdat.rundb import get_cron_notebooks, save_cron, destroy_table as\
    _destroy_table
from wzdat.util import gen_dummydata as _gen_dummydata, cache_files,\
    cache_finder, get_notebook_dir
from wzdat import event as evt

cfg = make_config()


def _remove_forwarder_file(es):
    return [e for e in es if FORWARDER_LOG_PREFIX not in e[3]]


def check_cache():
    # TODO: remove when file-wise caching done
    es = _remove_forwarder_file(evt.get_unhandled_events())
    if len(es) > 0:
        cache_all()
        ids = [e[0] for e in es]
        evt.mark_handled_events('check_cache', ids)


def update_notebooks():
    '''Check notebook's dependency and run if needed.'''
    logging.debug('update_notebooks')
    nbdir = get_notebook_dir()
    from wzdat.nbdependresolv import DependencyTree
    skip_nbs = [os.path.join(nbdir, 'test-notebook6.ipynb')]
    dt = DependencyTree(nbdir, skip_nbs)
    return dt.resolve(True)


def cache_all():
    logging.debug('cache_all')
    cache_files()
    cache_finder()


def register_cron():
    from wzdat.util import register_cron_notebooks
    logging.debug("register_cron")
    paths, scheds = register_cron_notebooks()
    save_cron(paths, scheds)


def run_all_cron_notebooks():
    logging.debug('run_all_cron_notebooks')
    for nbpath in get_cron_notebooks():
        run_notebook(nbpath)


@argh.arg('path', help="notebook path")
def run_notebook(path):
    path = path.decode('utf-8') if type(path) == str else path
    logging.debug(u'run_notebook {}'.format(path))
    update_notebook_by_run(path)


@argh.arg('tbname', help="table name to destroy")
def destroy_table(tbname):
    """Destroy DB table if exists."""
    _destroy_table(tbname)


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


if __name__ == "__main__":
    argh.dispatch_commands([cache_all, register_cron, run_notebook,
                            gen_dummydata, run_all_cron_notebooks,
                            register_event, check_cache, destroy_table,
                            update_notebooks])
