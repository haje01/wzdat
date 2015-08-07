"""
    Database module for IPython Notebook Runner
"""

import logging

import redis

from wzdat.make_config import make_config
from wzdat.util import file_checksum, get_client_sdatetime,\
    parse_client_sdatetime, get_client_datetime
from wzdat.const import EVENT_DEFAULT_PRIOR, FORWARDER_LOG_PREFIX

WZDAT_REDIS_DB = 1

cfg = make_config()

r = redis.StrictRedis(db=WZDAT_REDIS_DB)


def reset_run(path):
    """Reset run info."""
    logging.debug(u"reset_run for {}".format(path))
    nbchksum = file_checksum(path)
    key = u'run:{}'.format(path)
    info = {'cur': 0, 'total': 0, 'nbchksum': nbchksum, 'start': None,
            'elapsed': None, 'error': None}
    r.hmset(key, info)


def start_run(path, total):
    start_dt = get_client_sdatetime()
    info = {'start': start_dt, 'cur': 0, 'total': total, 'error': None}
    r.hmset(u'run:{}'.format(path), info)


def finish_run(path, err):
    key = u'run:{}'.format(path)
    if r.exists(key):
        _start_dt, total = r.hmget(key, 'start', 'total')
        if err is None:
            start_dt = parse_client_sdatetime(_start_dt)
            elapsed = get_client_datetime() - start_dt
            r.hmset(key, {'elapsed': elapsed.total_seconds(), 'cur': total})
        else:
            r.hmset(key, {'error': err})


def update_run_info(path, curcell):
    key = u'run:{}'.format(path)
    if r.exists(key):
        r.hset(key, 'cur', curcell)


def get_run_info(path):
    key = u'run:{}'.format(path)
    if r.exists(key):
        return r.hmget(key, 'start', 'elapsed', 'cur', 'total', 'error')


def update_cache_info():
    logging.debug('update_cache_info')
    r.set('last_cached', get_client_sdatetime())


def get_cache_info():
    return r.get('last_cached')


def update_finder_info(info):
    logging.debug('update_finder_info')
    r.delete('finder_*')
    for ft, _dates, _kinds, _nodes in info:
        key = 'finder_{}'.format(ft)
        dates = ','.join(_dates)
        kinds = ','.join(_kinds)
        nodes = ','.join(_nodes)
        r.hmset(key, {'dates': dates, 'kinds': kinds, 'nodes': nodes})


def get_finder_info():
    logging.debug('get_finder_info')
    ret = []
    for key in r.keys('finder_*'):
        ft = key.split('_')[-1]
        _dates, _kinds, _nodes = r.hmget(key, 'dates', 'kinds', 'nodes')
        dates = _dates.split(',')
        kinds = _kinds.split(',')
        nodes = _nodes.split(',')
        ret.append((ft, dates, kinds, nodes))
    return ret


def check_notebook_error_and_changed(path):
    """
        Return notebook has error and changed after last run.
    """
    nbchksum = file_checksum(path)
    key = u'run:{}'.format(path)
    if r.exists(key):
        error, prevchksum = r.hmget(key, 'error', 'nbchksum')
        return error is not None, int(prevchksum) != nbchksum
    return False, False


def register_event(etype, info, prior=EVENT_DEFAULT_PRIOR):
    # skip forwarder files
    if FORWARDER_LOG_PREFIX in info:
        return
    logging.debug('register_event {} - {}'.format(etype, info))
    raised = get_client_sdatetime()
    r.sadd('unhandled', (prior, etype, info, raised))


def unhandled_events():
    return r.smembers('unhandled')


def flush_unhandled_events():
    r.delete('unhandled')
