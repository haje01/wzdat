import time

import pytest
import sqlite3

from wzdat.rundb import Cursor, _update_run_info, _update_cache_info
from wzdat.make_config import make_config

cfg = make_config()

RUNNER_DB_PATH = cfg['runner_db_path']
TEST_DB_LOCK = False


def is_table_exist(tbname):
    with Cursor(RUNNER_DB_PATH) as cur:
        rv = cur.execute('SELECT count(*) FROM sqlite_master WHERE type'
                         '="table" and name="{}"'.format(tbname)).fetchone()
        return rv[0] == 1


def test_db_create(fxdb):
        assert is_table_exist('info')
        assert is_table_exist('cache')
        assert is_table_exist('finder')
        assert is_table_exist('event')


@pytest.mark.skipif(not TEST_DB_LOCK, reason="No DBLock Test")
def test_db_start_run(initdb):
    with Cursor(RUNNER_DB_PATH) as cur:
        for i in range(5000000):
            _update_cache_info(cur)


@pytest.mark.skipif(not TEST_DB_LOCK, reason="No DBLock Test")
def test_db_update_run():
    with Cursor(RUNNER_DB_PATH) as cur:
        for i in range(5000000):
            try:
                st = time.time()
                _update_run_info(cur, '/notes/mynote{}'.format(i), i)
            except sqlite3.OperationalError, e:
                print str(e)
                print i, time.time() - st
                raise
