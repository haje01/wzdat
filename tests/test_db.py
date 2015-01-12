import pytest

from wzdat.rundb import create_db, destroy_db, Cursor
from wzdat.make_config import make_config

cfg = make_config()

RUNNER_DB_PATH = cfg['runner_db_path']


@pytest.yield_fixture(scope='module')
def db():
    create_db()
    yield
    destroy_db()


def is_table_exist(tbname):
    with Cursor(RUNNER_DB_PATH) as cur:
        rv = cur.execute('SELECT count(*) FROM sqlite_master WHERE type'
                         '="table" and name="{}"'.format(tbname)).fetchone()
        return rv[0] == 1


def test_db_create(db):
        assert is_table_exist('info')
        assert is_table_exist('cache')
        assert is_table_exist('finder')
        assert is_table_exist('cron')
        assert is_table_exist('event')
#
