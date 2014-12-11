import pytest

from wzdat.rundb import create_db, destroy_db
from wzdat.dashboard.app import app as dapp
from wzdat.make_config import make_config

cfg = make_config()


@pytest.yield_fixture(scope='module')
def app():
    create_db()
    yield dapp.test_client()
    destroy_db()


def test_dashboard_home(app):
    rv = app.get('/', follow_redirects=True)
    assert '200 OK' == rv.status
    assert 'WzDat MYPRJ Dashboard' in rv.data
