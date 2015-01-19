import pytest

from wzdat.rundb import reset_db
from wzdat.dashboard.app import app as dapp
from wzdat.make_config import make_config

cfg = make_config()


@pytest.yield_fixture(scope='module')
def app():
    reset_db()
    yield dapp.test_client()


def test_dashboard_home(app):
    rv = app.get('/', follow_redirects=True)
    assert '200 OK' == rv.status
    assert 'WzDat MYPRJ Dashboard' in rv.data
