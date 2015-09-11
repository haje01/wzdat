import pytest

from wzdat.rundb import r
from wzdat.dashboard.app import app as dapp
from wzdat.make_config import make_config

cfg = make_config()


@pytest.yield_fixture(scope='module')
def app():
    r.delete('*')
    yield dapp.test_client()


def test_dashboard_home(app):
    rv = app.get('/', follow_redirects=True)
    assert '200 OK' == rv.status
    assert 'WzDat MYPRJ Dashboard' in rv.data
    assert 'test-notebook' in rv.data
    assert 'current time' in rv.data
    assert 'test-notebook2' in rv.data
    assert 'test-notebook3' not in rv.data
    assert 'test-notebook-plot' in rv.data
    assert '<img src' in rv.data


def test_dashboard_finder(app):
    rv = app.get('/finder', follow_redirects=True)
    assert '200 OK' == rv.status
    assert 'D2014_03_05' in rv.data
    assert 'D2014_02_24' in rv.data
