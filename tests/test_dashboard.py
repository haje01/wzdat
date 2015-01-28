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


def test_dashboard_finder(app):
    rv = app.get('/finder', follow_redirects=True)
    assert '200 OK' == rv.status
    assert 'D2014_03_05' in rv.data
    assert 'D2014_02_24' in rv.data

    data = {
        'start_dt': 'D2014_03_04',
        'end_dt': 'D2014_03_05',
        'nodes[]': ['node.jp_node_1', 'node.kr_node_1'],
        'kinds[]': ['kind.auth', 'kind.game']
    }
    data = "start_dt=D2014_03_04&end_dt=D2014_03_05&kinds%5B%5D=auth&"\
           "kinds%5B%5D=game&nodes%5B%5D=jp_node_1&nodes%5B%5D=kr_node_1"
    sub = 'finder_request_download'
    r = app.post('/{}/log'.format(sub), data=data, follow_redirects=True)
    assert r.status_code == 200
