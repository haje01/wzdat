import os
import re
import urllib2
import time
import shutil
from subprocess import check_output

import pytest

from wzdat import event as evt
from wzdat.rundb import reset_db
from wzdat.make_config import make_config

WEB_RESTART = False

host = os.environ['WZDAT_HOST'] if 'WZDAT_B2DHOST' not in os.environ else\
    os.environ['WZDAT_B2DHOST']
dashboard_url = 'http://{}:8085'.format(host)
env = os.environ.copy()


def _reset_data():
    from wzdat.util import gen_dummydata, get_var_dir
    cfg = make_config()
    ddir = cfg['data_dir']

    # remove previous dummy data
    if os.path.isdir(ddir):
        import shutil
        shutil.rmtree(ddir)

    # generate new dummy data
    gen_dummydata(ddir)
    # make _var_ dir
    get_var_dir()

    # reset db
    reset_db()


@pytest.yield_fixture(scope='session')
def fxdocker():
    # remove previous test resource
    path = os.path.expandvars('$WZDAT_DIR/tests/dummydata')
    if os.path.isdir(path):
        shutil.rmtree(path)

    cid = check_output(['docker', 'ps', '-q', 'wzdat_myprj']).strip()
    path = os.path.expandvars('$WZDAT_DIR/system')
    if not cid:
        # launch container
        ret = check_output(['fab', 'launch:myprj'], cwd=path)
        assert 'Done' in ret
        while True:
            try:
                urllib2.urlopen(dashboard_url)
                break
            except urllib2.URLError:
                time.sleep(2)
    else:
        _reset_data()
        # restart uWSGI
        if WEB_RESTART:
            ret = check_output(['fab', 'hosts:myprj', 'restart:uwsgi'],
                               cwd=path)
    yield


def test_system_file_event(fxdocker):
    evt.remove_all()

    ret = check_output(['docker', 'ps'])
    rport = re.search(r'.*:(\d+)->873.*', ret).groups()[0]
    path = os.path.expandvars('$WZDAT_DIR/tests')
    assert os.path.isdir(os.path.join(path, 'dummydata'))

    # sync all
    env['RSYNC_PASSWORD'] = 'test'

    def sync(locals, delete=False):
        _cmd = ['rsync']
        if delete:
            _cmd.append('--delete')
        for l in locals:
            cmd = _cmd + ['-azv', 'dummydata/{}'.format(l),
                          '--port={}'.format(rport),
                          'rsync-user@{}::rsync-data/test'.format(host)]
            ret = check_output(cmd, env=env, cwd=path)
            assert 'sent ' in ret
            time.sleep(3)  # wait for all events registered

    sync(('kr', 'us', 'jp'))
    assert 450 == len(evt.get_all())

    # modify & sync
    evt.remove_all()
    fpath = os.path.join(path, 'dummydata/kr/node-1/game_2014-02-24 01.log')
    with open(fpath, 'at') as f:
        f.write('---')
    sync(['kr'])
    rv = evt.get_all()[0]
    assert rv[2] == evt.FILE_MOVE_TO
    assert 'game_2014-02-24 01.log' in rv[3]

    # delete & sync
    evt.remove_all()
    os.remove(fpath)
    sync(['kr'], True)
    rv = evt.get_all()[0]
    assert rv[2] == evt.FILE_DELETE
    assert 'game_2014-02-24 01.log' in rv[3]


def test_system_finder():
    import requests

    # test finder home
    r = requests.get('{}/finder'.format(dashboard_url))
    assert r.status_code == 200
    assert 'D2014_03_05' in r.content
    assert 'D2014_02_24' in r.content

    data = "start_dt=D2014_03_04&end_dt=D2014_03_05&kinds%5B%5D=auth&"\
           "kinds%5B%5D=game&nodes%5B%5D=jp_node_1&nodes%5B%5D=kr_node_1"
    tmpl = '{}/{}/log'

    # test file select
    sub = 'finder_search'
    r = requests.post(tmpl.format(dashboard_url, sub), data=data)
    assert r.status_code == 200
    assert r.text.split('\n')[:-1] == [
        u'jp/node-1/log/auth_2014-03-04.log',
        u'jp/node-1/log/auth_2014-03-05.log',
        u'jp/node-1/log/game_2014-03-04 01.log',
        u'jp/node-1/log/game_2014-03-04 02.log',
        u'jp/node-1/log/game_2014-03-04 03.log',
        u'jp/node-1/log/game_2014-03-05 01.log',
        u'jp/node-1/log/game_2014-03-05 02.log',
        u'jp/node-1/log/game_2014-03-05 03.log',
        u'kr/node-1/log/auth_2014-03-04.log',
        u'kr/node-1/log/auth_2014-03-05.log',
        u'kr/node-1/log/game_2014-03-04 01.log',
        u'kr/node-1/log/game_2014-03-04 02.log',
        u'kr/node-1/log/game_2014-03-04 03.log',
        u'kr/node-1/log/game_2014-03-05 01.log',
        u'kr/node-1/log/game_2014-03-05 02.log',
        u'kr/node-1/log/game_2014-03-05 03.log']

    # test request download
    sub = 'finder_request_download'
    r = requests.post(tmpl.format(dashboard_url, sub), data=data)
    assert r.status_code == 200
    task_id = r.text

    def poll():
        sub = 'finder_poll_request_download'
        r = requests.post('{}/{}/{}'.format(dashboard_url, sub, task_id))
        return r.text

    time.sleep(3)
    rv = poll()
    assert '.zip' in rv


def test_system_download(fxdocker):
    # TODO: implement temporary file download test here
    pass
