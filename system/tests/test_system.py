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


def test_system_dashboard(fxdocker):
    f = urllib2.urlopen(dashboard_url)
    r = f.read()
    assert 'WzDat MYPRJ Dashboard' in r


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


def test_system_download(fxdocker):
    # TODO: implement temporary file download test here
    pass
