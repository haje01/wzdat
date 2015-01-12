import os
import re
import urllib2
import time
import shutil
from subprocess import check_output

import pytest

WEB_RESTART = False

host = os.environ['WZDAT_HOST']
dashboard_url = 'http://{}:8085'.format(host)
env = os.environ.copy()


@pytest.yield_fixture(scope='session')
def docker():
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
                time.sleep(3)
    else:
        # restart uWSGI
        if WEB_RESTART:
            ret = check_output(['fab', 'hosts:myprj', 'restart:uwsgi'],
                               cwd=path)

    yield

    # remove test resource
    path = os.path.expandvars('$WZDAT_DIR/tests/dummydata/test')
    shutil.rmtree(path)


def test_system_dashboard(docker):
    f = urllib2.urlopen(dashboard_url)
    r = f.read()
    assert 'WzDat MYPRJ Dashboard' in r


def test_system_rsync(docker):
    ret = check_output(['docker', 'ps'])
    rport = re.search(r'.*:(\d+)->873.*', ret).groups()[0]
    path = os.path.expandvars('$WZDAT_DIR/tests')
    cmd = ['rsync', '-azv', 'dummydata/', '--port={}'.format(rport),
           'rsync-user@{}::rsync-data/test'.format(host)]
    env['RSYNC_PASSWORD'] = 'test'
    ret = check_output(cmd, env=env, cwd=path)
    assert 'sent ' in ret
