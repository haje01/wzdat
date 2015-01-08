import os
import re
import urllib2
import time
from subprocess import check_output

import pytest

host = os.environ['WZDAT_HOST']
dashboard_url = 'http://{}:8085'.format(host)
env = os.environ.copy()
env['RSYNC_PASSWORD'] = 'test'


@pytest.yield_fixture(scope='session')
def docker():
    path = os.path.expandvars('$WZDAT_DIR/system')
    cid = check_output(['docker', 'ps', '-q', 'wzdat_myprj'], cwd=path).strip()
    if cid:
        check_output(['fab', 'rm:myprj'], cwd=path)
    ret = check_output(['fab', 'launch:myprj'], cwd=path)
    assert 'Done' in ret
    while True:
        try:
            urllib2.urlopen(dashboard_url)
            break
        except urllib2.URLError:
            time.sleep(1)
    yield


def test_system_dashboard(docker):
    f = urllib2.urlopen(dashboard_url)
    r = f.read()
    assert 'WzDat MYPRJ Dashboard' in r


def test_system_rsync(docker):
    ret = check_output(['docker', 'ps'])
    rport = re.search(r'.*:(\d+)->873.*', ret).groups()[0]
    cmd = ['rsync', '-azv', 'LICENSE', '--port={}'.format(rport),
           'rsync-user@{}::rsync-data/test'.format(host)]
    ret = check_output(cmd, env=env)
    assert 'sent ' in ret
