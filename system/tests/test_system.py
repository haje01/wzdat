import os
import re
from subprocess import check_output

import pytest

host = os.environ['WZDAT_HOST']
env = os.environ.copy()
env['RSYNC_PASSWORD'] = 'test'


@pytest.yield_fixture(scope='module')
def rport():
    ret = check_output(['docker', 'ps'])
    port = re.search(r'.*:(\d+)->873.*', ret).groups()[0]
    yield port


def test_system_dashboard():
    import urllib2
    url = 'http://{}:8085'.format(host)
    print url
    f = urllib2.urlopen(url)
    r = f.read()
    assert 'WzDat MYPRJ Dashboard' in r


def test_system_rsync(rport):
    cmd = ['rsync', '-azv', 'LICENSE', '--port={}'.format(rport),
           'rsync-user@{}::rsync-data/test'.format(host)]
    ret = check_output(cmd, env=env)
    assert 'sent ' in ret
