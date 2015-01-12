import os
import re
import urllib2
import time
import shutil
from subprocess import check_output

import pytest

from wzdat import event as evt

WEB_RESTART = False

host = os.environ['WZDAT_HOST']
dashboard_url = 'http://{}:8085'.format(host)
env = os.environ.copy()


@pytest.yield_fixture(scope='session')
def docker():
    # remove previous test resource
    path = os.path.expandvars('$WZDAT_DIR/tests/dummydata')
    if os.path.isdir(path):
        shutil.rmtree(path)

    # gen dummy data
    from wzdat.util import gen_dummydata
    gen_dummydata(path)

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


def test_system_dashboard(docker):
    f = urllib2.urlopen(dashboard_url)
    r = f.read()
    assert 'WzDat MYPRJ Dashboard' in r


def test_system_file_event(docker):
    evt.remove_all()

    ret = check_output(['docker', 'ps'])
    rport = re.search(r'.*:(\d+)->873.*', ret).groups()[0]
    path = os.path.expandvars('$WZDAT_DIR/tests')
    assert os.path.isdir(os.path.join(path, 'dummydata'))

    env['RSYNC_PASSWORD'] = 'test'
    for l in ('kr', 'us', 'jp'):
        cmd = ['rsync', '-azv', 'dummydata/{}'.format(l),
               '--port={}'.format(rport),
               'rsync-user@{}::rsync-data/test'.format(host)]
        ret = check_output(cmd, env=env, cwd=path)
        assert 'sent ' in ret

    time.sleep(2)  # wait for all event registered
    assert 450 == len(evt.get_all())
