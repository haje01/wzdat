import os

from fabric.api import local, run, cd, env


assert 'WZDAT_HOST' in os.environ
wzhost = os.environ['WZDAT_HOST']


def test():
    local("py.test tests/ system/tests --cov wzdat --cov-report=term-missing")


def coveralls():
    local("coveralls")


def diff():
    local("git diff")


def commit(msg=None):
    diff()
    if msg is None:
        msg = raw_input("Enter commit message: ")
    local('git commit -m "{}" -a'.format(msg))


def gpush():
    local("git push")


def prepare():
    # test()
    commit()
    gpush()


def remote_hosts():
    env.hosts = open('remote_hosts', 'r').readlines()


def deploy(_build=False):
    with cd('~/wzdat'):
        run("git pull")
        if _build:
            build(True)
        relaunch(True)


def dpush():
    local('docker push haje01/wzdat-base')
    local('docker push haje01/wzdat')


def status():
    run('supervisorctl status')


def ps():
    local('docker ps -a')


def rm(prj):
    local('docker rm -f wzdat_{prj}'.format(prj=prj))


def rm_all(_remote=False):
    cmd = run if _remote else local
    cmd('docker rm -f $(docker ps -aq)')


def _build_base(_remote):
    cmd = run if _remote else local
    cmd('ln -fs files/base.docker Dockerfile')
    cmd('docker build -t haje01/wzdat-base .')
    cmd('rm -f Dockerfile')


def _build(_remote):
    cmd = run if _remote else local
    if _remote:
        r = run('docker images -q haje01/wzdat-base')
    else:
        r = cmd('docker images -q haje01/wzdat-base', capture=True)
    if len(r) == 0:
        print "No local 'haje01/wzdat-base' image. Trying to find in the "\
            "docker hub."
    cmd('ln -fs files/self.docker Dockerfile')
    cmd('docker build --no-cache -t haje01/wzdat .')
    cmd('rm -f Dockerfile')


def _build_dev(_remote):
    cmd = run if _remote else local
    if _remote:
        r = cmd('docker images -q haje01/wzdat')
    else:
        r = cmd('docker images -q haje01/wzdat', capture=True)
    if len(r) == 0:
        print "No local 'haje01/wzdat' image. Trying to find in the docker"\
            " hub."
    cmd('ln -fs files/dev/dev.docker Dockerfile')
    cmd('docker build --no-cache -t haje01/wzdat-dev .')
    cmd('rm -f Dockerfile')


def build(_remote=False):
    with cd('system'):
        _build_base(_remote)
        _build(_remote)
        _build_dev(_remote)


def relaunch(_remote=False):
    rm_all(_remote)
    launch(_remote)


def launch(_remote=False):
    cmd = run if _remote else local
    cmd('python -m system.cmds launch')
