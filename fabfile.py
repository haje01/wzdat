import os

from fabric.api import local, run, env, abort, cd, parallel

env.password = 'docker'
prj_map = {}


assert 'WZDAT_HOST' in os.environ
wzhost = os.environ['WZDAT_HOST']


def test():
    local("py.test tests/ system/tests --cov wzdat --cov-report=term-missing")


def coveralls():
    local("coveralls")


def diff():
    local("git diff")


def commit(msg=None):
    if msg is None:
        msg = raw_input("Enter commit message: ")
    local('git commit -m "{}" -a'.format(msg))


def gpush():
    local("git push")


def prepare():
    # test()
    diff()
    commit()
    gpush()


def set_hosts():
    env.hosts = open('hosts_file', 'r').readlines()


def deploy():
    with cd('~/wzdat'):
        run("git pull")
        build(True)


def _get_prj_and_ports():
    r = local('docker ps -a', capture=True)
    prjs = []
    for line in r.split('\n')[1:]:
        port = None
        for col in line.split():
            if '22/tcp' in col:
                port = col.split('->')[0].split(':')[1]
        name = '_'.join(line.split()[-1].split('_')[1:])
        prjs.append((name, port))
    return prjs


def hosts(prj=None):
    hosts = []
    for _prj, port in _get_prj_and_ports():
        if prj is None or prj == _prj:
            host = 'root@{}:{}'.format(_get_host(), port)
            hosts.append(host)
            prj_map[host] = _prj
    env.hosts = hosts


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


@parallel
def build(_remote=False):
    _build_base(_remote)
    _build(_remote)
    _build_dev(_remote)


def _get_host():
    return os.environ['WZDAT_B2DHOST'] if 'WZDAT_B2DHOST' in os.environ else\
        '0.0.0.0'


def ssh(_prj):
    for prj, port in _get_prj_and_ports():
        if prj == _prj:
            local('ssh root@{host} -p {port}'.format(host=_get_host(),
                                                     port=port))
            return
    abort("Can't find project")


def log(prj):
    local('docker logs -f wzdat_{prj}'.format(prj=prj))


def cache():
    run('python -m wzdat.jobs cache-all')


def restart(prg):
    run('supervisorctl restart {prg}'.format(prg=prg))


def runcron():
    run('python -m wzdat.jobs run-all-cron-notebooks')


def relaunch(_remote=False):
    rm_all(_remote)
    launch(_remote)


def launch(_remote=False):
    cmd = run if _remote else local
    cmd('python -m system.launch')
