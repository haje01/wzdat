import os

from fabric.api import local, run, env, abort

from wzdat.make_config import make_config

env.password = 'docker'
prj_map = {}


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
            host = 'root@0.0.0.0:%s' % port
            hosts.append(host)
            prj_map[host] = _prj
    env.hosts = hosts


def push():
    local('docker push haje01/wzdat-base')
    local('docker push haje01/wzdat')


def status():
    run('supervisorctl status')


def ps():
    local('docker ps -a')


def rm(prj):
    local('docker rm -f wzdat_{prj}'.format(prj=prj))


def rm_all():
    local('docker rm -f $(docker ps -aq)')


def _build_base():
    local('ln -fs files/base.docker Dockerfile')
    local('docker build -t haje01/wzdat-base .')
    local('rm -f Dockerfile')


def _build():
    r = local('docker images -q haje01/wzdat-base', capture=True)
    if len(r) == 0:
        print "No local 'haje01/wzdat-base' image. Trying to find in the "\
            "docker hub."
    local('ln -fs files/self.docker Dockerfile')
    local('docker build --no-cache -t haje01/wzdat .')
    local('rm -f Dockerfile')


def _build_dev():
    r = local('docker images -q haje01/wzdat', capture=True)
    if len(r) == 0:
        print "No local 'haje01/wzdat' image. Trying to find in the docker"\
            " hub."
    local('ln -fs files/dev/dev.docker Dockerfile')
    local('docker build --no-cache -t haje01/wzdat-dev .')
    local('rm -f Dockerfile')


def build():
    _build_base()
    _build()
    _build_dev()


def ssh(_prj):
    assert 'WZDAT_HOST' in os.environ
    for prj, port in _get_prj_and_ports():
        if prj == _prj:
            ip = os.environ['WZDAT_HOST']
            local('ssh root@{ip} -p {port}'.format(ip=ip, port=port))
            return
    abort("Can't find project")


def log(prj):
    local('docker logs -f wzdat_{prj}'.format(prj=prj))


def _get_pkg():
    assert 'WZDAT_SOL_PKG' in os.environ
    return os.environ['WZDAT_SOL_PKG']


def _get_cfg_export():
    pkg = _get_pkg()
    assert len(prj_map) > 0, "No host is designated!!"
    for _, prj in prj_map.iteritems():
        yield 'WZDAT_CFG=/solution/{pkg}/{prj}/config.yml'.format(pkg=pkg,
                                                                  prj=prj)


def cache():
    for cfgex in _get_cfg_export():
        run('cd /solution && {} python -m wzdat.jobs cache-all'.format(cfgex))


def restart(prg):
    run('supervisorctl restart {prg}'.format(prg=prg))


def runcron():
    assert 'WZDAT_HOST' in os.environ
    for cfgex in _get_cfg_export():
        wzhost = os.environ['WZDAT_HOST']
        run('WZDAT_HOST={} {} python -m wzdat.jobs run-all-cron-notebooks'.
            format(wzhost, cfgex))


def launch(prj, dbg=False):
    assert 'WZDAT_DIR' in os.environ
    assert 'WZDAT_SOL_DIR' in os.environ
    assert 'WZDAT_HOST' in os.environ
    wzpkg = _get_pkg()
    wzdir = os.environ['WZDAT_DIR']
    wzsol = os.environ['WZDAT_SOL_DIR']
    wzhost = os.environ['WZDAT_HOST']
    runopt = ""
    cmd = ""
    if dbg:
        runopt = "-ti"
        cmd = "bash"
    else:
        runopt = "-d"
    cfgpath = os.path.join(wzsol, wzpkg, prj, 'config.yml')
    cfg = make_config(cfgpath)
    iport = cfg['host_ipython_port']
    dport = cfg['host_dashboard_port']
    if 'data_dir' in cfg:
        datavol = '-v {}:/logdata'.format(cfg['data_dir'])
    else:
        datavol = '/logdata/{}'.format(prj)
    cmd = 'docker run {runopt} -p 22 -p {iport}:8090 -p {dport}:80\
            -p 873 --name "wzdat_{wzprj}"\
            -v {wzdir}:/wzdat -v {wzsol}:/solution\
            {datavol}\
            -v $HOME/.vimrc:/root/.vimrc\
            -v $HOME/.vim/:/root/.vim\
            -v $HOME/.gitconfig:/root/.gitconfig\
            -v $HOME/.screenrc:/root/.screenrc\
            -e WZDAT_DIR=/wzdat\
            -e WZDAT_SOL_DIR=/solution\
            -e WZDAT_SOL_PKG={wzpkg}\
            -e WZDAT_PRJ={wzprj}\
            -e WZDAT_HOST={wzhost}\
            -e WZDAT_CFG=/solution/{wzpkg}/{wzprj}/config.yml\
            -e HOME=/root\
            haje01/wzdat-dev {cmd}'.format(runopt=runopt, wzprj=prj,
                                           wzdir=wzdir, wzsol=wzsol,
                                           wzpkg=wzpkg, wzhost=wzhost,
                                           iport=iport, dport=dport,
                                           datavol=datavol, cmd=cmd)
    local(cmd)
