import os

from fabric.api import local, run, env, abort

env.password = 'docker'
prj_map = {}


assert 'WZDAT_HOST' in os.environ
wzhost = os.environ['WZDAT_HOST']


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
            host = 'root@{}:{}'.format(wzhost, port)
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


def ssh(_prj, use_host=True):
    for prj, port in _get_prj_and_ports():
        if prj == _prj:
            host = wzhost if use_host is True else '0.0.0.0'
            local('ssh root@{host} -p {port}'.format(host=host, port=port))
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
    for cfgex in _get_cfg_export():
        run('WZDAT_HOST={} {} python -m wzdat.jobs run-all-cron-notebooks'.
            format(wzhost, cfgex))


def _make_config(cfgpath=None, usecache=True):
    """Make config object for project and return it."""
    import yaml

    def _expand_var(dic):
        assert type(dic) == dict
        # expand vars
        for k, v in dic.iteritems():
            typ = type(v)
            if typ != str and typ != unicode:
                continue
            dic[k] = os.path.expandvars(v)
        return dic

    class ChangeDir(object):
        def __init__(self, *dirs):
            self.cwd = os.getcwd()
            self.path = os.path.join(*dirs)

        def __enter__(self):
            assert os.path.isdir(self.path)
            os.chdir(self.path)

        def __exit__(self, atype, value, tb):
            os.chdir(self.cwd)

    _cfg = {}
    if cfgpath is None:
        assert 'WZDAT_CFG' in os.environ
        cfgpath = os.environ['WZDAT_CFG']

    adir = os.path.dirname(cfgpath)
    afile = os.path.basename(cfgpath)

    with ChangeDir(adir):
        loaded = yaml.load(open(afile, 'r'))
        loaded = _expand_var(loaded)
        if 'base_cfg' in loaded:
            bcfgpath = loaded['base_cfg']
            bcfg = _make_config(bcfgpath, False)
            del loaded['base_cfg']
            bcfg.update(loaded)
            loaded = bcfg
    _cfg.update(loaded)
    return _cfg


def launch(prj, dbg=False):
    assert 'WZDAT_DIR' in os.environ
    assert 'WZDAT_SOL_DIR' in os.environ
    wzpkg = _get_pkg()
    wzdir = os.environ['WZDAT_DIR']
    wzsol = os.environ['WZDAT_SOL_DIR']
    runopt = ""
    cmd = ""
    if dbg is not False:
        runopt = "-ti"
        cmd = "bash"
    else:
        runopt = "-d"
    cfg = _make_config()
    iport = cfg['host_ipython_port']
    iport = cfg['host_ipython_port']
    dport = cfg['host_dashboard_port']
    if 'data_dir' in cfg:
        datavol = '{}'.format(cfg['data_dir'])
    else:
        # for service systems, project logdata dir is  /logdata/{prj}
        datavol = '/logdata/{}'.format(prj)
    cmd = 'docker run {runopt} -p 22 -p {iport}:8090 -p {dport}:80\
            -p 873 --name "wzdat_{wzprj}"\
            -v {wzdir}:/wzdat -v {wzsol}:/solution\
            -v {datavol}:/logdata\
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