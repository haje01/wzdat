import os

from fabric.api import local, run, cd, env, abort

env.password = 'docker'
prj_map = {}


assert 'WZDAT_HOST' in os.environ


def docker_build_cmd(_nocache):
    return 'docker build {} '.format('--no-cache' if _nocache else '')


class _ChangeDir(object):
    def __init__(self, *dirs):
        self.cwd = os.getcwd()
        self.path = os.path.join(*dirs)

    def __enter__(self):
        assert os.path.isdir(self.path)
        os.chdir(self.path)

    def __exit__(self, atype, value, tb):
        os.chdir(self.cwd)


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


def push():
    local("git push")


def prepare():
    commit()
    push()


def remote_hosts():
    env.hosts = open('remote_hosts', 'r').readlines()


def gpull():
    with cd('~/wzdat'):
        run("git pull")


def dpull():
    with cd('~/wzdat'):
        run("docker pull")


def deploy(_dpull=False):
    with cd('~/wzdat'):
        run("git pull")
        if _dpull:
            build(True)
        relaunch_all(True)


def dpush():
    local('docker push haje01/wzdat-base')
    local('docker push haje01/wzdat')


def status(_remote=False):
    _container_cmd('ccmd_status', _remote)


def ccmd_status():
    run('supervisorctl status')


def ps():
    local('docker ps -a')


def _cmd(cmdstr, _remote=False):
    cmd = run if _remote else local
    cmd(cmdstr)


def _container_cmd(cmdstr, _remote):
    if _remote:
        with cd('~/wzdat'):
            run('fab remote_hosts {}'.format(cmdstr))
    else:
        wdir = os.environ['WZDAT_DIR']
        with _ChangeDir(wdir):
            local('fab docker_hosts {}'.format(cmdstr))


def rm_all(_remote=False):
    _cmd('docker rm -f $(docker ps -aq)', _remote)


def _build_base(_nocache, _remote):
    with open('../requirements.txt', 'r') as f:
        reqs = ['RUN pip install {}'.format(line.rstrip()) for line in
                f.readlines()]
    reqs.insert(0, '# WzDat requirements')
    with open('files/base.docker', 'r') as f:
        dockfile = f.read().replace('{%WZDAT_REQUIREMENTS%}', '\n'.join(reqs))
    with open('Dockerfile', 'w') as f:
        f.write(dockfile)

    _cmd(docker_build_cmd(_nocache) + '-t haje01/wzdat-base .', _remote)
    _cmd('rm -f Dockerfile', _remote)


def _build(_nocache, _remote):
    if _remote:
        r = run('docker images -q haje01/wzdat-base')
    else:
        r = local('docker images -q haje01/wzdat-base', capture=True)
    if len(r) == 0:
        print "No local 'haje01/wzdat-base' image. Trying to find in the "\
            "docker hub."
    _cmd('ln -fs files/self.docker Dockerfile', _remote)
    _cmd(docker_build_cmd(_nocache) + '-t haje01/wzdat .', _remote)
    _cmd('rm -f Dockerfile', _remote)


def _build_dev(_nocache, _remote):
    if _remote:
        r = run('docker images -q haje01/wzdat')
    else:
        r = local('docker images -q haje01/wzdat', capture=True)
    if len(r) == 0:
        print "No local 'haje01/wzdat' image. Trying to find in the docker"\
            " hub."
    _cmd('ln -fs files/dev/dev.docker Dockerfile', _remote)
    _cmd(docker_build_cmd(_nocache) + '--no-cache -t haje01/wzdat-dev .',
         _remote)
    _cmd('rm -f Dockerfile', _remote)


def build(_nocache=False, _remote=False):
    if _remote:
        with cd('system'):
            _build_base(_nocache, True)
            _build(_nocache, True)
            _build_dev(_nocache, True)
    else:
        with _ChangeDir('system'):
            _build_base(_nocache, False)
            _build(_nocache, False)
            _build_dev(_nocache, False)


def relaunch_all(_remote=False):
    rm_all(_remote)
    launch_all(_remote)


def cache(_remote=False):
    _container_cmd('ccmd_cache', _remote)


def ccmd_cache():
    run('python -m wzdat.jobs cache-all')


def runcron(_remote=False):
    _container_cmd('ccmd_runcron', _remote)


def ccmd_runcron():
    run('python -m wzdat.jobs run-all-cron-notebooks')


def launch_all(_remote=False):
    if _remote is True:
        _container_cmd('launch', True)
    else:
        prjs = os.environ['WZDAT_PRJS'].split(',')
        for prj in prjs:
            launch_prj(prj)


def _get_pkg():
    assert 'WZDAT_SOL_PKG' in os.environ
    return os.environ['WZDAT_SOL_PKG']


def _make_config(cfgpath):
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

    _cfg = {}
    if cfgpath is None:
        assert 'WZDAT_CFG' in os.environ
        cfgpath = os.environ['WZDAT_CFG']

    adir = os.path.dirname(cfgpath)
    afile = os.path.basename(cfgpath)

    with _ChangeDir(adir):
        loaded = yaml.load(open(afile, 'r'))
        loaded = _expand_var(loaded)
        if 'base_cfg' in loaded:
            bcfgpath = loaded['base_cfg']
            if os.path.isfile(bcfgpath):
                bcfg = _make_config(bcfgpath)
                del loaded['base_cfg']
                bcfg.update(loaded)
                loaded = bcfg
    _cfg.update(loaded)
    return _cfg


def rm_prj(prj):
    local('docker rm -f wzdat_{prj}'.format(prj=prj))


def launch_prj(prj, dbg=False):
    assert 'WZDAT_DIR' in os.environ
    assert 'WZDAT_SOL_DIR' in os.environ
    wzpkg = _get_pkg()
    wzdir = os.environ['WZDAT_DIR']
    wzsol = os.environ['WZDAT_SOL_DIR']
    wzhost = _get_host()
    #wzhost = os.environ['WZDAT_HOST'] if 'WZDAT_B2DHOST' not in os.environ else \
    #    os.environ['WZDAT_B2DHOST']
    runopt = ""
    cmd = ""
    if dbg is not False:
        runopt = "-ti"
        cmd = "bash"
    else:
        runopt = "-d"
    cfg_path = os.path.join(wzsol, wzpkg, prj, 'config.yml')
    cfg = _make_config(cfg_path)
    iport = cfg['host_ipython_port']
    dport = cfg['host_dashboard_port']

    if 'expose_redis' in cfg and cfg['expose_redis']:
        exredis = '-p 6379:6379'
    else:
        exredis = ''

    if 'data_dir' in cfg:
        datavol = '{}'.format(cfg['data_dir'])
    else:
        # for service systems, project logdata dir is  /logdata/{prj}
        datavol = '/logdata/{}'.format(prj)

    cmd = 'docker run {runopt} -p 22 -p {iport}:8090 -p {dport}:80\
            -p 873 {exredis} --name "wzdat_{wzprj}"\
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
                                           datavol=datavol, cmd=cmd,
                                           exredis=exredis)
    local(cmd)


def _get_host():
    host = os.environ['DOCKER_HOST'] if 'DOCKER_HOST' in os.environ else\
        'tcp://0.0.0.0'
    return host.split(':')[1][2:]


def ssh(_prj):
    for prj, port in _get_prj_and_ports():
        if prj == _prj:
            local('ssh root@{host} -p {port}'.format(host=_get_host(),
                                                     port=port))
            return
    abort("Can't find project")


def log(prj):
    local('docker logs -f wzdat_{prj}'.format(prj=prj))


def restart(prj):
    local('docker exec wzdat_{prj} supervisorctl restart uwsgi '
          'celery update_notebooks'.format(prj=prj))


def restart_all():
    prjs = os.environ['WZDAT_PRJS'].split(',')
    for prj in prjs:
        restart(prj)


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


def docker_hosts(prj=None):
    hosts = []
    for _prj, port in _get_prj_and_ports():
        if prj is None or prj == _prj:
            host = 'root@{}:{}'.format(_get_host(), port)
            hosts.append(host)
            prj_map[host] = _prj
    env.hosts = hosts
