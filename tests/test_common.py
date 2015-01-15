import os

import pytest

from wzdat.make_config import make_config, invalidate_config
from wzdat.util import ChangeDir, get_var_dir, get_tmp_dir, get_hdf_dir,\
    get_cache_dir, get_conv_dir


@pytest.yield_fixture(scope='module')
def fxcfg():
    import tempfile
    tdir = tempfile.gettempdir()
    cfg = os.path.join(tdir, '_cfg_')
    bcfg = os.path.join(tdir, '_bcfg_')
    # remove previous configs
    if os.path.isfile(cfg):
        os.remove(cfg)
    if os.path.isfile(bcfg):
        os.remove(bcfg)
    # write content
    with open(cfg, 'wt') as cfgf:
        cfgf.write('''
base_cfg: {}
val1: 1
val2: 2
env1: $WZDAT_CFG/test_cfg
'''.format(bcfg))
    with open(bcfg, 'wt') as bcfgf:
        bcfgf.write('''
val0: 0
val2: -1
env0: $WZDAT_CFG/test_bcfg
''')
    yield cfg
    os.remove(cfg)
    os.remove(bcfg)
    invalidate_config()


def test_common_change_dir():
    cwd = os.getcwd()
    with ChangeDir('..'):
        assert cwd != os.getcwd()
    assert cwd == os.getcwd()


def test_common_get_dir():
    d = get_var_dir()
    assert '_var_' in d
    assert os.path.isdir(d)

    d = get_tmp_dir()
    assert '_var_/tmp' in d
    assert os.path.isdir(d)

    d = get_hdf_dir()
    assert '_var_/hdf' in d
    assert os.path.isdir(d)

    d = get_hdf_dir()
    assert '_var_/hdf' in d
    assert os.path.isdir(d)

    d = get_cache_dir()
    assert '_var_/cache' in d
    assert os.path.isdir(d)

    d = get_conv_dir()
    assert '_var_/conv' in d
    assert os.path.isdir(d)


def test_common_config(fxcfg):
    invalidate_config()
    cfg = make_config(fxcfg)
    assert type(cfg) == dict
    assert cfg['val0'] == 0
    assert cfg['val1'] == 1
    assert cfg['val2'] == 2
    cp = os.environ['WZDAT_CFG']
    cfgenv0 = os.path.join(cp, 'test_bcfg')
    cfgenv1 = os.path.join(cp, 'test_cfg')
    assert cfg['env0'] == cfgenv0
    assert cfg['env1'] == cfgenv1
