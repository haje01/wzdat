import os

from wzdat.util import ChangeDir, get_var_dir, get_tmp_dir, get_hdf_dir,\
    get_cache_dir, get_conv_dir


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
