import os

from wzdat.util import ChangeDir


def test_common_change_dir():
    cwd = os.getcwd()
    with ChangeDir('..'):
        assert cwd != os.getcwd()
    assert cwd == os.getcwd()
