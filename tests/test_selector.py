import re
import os

import pytest

import wzdat
from wzdat.make_config import make_config
from ws_mysol.myprj import log as l


cfg = make_config()
localhost = os.environ['WZDAT_HOST']


@pytest.yield_fixture(scope="session")
def dummy():
    from wzdat.util import gen_dummydata
    ddir = cfg['data_dir']
    gen_dummydata(cfg['data_dir'])
    yield
    import shutil
    shutil.rmtree(ddir)


def test_selector_basic():
    assert len(l.files) == 450
    assert set(l.kinds) == set([l.kind.auth, l.kind.community, l.kind.game])
    assert set(l.nodes) == set([l.node.jp_node_1, l.node.jp_node_2,
                                l.node.jp_node_3, l.node.kr_node_1,
                                l.node.kr_node_2, l.node.kr_node_3,
                                l.node.us_node_1, l.node.us_node_2,
                                l.node.us_node_3])
    assert set(l.dates) == set([l.date.D2014_02_24,
                                l.date.D2014_02_25,
                                l.date.D2014_02_26,
                                l.date.D2014_02_27,
                                l.date.D2014_02_28,
                                l.date.D2014_03_01,
                                l.date.D2014_03_02,
                                l.date.D2014_03_03,
                                l.date.D2014_03_04,
                                l.date.D2014_03_05])


def test_selector_value():
    f = l.files[0]
    assert isinstance(f, wzdat.selector.FileValue)
    assert f.path == 'jp/node-1/auth_2014-02-24.log'
    assert f.node == l.node.jp_node_1
    assert f.kind == l.kind.auth
    assert f.date == l.date.D2014_02_24
    assert f.size == 823
    assert f.hsize == str('823 bytes')
    assert f.cols == ['datetime', 'type', 'msg']

    assert f.nodes == [l.node.jp_node_1]
    assert f.kinds == [l.kind.auth]
    assert f.dates == [l.date.D2014_02_24]

    assert f.link.data == '<a href="http://{localhost}:8085/file/jp/node-1/'\
        'auth_2014-02-24.log">jp/node-1/auth_2014-02-24.log</a>'.\
        format(localhost=localhost)
    zl = re.compile(r'<a href="http://{localhost}:\d+/tmp/(wzdat-[^"]+)">(\1)'
                    r'</a>'.format(localhost=localhost)).search(f.zlink.data)
    assert zl is not None

    with open(l.files[0].abspath) as i:
        assert i is not None
    f.head(5) == '''2014-02-24 00:00 [DEBUG] - Alloc
    2014-02-24 01:00 [INFO] - Move
    2014-02-24 02:00 [WARNING] - Mismatch
    2014-02-24 03:00 [ERROR] - Async
    2014-02-24 04:00 [CRITICAL] - Failed'''
    f.tail(5) == '''2014-02-24 19:00 [CRITICAL] - Failed
    2014-02-24 20:00 [DEBUG] - Alloc
    2014-02-24 21:00 [INFO] - Move
    2014-02-24 22:00 [WARNING] - Mismatch
    2014-02-24 23:00 [ERROR] - Async'''


def test_selector_fileselector():
    # l.files[0] == jp/node-1/auth_2014-02-24.log
    mf = l.files[0:2]
    assert isinstance(mf, wzdat.selector.FileSelector)

    assert hasattr(mf, 'path') is False
    assert hasattr(mf, 'node') is False
    assert hasattr(mf, 'kind') is False
    assert hasattr(mf, 'date') is False
    assert hasattr(mf, 'cols') is False

    assert mf.count == 2
    assert mf.size == 1646
    assert mf.hsize == '1.6 KB'

    assert mf.nodes == [l.node.jp_node_1]
    assert mf.kinds == [l.kind.auth]
    assert mf.dates == [l.date.D2014_02_24, l.date.D2014_02_25]
    assert mf.link.data == '<a href="http://{localhost}:8085/file/jp/node-1/'\
        'auth_2014-02-24.log">jp/node-1/auth_2014-02-24.log</a><br/><a href='\
        '"http://{localhost}:8085/file/jp/node-1/auth_2014-02-25.log">jp/node-1'\
        '/auth_2014-02-25.log</a>'.format(localhost=localhost)
    rx = r'<a href="http://{localhost}:\d+/tmp/(wzdat-[^"]+\.zip)">'\
         r'(\1)</a>'.format(localhost=localhost)
    zl = re.compile(rx).search(mf.zlink.data)
    assert zl is not None


def test_selector_hdf():
    from wzdat.util import HDF
    mf = l.files[l.kind.auth][:2]
    df = mf.to_frame()
    with HDF('test') as hdf:
        hdf.store['test_df'] = df

    with HDF('test') as hdf:
        sdf = hdf.store['test_df']

    assert sdf.equals(df)
