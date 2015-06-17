import re
import os

import wzdat
from wzdat.make_config import make_config

cfg = make_config()
localhost = os.environ['WZDAT_HOST']

TEST_IMPORT_ALL = False


def test_selector_basic(fxlogs):
    # Log
    log = fxlogs[0]
    update_err = wzdat.selector.get_load_errors()
    assert len(update_err) == 0
    assert len(log.files) == 450
    assert set(log.kinds) == set([log.kind.auth, log.kind.community,
                                  log.kind.game])
    assert set(log.nodes) == set([log.node.jp_node_1, log.node.jp_node_2,
                                  log.node.jp_node_3, log.node.kr_node_1,
                                  log.node.kr_node_2, log.node.kr_node_3,
                                  log.node.us_node_1, log.node.us_node_2,
                                  log.node.us_node_3])
    assert set(log.dates) == set([log.date.D2014_02_24, log.date.D2014_02_25,
                                  log.date.D2014_02_26, log.date.D2014_02_27,
                                  log.date.D2014_02_28, log.date.D2014_03_01,
                                  log.date.D2014_03_02, log.date.D2014_03_03,
                                  log.date.D2014_03_04, log.date.D2014_03_05])
    f = log.files[0]
    df = f.to_frame()
    assert len(df.columns) == 4

    # ExLog
    log = fxlogs[1]
    assert len(log.files) == 90
    f = log.files[0]
    assert 'ExLog' in f.path
    df = f.to_frame()
    assert len(df.columns) == 4

    # Dump
    dump = fxlogs[2]
    assert len(dump.files) == 27
    assert len(dump.kinds) == 3


def test_selector_dates(fxlogs):
    l = fxlogs[0]
    assert len(l.dates) == 10
    assert len(l.dates[-10:]) == 10
    assert len(l.dates[-14:]) == 10


def test_selector_value(fxlogs):
    log = fxlogs[0]
    f = log.files[0]
    assert isinstance(f, wzdat.selector.FileValue)
    assert f.path == 'jp/node-1/log/auth_2014-02-24.log'
    assert f.node == log.node.jp_node_1
    assert f.kind == log.kind.auth
    assert f.date == log.date.D2014_02_24
    assert f.size == 823
    assert f.hsize == str('823 bytes')
    assert f.cols == ['datetime', 'type', 'msg']

    assert f.nodes == [log.node.jp_node_1]
    assert f.kinds == [log.kind.auth]
    assert f.dates == [log.date.D2014_02_24]

    assert f.link.data == '<a href="http://{localhost}:8085/file/jp/node-1/'\
        'log/auth_2014-02-24.log">jp/node-1/log/auth_2014-02-24.log</a>'.\
        format(localhost=localhost)
    zl = re.compile(r'<a href="http://{localhost}:\d+/tmp/(wzdat-[^"]+)">(\1)'
                    r'</a>'.format(localhost=localhost)).search(f.zlink.data)
    assert zl is not None

    with open(log.files[0].abspath) as i:
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


def test_selector_fileselector(fxlogs):
    log = fxlogs[0]
    # log.files[0] == jp/node-1/log/auth_2014-02-24.log
    mf = log.files[0:2]
    assert isinstance(mf, wzdat.selector.FileSelector)

    assert hasattr(mf, 'path') is False
    assert hasattr(mf, 'node') is False
    assert hasattr(mf, 'kind') is False
    assert hasattr(mf, 'date') is False
    assert hasattr(mf, 'cols') is False

    assert mf.count == 2
    assert mf.size == 1646
    assert mf.hsize == '1.6 KB'

    assert mf.nodes == [log.node.jp_node_1]
    assert mf.kinds == [log.kind.auth]
    assert mf.dates == [log.date.D2014_02_24, log.date.D2014_02_25]
    assert mf.link.data ==\
        '<a href="http://{localhost}:8085/file/jp/node-1/log/auth_2014-02-24.log">'\
        'jp/node-1/log/auth_2014-02-24.log</a><br/><a href="http://{localhost}:8085'\
        '/file/jp/node-1/log/auth_2014-02-25.log">jp/node-1/log/auth_2014-02-25.log</a>'\
        .format(localhost=localhost)
    rx = r'<a href="http://{localhost}:\d+/tmp/(wzdat-[^"]+\.zip)">'\
         r'(\1)</a>'.format(localhost=localhost)
    zl = re.compile(rx).search(mf.zlink.data)
    assert zl is not None


def test_selector_hdf(fxlogs):
    log = fxlogs[0]
    from wzdat.util import HDF
    mf = log.files[log.kind.auth][:2]
    df = mf.to_frame()
    with HDF('test') as hdf:
        hdf.store['test_df'] = df

    with HDF('test') as hdf:
        sdf = hdf.store['test_df']

    assert sdf.equals(df)
