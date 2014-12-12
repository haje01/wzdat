import pytest

from wzdat.make_config import make_config
from ws_mysol.myprj import log as l


cfg = make_config()


@pytest.yield_fixture(scope="session")
def dummy():
    from wzdat.util import gen_dummydata
    gen_dummydata(cfg['data_dir'])
    yield
    # TODO: del_dummydata


def test_selector():
    assert len(l.files) == 180
    assert set(l.kinds) == set([l.kind.auth, l.kind.community])
    assert set(l.nodes) == set([l.node.jp_node_1, l.node.jp_node_2,
                                l.node.jp_node_3, l.node.kr_node_1,
                                l.node.kr_node_2, l.node.kr_node_3,
                                l.node.us_node_1, l.node.us_node_2,
                                l.node.us_node_3])
