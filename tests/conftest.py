import pytest

from wzdat.rundb import r
from wzdat.make_config import make_config

cfg = make_config()


@pytest.yield_fixture(scope="session")
def fxlogs():
    from wzdat.util import gen_dummydata
    ddir = cfg['data_dir']
    # remove previous dummy data
    import shutil
    shutil.rmtree(ddir)

    # generate new dummy data
    gen_dummydata(ddir)
    from ws_mysol.myprj import log
    log.load_info()
    from ws_mysol.myprj import exlog
    exlog.load_info()
    from ws_mysol.myprj import dump
    dump.load_info()
    yield log, exlog, dump


@pytest.yield_fixture(scope='module')
def fxdb():
    r.delete('*')
    yield
