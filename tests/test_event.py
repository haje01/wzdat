import pytest

from wzdat.rundb import flush_unhandled_events, register_event,\
    unhandled_events
from wzdat import event as evt


@pytest.yield_fixture(scope='function')
def db():
    flush_unhandled_events()
    yield


def test_event(db):
    register_event(evt.FILE_MOVE_TO, '/test/path')
    register_event(evt.FILE_DELETE, '/test/path')

    events = unhandled_events()
    assert len(events) == 2
