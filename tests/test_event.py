import time

import pytest

from wzdat.rundb import reset_db
from wzdat import event as evt


@pytest.yield_fixture(scope='function')
def db():
    reset_db()
    yield


def test_event(db):
    changed = evt.register_event(evt.FILE_MOVE_TO, '/test/path')
    assert changed == 1
    changed = evt.register_event(evt.FILE_DELETE, '/test/path')
    assert changed == 1

    events = evt.get_by_type(evt.FILE_MOVE_TO)
    assert len(events) == 1
    e = events[0]
    assert e[0] == 1
    assert e[1] == 2
    assert e[2] == evt.FILE_MOVE_TO
    assert e[3] == '/test/path'
    assert e[4] is not None  # event raised
    assert e[5] is None  # event handler
    assert e[6] is None  # event handled time

    events = evt.get_unhandled_events()
    assert len(events) == 2
    evt.mark_handled_events('file_cache', (1,))
    events = evt.get_unhandled_events()
    assert len(events) == 1
    events = evt.get_by_type(evt.FILE_MOVE_TO)
    e = events[0]
    assert e[5] is not None  # event handler
    assert e[6] is not None  # event handled time

    # trucate all
    changed = evt.remove_all()
    assert changed == 2

    # remove by raised time
    evt.register_event(evt.FILE_MOVE_TO, '/test/path')
    changed = evt.remove_by_raised(1)
    assert changed == 0
    time.sleep(2)
    changed = evt.remove_by_raised(1)
    assert changed == 1

    # remove by handled time
    evt.register_event(evt.FILE_MOVE_TO, '/test/path')
    evt.mark_handled_events('file_cache', (1,))
    changed = evt.remove_by_handled(1)
    assert changed == 0
    time.sleep(2)
    changed = evt.remove_by_handled(1)
    assert changed == 1

    evt.register_event(evt.FILE_MOVE_TO, '/test/path')
    evt.register_event(evt.FILE_DELETE, '/test/path')
    changed = evt.remove_by_type(evt.FILE_MOVE_TO)
    assert changed == 1
    changed = evt.remove_by_type(evt.FILE_MOVE_TO)
    assert changed == 0
    assert 1 == evt.remove_all()


def my_handler(row):
    return True


def test_event_dispatch(db):
    evt.register_event(evt.FILE_MOVE_TO, '/test/path')
    evt.register_handler(evt.FILE_MOVE_TO, my_handler)
    changed = evt.dispatch_events()  # default prior: 2
    assert changed == 1
    changed = evt.dispatch_events(1)
    assert changed == 0
    changed = evt.dispatch_events(3)
    assert changed == 0
