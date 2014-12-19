import time

import pytest

from wzdat.rundb import create_db, destroy_db
from wzdat import event as evt


@pytest.yield_fixture(scope='function')
def db():
    destroy_db()
    create_db()
    yield


def test_event(db):
    changed = evt.register_event(evt.FILE_NEW, '/test/path')
    assert changed == 1
    changed = evt.register_event(evt.FILE_DELETE, '/test/path')
    assert changed == 1

    events = evt.get_events(evt.FILE_NEW)
    assert len(events) == 1
    e = events[0]
    assert e[0] == 1
    assert e[1] == evt.FILE_NEW
    assert e[2] == '/test/path'
    assert e[3] is not None  # event raised
    assert e[4] is None  # event handler
    assert e[5] is None  # event handled time

    evt.handle_events('file_cache', (1,))
    events = evt.get_events(evt.FILE_NEW)
    e = events[0]
    assert e[4] is not None  # event handler
    assert e[5] is not None  # event handled time

    # trucate immediately
    changed = evt.truncate_handled_events()
    assert changed == 2

    # truncate by time
    evt.register_event(evt.FILE_NEW, '/test/path')
    evt.handle_events('file_cache', (1,))
    changed = evt.truncate_handled_events(1)
    assert changed == 0
    time.sleep(2)
    changed = evt.truncate_handled_events(1)
    assert changed == 1


def test_event_inotify(db):
    evt.register_event_by_inotify('/logdata/kr/login1/dblog/ MOVED_TO '
                                  'T_ActionLog20141219.csv')
    events = evt.get_events(evt.FILE_NEW)
    assert len(events) == 1
