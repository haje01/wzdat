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
    changed = evt.register_event(evt.FILE_WRITE, '/test/path')
    assert changed == 1
    changed = evt.register_event(evt.FILE_DELETE, '/test/path')
    assert changed == 1

    events = evt.get_events(evt.FILE_WRITE)
    assert len(events) == 1
    e = events[0]
    assert e[0] == 1
    assert e[1] == 2
    assert e[2] == evt.FILE_WRITE
    assert e[3] == '/test/path'
    assert e[4] is not None  # event raised
    assert e[5] is None  # event handler
    assert e[6] is None  # event handled time

    evt.mark_handled_events('file_cache', (1,))
    events = evt.get_events(evt.FILE_WRITE)
    e = events[0]
    assert e[5] is not None  # event handler
    assert e[6] is not None  # event handled time

    # trucate immediately
    changed = evt.truncate_handled_events()
    assert changed == 2

    # truncate by time
    evt.register_event(evt.FILE_WRITE, '/test/path')
    evt.mark_handled_events('file_cache', (1,))
    changed = evt.truncate_handled_events(1)
    assert changed == 0
    time.sleep(2)
    changed = evt.truncate_handled_events(1)
    assert changed == 1


def test_event_inotify(db):
    evt.register_event_by_inotify('/logdata/kr/login1/dblog/ CLOSE_WRITE,CLOSE'
                                  ' .T_ActionLog20141219.csv.eK7Nkf')
    events = evt.get_events(evt.FILE_WRITE)
    assert len(events) == 1
    assert events[0][3] == 'T_ActionLog20141219.csv'

    evt.register_event_by_inotify('/logdata/kr/login1/dblog/ DELETE'
                                  ' T_ActionLog20141219.csv')
    events = evt.get_events(evt.FILE_DELETE)
    assert len(events) == 1
    assert events[0][3] == 'T_ActionLog20141219.csv'


def my_handler(row):
    return True


def test_event_dispatch(db):
    evt.register_event(evt.FILE_WRITE, '/test/path')
    evt.register_handler(evt.FILE_WRITE, my_handler)
    changed = evt.dispatch_events()  # default prior: 2
    assert changed == 1
    changed = evt.dispatch_events(1)
    assert changed == 0
    changed = evt.dispatch_events(3)
    assert changed == 0
