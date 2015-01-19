import os
import sys
import time
import logging
from collections import defaultdict

from wzdat.rundb import Cursor
from wzdat.make_config import make_config
from wzdat.const import FORWARDER_LOG_PREFIX
from wzdat.util import get_htimestamp

cfg = make_config()

MAX_PRIOR = 3
DEFAULT_PRIOR = 2
RUNNER_DB_PATH = cfg['runner_db_path']

# WzDat Built-in Events
FILE_MOVE_TO = 'FILE_MOVE_TO'
FILE_DELETE = 'FILE_DELETE'

handlers = []
for i in range(MAX_PRIOR):
    handlers.append(defaultdict(list))


def register_event(etype, info, prior=DEFAULT_PRIOR):
    # skip forwarder files
    if FORWARDER_LOG_PREFIX in info:
        return
    logging.debug('register_event {} - {}'.format(etype, info))
    raised = get_htimestamp()
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('INSERT INTO event (prior, type, info, raised) VALUES (?,'
                    '?, ?, ?)', (prior, etype, info, raised))
        return cur.con.total_changes


def get_all():
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('SELECT * FROM event')
        return cur.fetchall()


def get_by_type(etype):
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('SELECT * FROM event WHERE type=?', (etype,))
        return cur.fetchall()


def get_unhandled_events():
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('SELECT * FROM event WHERE handled is NULL')
        return cur.fetchall()


def mark_handled_events(handler, event_ids):
    with Cursor(RUNNER_DB_PATH) as cur:
        handled = get_htimestamp()
        eids = "({})".format(', '.join([str(i) for i in event_ids]))
        shandler = handler if isinstance(handler, str) else handler.__name__
        cur.execute('UPDATE event SET handler=?, handled=? WHERE id in'
                    '{}'.format(eids), (shandler, handled))
        return cur.con.total_changes


def remove_all():
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('DELETE FROM event')
        return cur.con.total_changes


def remove_by_handled(agesec=None):
    with Cursor(RUNNER_DB_PATH) as cur:
        if agesec is None:
            cur.execute('DELETE FROM event WHERE handled is not NULL')
        else:
            old = get_htimestamp(time.time() - agesec)
            cur.execute('DELETE FROM event WHERE handled < ?', (old,))
        return cur.con.total_changes


def remove_by_type(etype):
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('DELETE FROM event WHERE type = ?', (etype,))
        return cur.con.total_changes


def remove_by_raised(agesec):
    with Cursor(RUNNER_DB_PATH) as cur:
        old = get_htimestamp(time.time() - agesec)
        cur.execute('DELETE FROM event WHERE raised < ?', (old,))
        return cur.con.total_changes


def register_handler(etype, handler, prior=DEFAULT_PRIOR):
    handlers[prior - 1][etype].append(handler)


def dispatch_events(prior=DEFAULT_PRIOR):
    """Dispatch events with matching priority."""
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('SELECT * FROM event WHERE prior=?', (prior,))
        handled_ids = []
        for row in cur.fetchall():
            eid = row[0]
            etype = row[2]
            handled_time = row[6]
            if handled_time is None:
                for handler in handlers[prior - 1][etype]:
                    handled = handler(row)
                    if handled:
                        handled_ids.append(eid)
            if len(handled_ids) > 0:
                return mark_handled_events(handler, handled_ids)
    return 0


def watch_files(target_dir):
    logging.debug('watch_files')
    import pyinotify
    excl_lst = [
        '/logdata/_var_',
    ]

    wm = pyinotify.WatchManager()
    mask = pyinotify.IN_CREATE | pyinotify.IN_MOVED_TO | pyinotify.IN_DELETE

    class FileEventHandler(pyinotify.ProcessEvent):
        def process_IN_MOVED_TO(self, event):
            register_event(FILE_MOVE_TO, event.pathname)

        def process_IN_DELETE(self, event):
            register_event(FILE_DELETE, event.pathname)

    assert os.path.isdir(target_dir)

    handler = FileEventHandler()
    pyinotify.AsyncNotifier(wm, handler)

    excl = pyinotify.ExcludeFilter(excl_lst)
    wm.add_watch(target_dir, mask, rec=True, auto_add=True,
                 exclude_filter=excl)

    import asyncore
    asyncore.loop()


# shortcut for shell command
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Usage: event.py {directory_to_watch}"
        sys.exit(-1)

    target_dir = sys.argv[1]
    watch_files(target_dir)
