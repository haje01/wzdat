import os
import time
import logging
from collections import defaultdict

from wzdat.rundb import Cursor
from wzdat.make_config import make_config

cfg = make_config()

MAX_PRIOR = 3
DEFAULT_PRIOR = 2
RUNNER_DB_PATH = cfg['runner_db_path']

# WzDat Built-in Events
FILE_WRITE = 'FILE_WRITE'
FILE_DELETE = 'FILE_DELETE'

handlers = []
for i in range(MAX_PRIOR):
    handlers.append(defaultdict(list))


def register_event(etype, info, prior=DEFAULT_PRIOR):
    raised = time.time()
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('INSERT INTO event (prior, type, info, raised) VALUES (?,'
                    '?, ?, ?)', (prior, etype, info, raised))
        return cur.con.total_changes


def get_events(etype=None):
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('SELECT * FROM event WHERE type=?', (etype,))
        return cur.fetchall()


def mark_handled_events(handler, event_ids):
    with Cursor(RUNNER_DB_PATH) as cur:
        handled = time.time()
        eids = "({})".format(', '.join([str(i) for i in event_ids]))
        cur.execute('UPDATE event SET handler=?, handled=? WHERE id in'
                    '{}'.format(eids), (handler.__name__, handled))
        return cur.con.total_changes


def truncate_handled_events(oldsec=None):
    with Cursor(RUNNER_DB_PATH) as cur:
        if oldsec is None:
            cur.execute('DELETE FROM event WHERE raised is not NULL')
        else:
            old = time.time() - oldsec
            cur.execute('DELETE FROM event WHERE raised < ?', (old,))
        return cur.con.total_changes


def register_event_by_inotify(inotifymsg):
    adir, events, afile = inotifymsg.split()
    events = events.split(',')
    ee = None
    if 'CLOSE_WRITE' in events:
        afile = '.'.join(afile.split('.')[1:-1])
        path = os.path.join(adir, afile)
        ee = FILE_WRITE
    elif 'DELETE' in events:
        path = os.path.join(adir, afile)
        ee = FILE_DELETE
    if ee is not None:
        register_event(ee, path)
    else:
        logging.error('Unknown Event: {}'.format(ee))


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


# shortcut for shell command
if __name__ == "__main__":
    import sys
    # read inotifywait output
    inotify_output = sys.stdin.read()
    logging.debug('register event by inoitywait: {}'.format(inotify_output))
    register_event_by_inotify(inotify_output)
