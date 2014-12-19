import os
import time
import logging

from wzdat.rundb import Cursor
from wzdat.make_config import make_config

cfg = make_config()

RUNNER_DB_PATH = cfg['runner_db_path']

# WzDat Built-in Events
FILE_NEW = 'FILE_NEW'
FILE_DELETE = 'FILE_DELETE'


def register_event(etype, info):
    raised = time.time()
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('INSERT INTO event (type, info, raised) VALUES (?, ?, ?)',
                    (etype, info, raised))
        return cur.con.total_changes


def get_events(etype=None):
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('SELECT * FROM event WHERE type=?', (etype,))
        return cur.fetchall()


def handle_events(handler, event_ids):
    with Cursor(RUNNER_DB_PATH) as cur:
        handled = time.time()
        eids = "({})".format(', '.join([str(i) for i in event_ids]))
        cur.execute('UPDATE event SET handler=?, handled=? WHERE id in'
                    '{}'.format(eids), (handler, handled))
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
    path = os.path.join(adir, afile)
    if 'MOVED_TO' in events:
        register_event(FILE_NEW, path)
    elif 'DELETE' in events:
        register_event(FILE_DELETE, path)


# shortcut for shell command
if __name__ == "__main__":
    import sys
    # read inotifywait output
    inotify_output = sys.stdin.read()
    logging.debug('register event by inoitywait: {}'.format(inotify_output))
    register_event_by_inotify(inotify_output)
