import os
import sys
import logging
from collections import defaultdict

from wzdat.make_config import make_config
from wzdat.rundb import register_event

cfg = make_config()

MAX_PRIOR = 3
RUNNER_DB_PATH = cfg['runner_db_path']

# WzDat Built-in Events
FILE_MOVE_TO = 'FILE_MOVE_TO'
FILE_DELETE = 'FILE_DELETE'


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
