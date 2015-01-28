"""
    Database module for IPython Notebook Runner
"""

import os
import sys
import time
import logging

import sqlite3

from wzdat.make_config import make_config
from wzdat.util import ChangeDir

cfg = make_config()
RUNNER_DB_PATH = cfg['runner_db_path']

sys.path.append('.')


class Cursor(object):
    def __init__(self, db_path):
        self.con = None
        self.cur = None
        self.db_path = db_path

    def __enter__(self):
        try:
            con = sqlite3.connect(self.db_path)
        except Exception, e:
            logging.error(str(e))
            raise
        else:
            self.con = con
            self.cur = con.cursor()
        return self

    def __exit__(self, _type, value, tb):
        if self.con is not None:
            self.con.commit()
            self.con.close()

    def execute(self, *args):
        assert self.cur is not None
        return self.cur.execute(*args)

    def fetchone(self):
        assert self.cur is not None
        return self.cur.fetchone()

    def fetchall(self):
        assert self.cur is not None
        return self.cur.fetchall()

    def log_changes(self):
        logging.debug("changed rows {}".format(self.con.total_changes))


def reset_db():
    remove_db_file()
    open(RUNNER_DB_PATH, 'a').close()  # touch
    create_db()


def create_db():
    """Create DB tables if not exist."""
    with Cursor(RUNNER_DB_PATH) as cur:
        # view notebook info
        cur.execute('CREATE TABLE IF NOT EXISTS info '
                    '(path TEXT PRIMARY KEY, start REAL, error TEXT, elapsed '
                    'REAL, cur INT, total INT);')
        # remember latest file cache
        cur.execute('CREATE TABLE IF NOT EXISTS cache '
                    '(id INTEGER PRIMARY KEY, time REAL);')
        # cache for file finder
        cur.execute('CREATE TABLE IF NOT EXISTS finder '
                    '(ft TEXT PRIMARY KEY, dates TEXT, kinds TEXT, nodes'
                    ' TEXT);')
        # cron notebook info
        cur.execute('CREATE TABLE IF NOT EXISTS cron '
                    '(path TEXT PRIMARY KEY, sched TEXT);')

        # event
        cur.execute('CREATE TABLE IF NOT EXISTS event '
                    '(id INTEGER PRIMARY KEY, prior INTEGER, type TEXT, info '
                    'TEXT, raised TEXT, handler TEXT, handled TEXT);')


def destroy_db():
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('DROP TABLE IF EXISTS info;')
        cur.execute('DROP TABLE IF EXISTS cache;')
        cur.execute('DROP TABLE IF EXISTS finder;')
        cur.execute('DROP TABLE IF EXISTS cron;')
        cur.execute('DROP TABLE IF EXISTS event;')


def remove_db_file():
    if os.path.isfile(RUNNER_DB_PATH):
        os.remove(RUNNER_DB_PATH)


def start_run(path, total):
    with Cursor(RUNNER_DB_PATH) as cur:
        _start_run(cur, path, total)


def _start_run(cur, path, total):
    cur.execute('SELECT * FROM info WHERE path=?', (path,))
    start = time.time()
    if cur.fetchone() is None:
        cur.execute('INSERT INTO info(path, start, total) VALUES(?, ?, ?)',
                    (path, start, total))
    else:
        cur.execute('UPDATE info SET error=NULL, start=?, elapsed=NULL, '
                    'cur=0, total=? WHERE path=?', (start, total, path))
        cur.log_changes()


def finish_run(path, err):
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('SELECT start, total FROM info WHERE path=?', (path,))
        rv = cur.fetchone()
        if rv is not None:
            start = rv[0]
            total = rv[1]
            elapsed = time.time() - start if err is None else None
            cur.execute('UPDATE info SET error=?, elapsed=?, cur=? WHERE '
                        'path=?', (err, elapsed, total, path))
            cur.log_changes()


def update_run_info(path, curcell):
    with Cursor(RUNNER_DB_PATH) as cur:
        _update_run_info(cur, path, curcell)


def _update_run_info(cur, path, curcell):
    cur.execute('UPDATE info SET cur=? WHERE path=?', (curcell, path))


def get_run_info(path):
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('SELECT start, elapsed, cur, total, error FROM info WHERE '
                    'path=?', (path,))
        return cur.fetchone()


def update_cache_info():
    logging.debug('update_cache_info')
    with Cursor(RUNNER_DB_PATH) as cur:
        _update_cache_info(cur)


def _update_cache_info(cur):
    cur.execute('INSERT INTO cache (time) VALUES (?)', (time.time(),))
    # trim old
    cur.execute('DELETE FROM cache WHERE ID NOT IN (SELECT id FROM Cache '
                'ORDER BY id DESC limit 10)')


def get_cache_info():
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('SELECT time FROM cache ORDER BY id DESC LIMIT 1')
        return cur.fetchone()


def update_finder_info(info):
    logging.debug('update_finder_info')
    with Cursor(RUNNER_DB_PATH) as cur:
        try:
            # delete old
            cur.execute('DELETE FROM finder')
        except sqlite3.OperationalError, e:
            logging.error(str(e))
            return

        for ft, _dates, _kinds, _nodes in info:
            dates = ','.join(_dates)
            kinds = ','.join(_kinds)
            nodes = ','.join(_nodes)
            # insert new
            cur.execute('INSERT INTO finder (ft, dates, kinds, nodes) VALUES'
                        '(?, ?, ?, ?)', (ft, dates, kinds, nodes))


def save_cron(paths, scheds):
    with Cursor(RUNNER_DB_PATH) as cur:
        for i, path in enumerate(paths):
            cur.execute('SELECT count(*) FROM cron WHERE path=?', (path,))
            if cur.fetchone()[0] > 0:
                continue
            sched = scheds[i]
            cur.execute('INSERT INTO cron (path, sched) VALUES (?, ?)',
                        (path, sched))


def get_cron_notebooks():
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('SELECT path FROM cron')
        return [r[0] for r in cur.fetchall()]


def get_finder_info():
    logging.debug('get_finder_info')
    with Cursor(RUNNER_DB_PATH) as cur:
        try:
            cur.execute('SELECT * FROM finder')
        except sqlite3.OperationalError, e:
            logging.error(str(e))
            return

        ret = []
        for row in cur.fetchall():
            ft = row[0]
            dates = row[1].split(',')
            kinds = row[2].split(',')
            nodes = row[3].split(',')
            ret.append((ft, dates, kinds, nodes))

        return ret


if __name__ == "__main__":
    # TODO: Use argh
    print "Current DB: " + RUNNER_DB_PATH
    if len(sys.argv) == 1:
        print "Commands: create, destroy"
    else:
        cmd = sys.argv[1]
        if cmd == 'create':
            create_db()
        elif cmd == 'destroy':
            destroy_db()
        else:
            print "Unregistered command: " + cmd


def cache_files():
    logging.debug('cache_files')
    with ChangeDir(cfg['sol_dir']):
        # prevent using cache
        if 'file_types' not in cfg:
            logging.warning('no file_types in cfg. exit')
            return
        old_use_cache = cfg['use_cache']
        prj = cfg['prj']
        print "Caching files for: %s" % prj
        datadir = cfg['data_dir']
        pkg = cfg['sol_pkg']
        ftypes = cfg['file_types']
        for ftype in ftypes:
            cmd = ['from %s.%s.%s import find_files_and_save; '
                   'find_files_and_save("%s")' % (pkg, prj, ftype, datadir)]
            cmd = ' '.join(cmd)
            exec(cmd)
        update_cache_info()
        cfg['use_cache'] = old_use_cache


def cache_finder():
    import imp
    # Make cache for file finder.
    logging.debug('cache_finder')
    with ChangeDir(cfg['sol_dir']):
        if 'file_types' not in cfg:
            logging.warning('no file_types in cfg. exit')
            return
        ret = []
        if ret is None or len(ret) == 0:
            pkg = cfg['sol_pkg']
            prj = cfg['prj']
            ftypes = cfg["file_types"]
            sol_dir = cfg['sol_dir']
            os.chdir(sol_dir)
            ret = []
            for ft in ftypes:
                mpath = '%s/%s/%s.py' % (pkg, prj, ft)
                mod = imp.load_source('%s' % ft,  mpath)
                dates = [str(date) for date in mod.dates[:-15:-1]]
                kinds = [str(kind) for kind in mod.kinds.group()]
                nodes = [str(node) for node in mod.nodes]
                info = ft, dates, kinds, nodes
                ret.append(info)
            update_finder_info(ret)
        return ret
