"""
    Database module for IPython Notebook Runner
"""

import sys
import time
import logging

import sqlite3

from wzdat.const import RUNNER_DB_PATH

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


def create_db():
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('CREATE TABLE IF NOT EXISTS info '
                    '(path TEXT PRIMARY KEY, start REAL, elapsed REAL, cur INT'
                    ', total INT);')
        cur.execute('CREATE TABLE IF NOT EXISTS cache '
                    '(id INTEGER PRIMARY KEY, time REAL);')
        cur.execute('CREATE TABLE IF NOT EXISTS finder '
                    '(ft TEXT PRIMARY KEY, dates TEXT, kinds TEXT, nodes'
                    ' TEXT);')


def destroy_db():
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('DROP TABLE IF EXISTS info;')
        cur.execute('DROP TABLE IF EXISTS cache;')
        cur.execute('DROP TABLE IF EXISTS finder;')


def start_run(path, total):
    with Cursor(RUNNER_DB_PATH) as cur:
        path = path.decode('utf8')
        cur.execute('SELECT * FROM info WHERE path=?', (path,))
        start = time.time()
        if cur.fetchone() is None:
            cur.execute('INSERT INTO info(path, start, total) VALUES(?, ?, ?)',
                        (path, start, total))
        else:
            cur.execute('UPDATE info SET start=?, elapsed=NULL, cur=0, total=?'
                        ' WHERE path=?', (start, total, path))


def finish_run(path):
    with Cursor(RUNNER_DB_PATH) as cur:
        path = path.decode('utf8')
        cur.execute('SELECT start, total FROM info WHERE path=?', (path,))
        rv = cur.fetchone()
        if rv is not None:
            start = rv[0]
            total = rv[1]
            elapsed = time.time() - start
            cur.execute('UPDATE info SET elapsed=?, cur=? WHERE path=?',
                        (elapsed, total, path))


def update_run_info(path, curcell):
    with Cursor(RUNNER_DB_PATH) as cur:
        path = path.decode('utf8')
        cur.execute('SELECT start FROM info WHERE path=?', (path,))
        rv = cur.fetchone()
        if rv is not None:
            cur.execute('UPDATE info SET cur=? WHERE path=?', (curcell, path))


def get_run_info(path):
    with Cursor(RUNNER_DB_PATH) as cur:
        path = path.decode('utf8')
        cur.execute('SELECT start, elapsed, cur, total FROM info WHERE path=?',
                    (path,))
        return cur.fetchone()


def update_cache_info():
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('INSERT INTO cache (time) VALUES (?)', (time.time(),))
        # trim old
        cur.execute('DELETE FROM cache WHERE ID NOT IN (SELECT id FROM Cache '
                    'ORDER BY id DESC limit 10)')


def get_cache_info():
    with Cursor(RUNNER_DB_PATH) as cur:
        cur.execute('SELECT time from cache ORDER BY id DESC LIMIT 1')
        return cur.fetchone()


def update_finder_info(info):
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


def get_finder_info():
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
