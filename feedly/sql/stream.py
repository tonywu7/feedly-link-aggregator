# MIT License
#
# Copyright (c) 2020 Tony Wu <tony[dot]wu(at)nyu[dot]edu>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import logging
import sqlite3
from collections import deque

from ..utils import watch_for_timing
from . import SCHEMA_VERSION
from .factory import create_helpers
from .utils import (count_rows, create_all, create_indices, drop_indices,
                    is_locked, mark_as_locked, mark_as_unlocked,
                    verify_version)

BEGIN = 'BEGIN;'
END = 'END;'
FOREIGN_KEY_ON = 'PRAGMA foreign_keys = ON;'
FOREIGN_KEY_OFF = 'PRAGMA foreign_keys = OFF;'


class DatabaseWriter:
    def __init__(self, path, tables, models, buffering, debug=False):
        self.db_path = path
        self.log = logging.getLogger('db.writer')

        conn = sqlite3.connect(path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        self._queues = {t: deque() for t in tables}
        self._conn = conn

        self._closed = False

        self._setup_debug(debug)

        verify_version(conn, SCHEMA_VERSION)
        create_all(conn)
        drop_indices(conn)
        conn.execute(FOREIGN_KEY_OFF)
        self._lock_db()

        self._tables = tables
        self.buffering = buffering
        self._recordcount = 0

        self._load_helpers(models)
        self._load_foreign_keys()
        for create_trigger in self._create_trigger.values():
            create_trigger(conn)
        self.flush()

        self._rowcounts = {t: None for t in tables}
        self._tally()

    def _lock_db(self):
        unclean = False
        if is_locked(self._conn):
            self.log.warn('Database lock table exists')
            unclean = True
        mark_as_locked(self._conn)
        if unclean:
            self.log.warn('Previous crawler did not exit properly')

    def _setup_debug(self, debug):
        if debug:
            sql_log = logging.getLogger('db.sql')
            sql_log.propagate
            self._conn.set_trace_callback(sql_log.debug)
            if debug is not True:
                handler = logging.StreamHandler(open(debug, 'w+'))
                sql_log.addHandler(handler)

    def _load_foreign_keys(self):
        update_funcs = {}
        for table in self._tables:
            update_funcs[table] = conf = {}
            for row in self._conn.execute(f'PRAGMA foreign_key_list({table})'):
                conf[row['id']] = self._update_foreign[table][row['from']]
        self._update_foreign = update_funcs

    def _load_helpers(self, models):
        self._insert_into = {}
        self._remove_duplicate = {}
        self._update_foreign = {}
        self._create_trigger = {}
        self._drop_trigger = {}
        helpers = {
            'ins': self._insert_into,
            'dup': self._remove_duplicate,
            'ufk': self._update_foreign,
            'cft': self._create_trigger,
            'dft': self._drop_trigger,
        }
        for table, funcs in create_helpers(models).items():
            for k, t in helpers.items():
                t[table] = funcs[k]

    def _begin(self):
        try:
            self._conn.execute(BEGIN)
            self.log.debug('Began new transaction')
        except sqlite3.OperationalError:
            pass

    def _end(self):
        try:
            self._conn.execute(END)
            self.log.debug('Ended new transaction')
        except sqlite3.OperationalError:
            pass

    def write(self, table, item):
        self._queues[table].append(item)
        self._recordcount += 1
        if self.buffering and self._recordcount >= self.buffering:
            self.flush()

    def _apply_changes(self):
        for table in self._tables:
            q = self._queues[table]
            if not q:
                continue
            try:
                self._insert_into[table](self._conn, q)
            except sqlite3.IntegrityError:
                self._conn.rollback()
                raise
            else:
                self._conn.commit()
                self._recordcount -= len(q)
                q.clear()

    def flush(self):
        if self._recordcount:
            self.log.info(f'Saving {self._recordcount} records')
            with watch_for_timing('Flushing'):
                self._apply_changes()
        self._end()
        self._begin()

    def deduplicate(self):
        self.log.info('Deduplicating database records')
        try:
            with watch_for_timing('Deduplicating'):
                for table in self._tables:
                    self._remove_duplicate[table](self._conn)
        except sqlite3.IntegrityError:
            self._conn.rollback()
            self._end()
            raise
        else:
            self._conn.commit()

    def reconcile(self):
        self.log.info('Enforcing internal references')
        try:
            funcs = self._update_foreign
            with watch_for_timing('Fixing foreign keys'):
                mismatches = self._conn.execute('PRAGMA foreign_key_check;')
                for table, rowid, parent, fkid in mismatches:
                    funcs[table][fkid](self._conn, rowid)
        except sqlite3.IntegrityError:
            self._conn.rollback()
            self._end()
            raise
        else:
            self._conn.commit()

    def close(self):
        if self._closed:
            return
        self.flush()
        conn = self._conn

        for drop_trigger in self._drop_trigger.values():
            drop_trigger(conn)
        self.reconcile()
        self.deduplicate()

        create_indices(conn)
        conn.execute(FOREIGN_KEY_ON)

        self._tally()
        mark_as_unlocked(self._conn)
        conn.close()
        self._closed = True

    def _tally(self):
        count = {t: count_rows(self._conn, t) for t in self._tables}
        diff = {t: v is not None and count[t] - v for t, v in self._rowcounts.items()}
        self.log.info('Database statistics:')
        for table in self._tables:
            if diff[table] is not False:
                self.log.info(f'  {table}: {count[table]} ({diff[table]:+})')
            else:
                self.log.info(f'  {table}: {count[table]}')
        self._rowcounts.update(count)

    def __enter__(self):
        return self

    def __exit__(self, typ, val=None, tb=None):
        self.close()
        if not typ:
            return True
        if val is None:
            if tb is None:
                raise typ
            val = typ()
        if tb is not None:
            val = val.with_traceback(tb)
        raise val

    @property
    def execute(self):
        return self._conn.execute

    @property
    def executemany(self):
        return self._conn.executemany

    @property
    def executescript(self):
        return self._conn.executescript
