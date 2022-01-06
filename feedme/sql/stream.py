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
from contextlib import suppress
from pathlib import Path
from threading import Lock
from typing import Union

from ..utils import append_stem, randstr, watch_for_timing
from .factory import Database

_PathLike = Union[str, Path]


class DatabaseWriter:
    def __init__(self, path: _PathLike, database: Database,
                 debug=False, cache_path=None, silent=False):
        self.log = logging.getLogger('db.writer')
        if silent:
            self.log.setLevel(logging.WARNING)

        main_db = Path(path)
        cache_db = Path(cache_path) if cache_path else append_stem(path, f'~tmp-{randstr(8)}')

        self.db = database
        self._queues = {t: deque() for t in database.tablemap}
        self._flush_lock = Lock()

        self._main = self._connect(main_db, 'main', debug)
        self._cache = self._connect(cache_db, 'temp', debug)
        self._paths = {self._main: main_db, self._cache: cache_db}

        self._corked = True
        self._closed = False
        self._rowcounts = {conn: {t: None for t in database.tablemap}
                           for conn in (self._main, self._cache)}

        self._bind_tables()
        self.report()
        self.uncork()
        self.flush()

    def _connect(self, path: _PathLike, name=None, debug=False):
        conn = sqlite3.connect(path, isolation_level=None, timeout=30,
                               check_same_thread=False)
        conn.row_factory = sqlite3.Row
        if debug:
            self._setup_debug(conn, name, debug)

        self.db.verify_version(conn)
        self.db.set_version(conn)
        self.db.create_all(conn)
        return conn

    @property
    def record_count(self):
        return sum(len(q) for q in self._queues.values())

    def _lock_db(self, conn: sqlite3.Connection):
        self.log.debug(f'Locking database {self._paths[conn]}')
        if self.db.is_locked(conn):
            self.log.warning('Database lock table exists')
            self.log.warning('Previous crawler did not exit properly')
        self.db.mark_as_locked(conn)

    def _unlock_db(self, conn: sqlite3.Connection):
        self.log.debug(f'Unlocking database {self._paths[conn]}')
        self.db.mark_as_unlocked(conn)

    def _setup_debug(self, conn: sqlite3.Connection, name, debug_out):
        sql_log = logging.getLogger(f'db.sql.{name}')
        sql_log.setLevel(logging.DEBUG)
        conn.set_trace_callback(sql_log.debug)
        if not isinstance(debug_out, bool):
            sql_log.propagate = False
            path = append_stem(Path(debug_out), f'-{name}')
            file = open(path, 'w+')
            handler = logging.StreamHandler(file)
            sql_log.addHandler(handler)

    def _bind_tables(self):
        for table in self.db.tables:
            table.bind_foreign_key(self._cache)
            table.bind_offset(self._main)

    def _foreign_key_off(self, conn: sqlite3.Connection):
        conn.execute('PRAGMA foreign_keys = OFF')
        self.log.debug(f'Foreign key is OFF for {self._paths[conn]}')

    def _foreign_key_on(self, conn: sqlite3.Connection):
        conn.execute('PRAGMA foreign_keys = ON')
        self.log.debug(f'Foreign key is ON for {self._paths[conn]}')

    def _rebuild_index(self, conn: sqlite3.Connection):
        self.log.info('Rebuilding index')
        self.db.create_indices(conn)

    def _begin(self, conn: sqlite3.Connection):
        try:
            conn.execute('BEGIN')
            self.log.debug(f'Began new transaction on {self._paths[conn]}')
        except sqlite3.OperationalError:
            pass

    def _begin_exclusive(self, conn: sqlite3.Connection):
        while True:
            try:
                conn.execute('BEGIN EXCLUSIVE')
                self.log.debug('Began exclusive transaction'
                               f' on {self._paths[conn]}')
            except sqlite3.OperationalError:
                self.log.warning('Cannot acquire exclusive write access')
                self.log.warning('Another program is writing to the database')
                self.log.warning('Retrying...')
            else:
                return

    def _apply_changes(self):
        queues = self._queues
        self._queues = {t: deque() for t in self.db.tablemap}
        cache = self._cache
        for name, table in self.db.tablemap.items():
            q = queues[name]
            if not q:
                continue

            try:
                table.insert(cache, q)
            except sqlite3.IntegrityError:
                cache.rollback()
                for k, v in queues.items():
                    self._queues[k].appendleft(v)
                raise
            else:
                cache.commit()
                del queues[name]

    def _verify(self, conn: sqlite3.Connection):
        self._foreign_key_off(conn)
        for table in self.db.tables:
            table.drop_proxy(conn)
            table.restore_original(conn)
        conn.commit()
        self.reconcile(conn)
        self.deduplicate(conn)
        for table in self.db.tables:
            table.drop_temp_index(conn)
        self._rebuild_index(conn)
        self._foreign_key_on(conn)
        self._optimize(conn)

    def _optimize(self, conn: sqlite3.Connection):
        self.log.debug(f'Optimizing {self._paths[conn]}')
        conn.execute('PRAGMA optimize')

    def _merge_other(self, other=None, discard=False):
        main = self._main
        if not other:
            other_db = self._cache
            other = str(self._paths[other_db])
        else:
            other = str(other)
            other_db = sqlite3.connect(other, isolation_level=None)
        max_rowids = self.db.get_max_rowids(main)
        self._foreign_key_off(main)
        self._begin_exclusive(main)
        self._lock_db(main)
        self.db.attach(main, other)
        self.log.debug(f'Attached {other} to {self._paths[main]}')

        try:
            self.log.debug('Matching existing records')
            with watch_for_timing('Matching'):
                for table in self.db.tables:
                    self.log.debug(f'Matching {table}')
                    table.match_primary_keys(main)
                    table.match_foreign_keys(main)

            self.log.debug('Dropping indices')
            self.db.drop_indices(main)

            self.log.debug('Merging into main database')
            with watch_for_timing('Merging'):
                for table in self.db.tables:
                    self.log.debug(f'Merging {table}')
                    table.dedup_primary_keys(main)
                    table.merge_attached(main)

            self.log.debug('Deduplicating records')
            with watch_for_timing('Deduplicating'):
                for table in self.db.tables:
                    self.log.debug(f'Deduplicating {table}')
                    table.dedup(main, max_rowids[table.name])

        except sqlite3.IntegrityError:
            main.rollback()
            raise

        else:
            self.log.debug('Committing changes')
            main.commit()
            self.db.detach(main)
            self._foreign_key_on(main)
            self._optimize(main)
            self.log.debug('Finalizing merge')

        finally:
            if not discard:
                self.log.debug('Removing transcient data')
                with watch_for_timing('Restoring'):
                    for table in self.db.tables:
                        table.restore_original(other_db)
            self._rebuild_index(main)
            self._unlock_db(main)

    def uncork(self):
        if not self._corked:
            return
        conn = self._cache

        self._lock_db(conn)
        self.db.drop_indices(conn)
        self._foreign_key_off(conn)
        for table in self.db.tables:
            table.create_proxy(conn)
        self._corked = False

    def cork(self):
        if self._corked:
            return
        self.flush()
        conn = self._cache
        self._verify(conn)
        self._unlock_db(conn)
        self._corked = True

    def write(self, table, item):
        self._queues[table].append(item)

    def flush(self):
        with self._flush_lock:
            if self._corked:
                return

            count = self.record_count
            if count:
                self.log.info(f'Saving {count} records')
                with watch_for_timing('Flushing'):
                    self._apply_changes()

            self._cache.commit()
            self._begin(self._cache)

    def deduplicate(self, conn=None):
        self.log.info('Deduplicating database records')
        conn = conn or self._cache
        conn.commit()
        self._begin_exclusive(conn)
        try:
            with watch_for_timing('Deduplicating'):
                for table in self.db.tables:
                    table.fast_dedup(conn)
        except sqlite3.IntegrityError:
            conn.rollback()
            raise
        finally:
            conn.commit()

    def reconcile(self, conn=None):
        self.log.info('Enforcing internal references')
        conn = conn or self._cache
        conn.commit()
        self._begin_exclusive(conn)
        try:
            with watch_for_timing('Fixing foreign keys'):
                mismatches = conn.execute('PRAGMA foreign_key_check')
                for table, rowid, parent, fkid in mismatches:
                    self.db.tablemap[table].update_fk(conn, fkid, rowid)
        except sqlite3.IntegrityError:
            conn.rollback()
            raise
        else:
            conn.commit()

    def merge(self):
        self.cork()
        self.log.info('Merging new data into main database')
        self._merge_other(discard=True)
        self.report()

    def close(self):
        self._main.close()
        self._cache.close()
        self._corked = True
        self._closed = True

    def interrupt(self):
        self._main.interrupt()
        self._cache.interrupt()

    def cleanup(self):
        cache = self._paths[self._cache]
        shm = cache.with_suffix('.db-shm')
        wal = cache.with_suffix('.db-wal')
        with suppress(FileNotFoundError):
            cache.unlink()
            shm.unlink()
            wal.unlink()

    def finish(self, merge=True):
        if not merge:
            self.cork()
            self.close()
            return
        self.merge()
        self.close()
        self.cleanup()

    def _tally(self, conn):
        count = self.db.count_rows(conn)
        diff = {t: v is not None and count[t] - v for t, v in self._rowcounts[conn].items()}
        msg = ['Database stats:']
        for table in self.db.tablemap:
            if diff[table] is not False:
                msg.append(f'  {table}: {count[table]} ({diff[table]:+})')
            else:
                msg.append(f'  {table}: {count[table]}')
        self._rowcounts[conn].update(count)
        return msg

    def report(self):
        for line in self._tally(self._main):
            self.log.info(line)

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
