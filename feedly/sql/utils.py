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
import os
import sqlite3
from functools import reduce
from pathlib import Path

from setuptools.version import pkg_resources

from ..utils import colored as _
from ..utils import findpath
from . import SCHEMA_VERSION

SQL_REPO = Path(Path(__file__).with_name('commands')).resolve(True)
METADATA = Path(Path(__file__).with_name('metadata')).resolve(True)
MIGRATIONS = Path(Path(__file__).with_name('migrations')).resolve(True)

Version = pkg_resources.parse_version

commands = {}
for cmdf in os.listdir(SQL_REPO):
    with open(SQL_REPO.joinpath(cmdf)) as f:
        commands[cmdf[:-4]] = f.read()


def create_indices(conn):
    conn.executescript(commands['create-indices'])


def drop_indices(conn):
    conn.executescript(commands['drop-indices'])


def create_all(conn):
    scripts = [commands[t] for t in ('pragma', 'version', 'create-tables')]
    scripts = '\n'.join(scripts)
    conn.executescript(scripts)


def verify_version(conn, target_ver):
    try:
        db_ver = conn.execute('SELECT version FROM __version__;').fetchone()
    except sqlite3.OperationalError:
        db_ver = None
        conn.execute(commands['version'])
        conn.commit()
    if not db_ver:
        conn.execute('INSERT INTO __version__ (version) VALUES (?)', (target_ver,))
    else:
        db_ver = db_ver[0]
        if db_ver != target_ver:
            raise DatabaseVersionError(db=db_ver, target=target_ver)


def mark_as_locked(conn):
    conn.execute('CREATE TABLE IF NOT EXISTS lock (locked INTEGER)')


def mark_as_unlocked(conn):
    conn.execute('DROP TABLE IF EXISTS lock')


def is_locked(conn):
    exists = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name == 'lock'")
    return len(list(exists))


def check(db_path):
    from .metadata import models, tables
    from .stream import DatabaseWriter
    log = logging.getLogger('db.check')
    try:
        DatabaseWriter(db_path, tables, models, 0).close()
    except DatabaseVersionError as exc:
        log.critical(exc)
        log.error(_('Run `python -m feedly upgrade-db` to upgrade it to the current version.', color='cyan'))


def migrate(db_path, version=SCHEMA_VERSION):
    conn = sqlite3.Connection(db_path, isolation_level=None)
    log = logging.getLogger('db.migrate')

    if is_locked(conn):
        log.error('Database was left in an inconsistent state.')
        log.error('Run `python -m feedly check-db` to fix it first.')
        return 1

    outdated = False
    try:
        verify_version(conn, version)
    except DatabaseVersionError as e:
        outdated = e.db

    if not outdated:
        log.info(_('Database version is already up-to-date.', color='green'))
        return

    source_ver = Version(outdated)
    target_ver = Version(version)
    versions = {}
    for cmd in os.listdir(MIGRATIONS):
        from_, to_ = cmd[:-4].split('_')
        from_ = Version(from_)
        to_ = Version(to_)
        to_versions = versions.setdefault(from_, set())
        to_versions.add(to_)

    path = []
    scripts = []
    if findpath(source_ver, target_ver, versions, path):
        reduce(lambda x, y: scripts.append((x, y, f'{x}_{y}.sql')) or y, path)
    else:
        log.error(f'This version of the program no longer supports migrating from {source_ver} to {target_ver}')
        return

    for old, new, cmd in scripts:
        log.info(f'Upgrading database schema from v{old} to v{new}. This may take a long time.')
        with open(MIGRATIONS.joinpath(cmd)) as f:
            try:
                conn.executescript(f.read())
            except sqlite3.OperationalError as e:
                log.error(e, exc_info=True)
                log.error('Failed to upgrade database. Undoing.')
                conn.rollback()
                conn.close()
                return 1
            else:
                conn.commit()

    log.info(_('Compacting database... This may take a long time.', color='cyan'))
    conn.execute('VACUUM;')
    log.info(_('Done.', color='green'))


def count_rows(conn, table):
    row = conn.execute(f'SELECT count(id) FROM {table}').fetchone()
    if row is None:
        row = [None]
    max_id = row[0] or 0
    return max_id


def bulk_fetch(cur, size=100000, log=None):
    i = 0
    rows = cur.fetchmany(size)
    while rows:
        for row in rows:
            i += 1
            yield row
        if log:
            log.info(f'Fetched {i} rows.')
        rows = cur.fetchmany(size)


class DatabaseVersionError(TypeError):
    def __init__(self, db, target, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = db
        self.target = target

    def __str__(self):
        return f'Cannot write to database of version {self.db}; currently supported version: {self.target}'

    def __reduce__(self):
        return self.__class__, (self.db, self.target)
