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


def create_all(conn):
    with conn:
        for name, cmd in commands.items():
            if name[:5] == 'init_':
                conn.executescript(cmd)


def verify_version(conn, target_ver):
    db_ver = conn.execute('SELECT version FROM __version__;').fetchone()
    if not db_ver:
        with conn:
            conn.execute('INSERT INTO __version__ (version) VALUES (?)', (target_ver,))
    else:
        db_ver = db_ver[0]
        if db_ver != target_ver:
            raise DBVersionError(db=db_ver, target=target_ver)


def migrate(db_path, version=SCHEMA_VERSION):
    conn = sqlite3.Connection(db_path)
    log = logging.getLogger('feedly.db.migrate')
    outdated = False
    try:
        verify_version(conn, version)
    except DBVersionError as e:
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
        log.info(f'{old} -> {new}')
        with open(MIGRATIONS.joinpath(cmd)) as f:
            with conn:
                conn.executescript(f.read())

    log.info(_('Cleaning up... This may take a long time.', color='cyan'))
    with conn:
        conn.execute('VACUUM;')
    log.info(_('Done.', color='green'))


def select_max_rowids(conn, tables):
    max_row = {}
    for table in tables:
        row = conn.execute(f'SELECT max(id) FROM {table}').fetchone()
        if row is None:
            row = [None]
        max_id = row[0] or 0
        max_row[table] = max_id + 1
    return max_row


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


class DBVersionError(TypeError):
    def __init__(self, db, target, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = db
        self.target = target

    def __str__(self):
        return f'Cannot write to database of version {self.db}; currently supported version: {self.target}'
