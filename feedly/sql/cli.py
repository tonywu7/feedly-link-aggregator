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
from .metadata import models, tables
from .stream import DatabaseWriter
from .utils import DatabaseVersionError, is_locked, verify_version

MIGRATIONS = Path(Path(__file__).with_name('migrations')).resolve(True)
Version = pkg_resources.parse_version


def check(db_path, debug=False):
    log = logging.getLogger('db.check')
    try:
        DatabaseWriter(db_path, tables, models, 0, debug=debug).close()
        log.info(_('Database is OK.', color='green'))
    except DatabaseVersionError as exc:
        log.critical(exc)
        log.error(_('Run `python -m feedly upgrade-db` to upgrade it to the current version.', color='cyan'))
    except Exception as exc:
        log.critical(exc, exc_info=True)
        log.error(_('Database has irrecoverable inconsistencies.', color='red'))


def migrate(db_path, debug=False, version=SCHEMA_VERSION):
    conn = sqlite3.Connection(db_path, isolation_level=None)
    log = logging.getLogger('db.migrate')
    if debug:
        conn.set_trace_callback(log.debug)

    if is_locked(conn):
        log.error('Database was left in a partially consistent state.')
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
        with open(MIGRATIONS / cmd) as f:
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
