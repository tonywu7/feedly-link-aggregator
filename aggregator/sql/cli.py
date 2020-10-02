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
import re
import shutil
import sqlite3
from functools import reduce
from pathlib import Path

from setuptools.version import pkg_resources

from ..utils import colored as _
from ..utils import findpath, randstr
from .db import db
from .factory import DatabaseVersionError
from .stream import DatabaseWriter

MIGRATIONS = Path(Path(__file__).with_name('migrations')).resolve(True)
Version = pkg_resources.parse_version


def check(db_path, debug=False):
    log = logging.getLogger('db.check')
    try:
        writer = DatabaseWriter(db_path, db, debug=debug, cache_path=':memory:')
        writer._verify(writer._main)
        writer.close()
        log.info(_('Database is OK.', color='green'))
    except DatabaseVersionError as exc:
        log.critical(exc)
        log.error(_('Run `python -m aggregator upgrade-db` to upgrade it to the current version.', color='cyan'))
        return 1
    except Exception as exc:
        log.critical(exc, exc_info=True)
        log.error(_('Database has irrecoverable inconsistencies.', color='red'))
        return 1
    else:
        return 0


def merge(output, *db_paths, debug=False):
    log = logging.getLogger('db.merge')
    for path in db_paths:
        log.info(_(f'Checking database at {path}', color='cyan'))
        exc = check(path, debug)
        if exc:
            return exc
    output = Path(output)
    db_paths = [Path(p) for p in db_paths]
    initial = db_paths[0]
    log.info(_(f'Copying initial database {initial}', color='cyan'))
    shutil.copyfile(initial, output)
    out = DatabaseWriter(output, db, debug=debug, cache_path=':memory:')
    for path in db_paths[1:]:
        log.info(_(f'Copying database {path}', color='cyan'))
        cp = output.with_name(randstr(8) + '.db')
        shutil.copyfile(path, cp)
        log.info(_(f'Merging {path}', color='cyan'))
        out._merge_other(other=cp)
        cp.unlink()
        cp.with_suffix('.db-shm').unlink()
        cp.with_suffix('.db-wal').unlink()
    out.report()
    out.close()
    return 0


def migrate(db_path, debug=False, version=db.version):
    conn = sqlite3.Connection(db_path, isolation_level=None)
    log = logging.getLogger('db.migrate')
    if debug:
        conn.set_trace_callback(log.debug)

    if db.is_locked(conn):
        log.error('Database was left in a partially consistent state.')
        log.error('Run `python -m aggregator check-db` to fix it first.')
        return 1

    outdated = False
    try:
        db.verify_version(conn)
    except DatabaseVersionError as e:
        outdated = e.db

    if not outdated:
        log.info(_('Database version is already up-to-date.', color='green'))
        return 0

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
        return 1

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
    return 0


def leftovers(wd, debug=False):
    log = logging.getLogger('db.leftovers')
    main = Path(wd) / 'index.db'
    tmp_pattern = re.compile(r'.*~tmp-[0-9a-f]{8}\.db$')
    for temp in os.listdir(wd):
        if tmp_pattern.match(temp):
            temp = main.with_name(temp)
            log.info(f'Found unmerged temp database {temp}')
            writer = DatabaseWriter(main, db, debug=debug, cache_path=temp)
            writer.merge()
            writer.close()
            writer.cleanup()
    log.info(_('All temporary databases have been merged.', color='green'))
