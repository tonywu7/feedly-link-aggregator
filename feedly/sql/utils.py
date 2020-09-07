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

import os
from pathlib import Path

import simplejson as json

SQL_REPO = Path(Path(__file__).with_name('commands')).resolve(True)
METADATA = Path(Path(__file__).with_name('metadata')).resolve(True)

commands = {}
for cmdf in os.listdir(SQL_REPO):
    with open(SQL_REPO.joinpath(cmdf)) as f:
        commands[cmdf[:-4]] = f.read()


def create_all(conn, cursor):
    for name, cmd in commands.items():
        if name[:5] == 'init_':
            cursor.executescript(cmd)
    conn.commit()


def verify_version(conn, cursor, target_ver):
    cursor.execute('SELECT version FROM __version__;')
    db_ver = cursor.fetchone()
    if not db_ver:
        cursor.execute('INSERT INTO __version__ (version) VALUES (?)', (target_ver,))
        conn.commit()
    else:
        db_ver = db_ver[0]
        if db_ver != target_ver:
            raise ValueError(f'Cannot write to database of version {db_ver}; currently supported version: {target_ver}')


def select_max_rowids(cursor, tables):
    max_row = {}
    for table in tables:
        cursor.execute(f'SELECT max(id) FROM {table}')
        row = cursor.fetchone()
        if row is None:
            row = [None]
        max_id = row[0] or 0
        max_row[table] = max_id + 1
    return max_row


PRIMARY_KEY = 'primary_key'
AUTOINCREMENT = 'autoincrement'
UNIQUE = 'unique'


def load_identity_config():
    with open(METADATA.joinpath('identity.json')) as f:
        config = json.load(f)
    transform = {
        AUTOINCREMENT: tuple,
        PRIMARY_KEY: tuple,
        UNIQUE: lambda cols: {tuple(arr) for arr in cols},
    }
    for table, conf in config.items():
        for opt in conf:
            conf[opt] = transform[opt](conf[opt])
    return config


def select_identity(cursor, table, config):
    opts = config.keys()
    if opts == {PRIMARY_KEY, AUTOINCREMENT, UNIQUE}:
        return _make_unique_auto_mapping(cursor, table, config)
    if opts == {PRIMARY_KEY, AUTOINCREMENT}:
        return _make_pk_auto_mapping(cursor, table, config)
    if opts == {PRIMARY_KEY, UNIQUE}:
        return _make_unique_pk_mapping(cursor, table, config)
    if opts == {PRIMARY_KEY}:
        return _make_pk_mapping(cursor, table, config)


def _make_unique_auto_mapping(cursor, table, config):
    if config[PRIMARY_KEY] != config[AUTOINCREMENT]:
        return _make_unique_pk_mapping(cursor, table, config)
    keys = _select_unique(cursor, table, config)
    values = _select_auto(cursor, table, config)
    return {k: v for k, v in zip(keys, values)}


def _make_pk_auto_mapping(cursor, table, config):
    keys = _select_pk(cursor, table, config)
    values = _select_auto(cursor, table, config)
    return {k: v for k, v in zip(keys, values)}


def _make_unique_pk_mapping(cursor, table, config):
    keys = _select_unique(cursor, table, config)
    values = _select_pk(cursor, table, config)
    return {k: v for k, v in zip(keys, values)}


def _make_pk_mapping(cursor, table, config):
    keys = _select_pk(cursor, table, config)
    return {k: True for k in keys}


def _select_unique(cursor, table, config):
    columns = []
    for t in config[UNIQUE]:
        cols = ', '.join(t)
        cursor.execute(f'SELECT {cols} FROM {table}')
        columns.append(cursor.fetchall())
    return zip(*columns)


def _select_pk(cursor, table, config):
    cursor.execute(f'SELECT {", ".join(config[PRIMARY_KEY])} FROM {table}')
    return cursor.fetchall()


def _select_auto(cursor, table, config):
    cursor.execute(f'SELECT {config[AUTOINCREMENT][0]} FROM {table}')
    return [t[0] for t in cursor.fetchall()]
