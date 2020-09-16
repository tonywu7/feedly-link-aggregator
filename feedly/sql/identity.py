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

from operator import itemgetter

import simplejson as json

from ..utils import JSONDict
from .utils import METADATA

COLUMNS = 'columns'
PRIMARY_KEY = 'primary_key'
AUTOINCREMENT = 'autoincrement'
UNIQUE = 'unique'
FOREIGN_KEYS = 'foreign_keys'

TRANSFORM = {
    COLUMNS: tuple,
    PRIMARY_KEY: tuple,
    AUTOINCREMENT: tuple,
    UNIQUE: lambda cols: tuple(tuple(arr) for arr in cols),
    FOREIGN_KEYS: lambda cols: tuple(tuple(arr) for arr in cols),
}


def load_identity_config():
    with open(METADATA.joinpath('identity.json')) as f:
        config: JSONDict = json.load(f)

    for conf in config.values():
        for opt in conf:
            conf[opt] = TRANSFORM[opt](conf[opt])

    inserts = {table: insert_stmt(table, conf) for table, conf in config.items()}
    ident_funcs = {table: identity_funcs(table, conf) for table, conf in config.items()}
    remote_funcs = {table: foreign_id_funcs(conf) for table, conf in config.items()}

    return inserts, ident_funcs, remote_funcs


def insert_stmt(table, conf):
    if AUTOINCREMENT in conf:
        columns = [c for c in conf[COLUMNS] if c != conf[AUTOINCREMENT][0]]
    else:
        columns = conf[COLUMNS]
    insert = ', '.join(columns)
    subs = ', '.join([f':{c}' for c in columns])
    insert = f'INSERT INTO {table} ({insert}) VALUES ({subs})'
    return insert


def identity_funcs(table, conf):
    opts = conf.keys()
    if opts == {COLUMNS, FOREIGN_KEYS, PRIMARY_KEY, AUTOINCREMENT, UNIQUE}:
        return _layout_uq_auto(table, conf)
    if opts == {COLUMNS, FOREIGN_KEYS, PRIMARY_KEY, AUTOINCREMENT}:
        return _layout_pk_auto(table, conf)
    if opts == {COLUMNS, FOREIGN_KEYS, PRIMARY_KEY, UNIQUE}:
        return _layout_uq_pk(table, conf)
    if opts == {COLUMNS, FOREIGN_KEYS, PRIMARY_KEY}:
        return _layout_pk_none(table, conf)


def foreign_id_funcs(conf):
    foreign_keys = conf[FOREIGN_KEYS]
    if not foreign_keys:
        return (None, None)

    local_columns = [t[0] for t in foreign_keys]
    remote_tables = [t[1] for t in foreign_keys]

    def translate_data(data, *identity_maps):
        data.update({lc: m[data[lc]] for lc, m in zip(local_columns, identity_maps)})

    if len(remote_tables) == 1:
        table = remote_tables[0]

        def get_remote_keys(tables):
            return (tables[table],)
    else:
        get_remote_keys = itemgetter(*remote_tables)

    return translate_data, get_remote_keys


def _layout_uq_auto(table, conf):
    if conf[PRIMARY_KEY] != conf[AUTOINCREMENT]:
        return _layout_uq_pk(table, conf)

    def select_identity(conn, from_rowid=0):
        keys = _select_unique(conn, table, conf[UNIQUE], from_rowid)
        values = _select_auto(conn, table, conf[AUTOINCREMENT][0], from_rowid)
        return dict(zip(keys, values))

    get_identity = _make_id_getter(conf[UNIQUE])
    return select_identity, get_identity


def _layout_pk_auto(table, conf):
    def select_identity(conn, from_rowid=0):
        keys = _select_pk(conn, table, conf[UNIQUE], from_rowid)
        values = _select_auto(conn, table, conf[AUTOINCREMENT][0], from_rowid)
        return dict(zip(keys, values))

    get_identity = _make_id_getter(conf[PRIMARY_KEY])
    return select_identity, get_identity


def _layout_uq_pk(table, conf):
    def select_identity(conn, from_rowid=0):
        keys = _select_unique(conn, table, conf[UNIQUE], from_rowid)
        values = _select_pk(conn, table, conf[PRIMARY_KEY], from_rowid)
        return dict(zip(keys, values))

    get_identity = _make_id_getter(conf[UNIQUE])
    return select_identity, get_identity


def _layout_pk_none(table, conf):
    def select_identity(conn, from_rowid=0):
        keys = _select_pk(conn, table, conf[PRIMARY_KEY], from_rowid)
        return {k: True for k in keys}

    get_identity = _make_id_getter(conf[PRIMARY_KEY])
    return select_identity, get_identity


def _make_id_getter(columns):
    if len(columns) == 1 and isinstance(columns[0], tuple):
        columns = columns[0]

    if isinstance(columns[0], tuple):
        igetters = [itemgetter(*g) for g in columns]

        def get_id(data):
            return tuple(g(data) for g in igetters)
    else:
        get_id = itemgetter(*columns)

    return get_id


def _select_unique(conn, table, column_groups, from_rowid=0):
    columns = []
    for t in column_groups:
        cols = ', '.join(t)
        columns.append(conn.execute(f'SELECT {cols} FROM {table} WHERE rowid > ?', (from_rowid,)).fetchall())
    rows = list(zip(*columns))
    while rows and isinstance(rows[0], tuple) and len(rows[0]) == 1:
        rows = [r[0] for r in rows]
    return rows


def _select_pk(conn, table, columns, from_rowid=0):
    rows = conn.execute(f'SELECT {", ".join(columns)} FROM {table} WHERE rowid > ?', (from_rowid,)).fetchall()
    while rows and isinstance(rows[0], tuple) and len(rows[0]) == 1:
        rows = [r[0] for r in rows]
    return rows


def _select_auto(conn, table, column, from_rowid=0):
    return [t[0] for t in conn.execute(f'SELECT {column} FROM {table} WHERE rowid > ?', (from_rowid,)).fetchall()]
