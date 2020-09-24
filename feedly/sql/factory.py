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

from itertools import chain

COLUMNS = 'columns'
INFO = 'info'
PRIMARY_KEY = 'primary_key'
AUTOINCREMENT = 'autoincrement'
UNIQUE = 'unique'
FOREIGN_KEYS = 'foreign_keys'

TRANSFORM = {
    COLUMNS: dict,
    INFO: dict,
    PRIMARY_KEY: tuple,
    AUTOINCREMENT: tuple,
    UNIQUE: lambda cols: tuple(tuple(arr) for arr in cols),
    FOREIGN_KEYS: lambda cols: tuple(tuple(arr) for arr in cols),
}


def insert_funcs(table, conf, *args, **kwargs):
    if AUTOINCREMENT in conf:
        columns = [c for c in conf[COLUMNS] if c != conf[AUTOINCREMENT][0]]
    else:
        columns = conf[COLUMNS]

    keys = ', '.join(columns)
    subs = ', '.join([f':{c}' for c in columns])
    insert = f'INSERT INTO {table} ({keys}) VALUES ({subs})'

    def do_insert(conn, data):
        conn.executemany(insert, data)

    return {'ins': do_insert}


def dedup_funcs(table, conf, *args, **kwargs):
    columns = set(chain(*conf.get(UNIQUE, []), conf.get(PRIMARY_KEY)))
    columns.discard(conf.get(AUTOINCREMENT, [None])[0])
    if not columns:
        def do_dedup(conn):
            conn.execute(delete)
        return do_dedup

    keys = ', '.join(columns)
    func = conf[INFO].get('dedup', 'min')
    delete = (f'DELETE FROM {table} WHERE rowid NOT IN '
              f'(SELECT {func}(rowid) FROM {table} GROUP BY {keys})')

    def do_dedup(conn):
        conn.execute(delete)

    return {'dup': do_dedup}


def foreign_key_proxies(table, conf, config, *args, **kwargs):
    if not conf[FOREIGN_KEYS]:
        def create(conn):
            pass

        def drop(conn):
            pass

        return {'cft': create, 'dft': drop}

    view_name = f'proxy_{table}'
    trigger_name = f'fkey_cascade_{table}'

    columns = set(conf[COLUMNS])
    columns.discard(conf.get(AUTOINCREMENT, [None])[0])

    local_columns = set()
    static_columns = set()
    subqueries = {}
    indices = {}

    for local_column, remote_table, remote_column in conf[FOREIGN_KEYS]:
        remote_identity = config[remote_table].get(UNIQUE, [])
        if len(remote_identity) != 1 or len(remote_identity[0]) != 1:
            raise ValueError
        remote_identity = remote_identity[0][0]
        local = f'NEW.{local_column}'
        remote = f'{remote_table}.{remote_column}'
        referenced = f'{remote_table}.{remote_identity}'

        local_columns.add(local_column)
        subqueries[local_column] = (
            f'coalesce('
            f'(SELECT {remote} FROM {remote_table} '
            f'WHERE {referenced} == {local} '
            f'ORDER BY rowid LIMIT 1), '
            f'{local})'
        )
        index_name = f'tmp_ix_{remote_table}_{remote_identity}'
        indices[index_name] = (
            f'CREATE INDEX IF NOT EXISTS {index_name} '
            f'ON {remote_table} ({remote_identity});'
        )

    static_columns = columns - local_columns
    subqueries.update({c: f'NEW.{c}' for c in static_columns})
    columns = list(columns)
    subqueries = [subqueries[c] for c in columns]

    create_trigger = (
        f'CREATE TRIGGER IF NOT EXISTS {trigger_name} '
        f'INSTEAD OF INSERT ON {view_name} BEGIN '
        f'INSERT INTO {table} ({", ".join(columns)}) '
        f"VALUES ({', '.join(subqueries)}); END;"
    )
    drop_trigger = f'DROP TRIGGER IF EXISTS {trigger_name}'

    create_view = f'CREATE VIEW IF NOT EXISTS {view_name} AS SELECT * FROM {table};'
    drop_view = f'DROP VIEW IF EXISTS {view_name};'

    indices = [(v, f'DROP INDEX IF EXISTS {k}') for k, v in indices.items()]

    def create(conn):
        conn.execute(create_view)
        conn.execute(create_trigger)
        for create_index, _ in indices:
            conn.execute(create_index)

    def drop(conn):
        for _, drop_index in indices:
            conn.execute(drop_index)
        conn.execute(drop_trigger)
        conn.execute(drop_view)

    return {'cft': create, 'dft': drop, **insert_funcs(view_name, conf)}


def foreign_key_funcs(table, conf, config, *args, **kwargs):
    if not conf[FOREIGN_KEYS]:
        return {'ufk': {}}

    update_funcs = {}
    columns = []

    for local_column, remote_table, remote_column in conf[FOREIGN_KEYS]:
        columns.append(local_column)

        local = f'{table}.{local_column}'
        remote = f'{remote_table}.{remote_column}'

        remote_identity = config[remote_table].get(UNIQUE, [])
        if len(remote_identity) != 1 or len(remote_identity[0]) != 1:
            raise ValueError
        remote_identity = f'{remote_table}.{remote_identity[0][0]}'

        subquery = (
            f'SELECT {remote} FROM {remote_table} '
            f'WHERE {remote_identity} == {local}'
        )
        update = (
            f'UPDATE {table} '
            f'SET {local_column} = ({subquery}) '
            f'WHERE {table}.rowid == ?'
        )

        def do_update(conn, rowid, update=update):
            conn.execute(update, (rowid,))

        update_funcs[local_column] = do_update

    return {'ufk': update_funcs}


FACTORIES = (insert_funcs, dedup_funcs, foreign_key_funcs, foreign_key_proxies)


def create_helpers(config):
    for conf in config.values():
        for opt in conf:
            conf[opt] = TRANSFORM[opt](conf[opt])

    funcs = {k: {} for k in config}
    for table, conf in config.items():
        for factory in FACTORIES:
            funcs[table].update(factory(table, conf, config))

    return funcs
