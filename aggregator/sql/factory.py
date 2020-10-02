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

import sqlite3
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


def no_op(*args, **kwargs):
    pass


class Database:
    def __init__(self, descriptor):
        models = descriptor['models']
        creates = descriptor['tables']
        for model in models.values():
            for opt in model:
                model[opt] = TRANSFORM[opt](model[opt])
        self.tables = [Table(name, models, creates) for name in descriptor['order']]
        Table.associate(*self.tables)

        self.tablemap = {t.name: t for t in self.tables}
        self.version = descriptor['version']
        self.descriptor = descriptor

    def set_version(self, conn: sqlite3.Connection):
        ver = self.descriptor['versioning']
        conn.execute(ver['create'])
        conn.execute(ver['insert'], (self.version,))

    def create_all(self, conn: sqlite3.Connection):
        for stmt in self.descriptor['init']:
            conn.execute(stmt)
        create = self.descriptor['tables']
        for table in self.descriptor['order']:
            conn.execute(create[table])

    def create_indices(self, conn: sqlite3.Connection):
        for stmt in self.descriptor['indices'].values():
            conn.execute(stmt)

    def drop_indices(self, conn: sqlite3.Connection):
        for index in self.descriptor['indices']:
            conn.execute(f'DROP INDEX IF EXISTS {index}')

    def verify_version(self, conn: sqlite3.Connection):
        try:
            db_ver = conn.execute('SELECT version FROM __version__;').fetchone()
        except sqlite3.OperationalError:
            db_ver = None
        if not db_ver:
            tablecount = conn.execute("SELECT count(name) FROM sqlite_master WHERE type='table'").fetchone()[0]
            if tablecount:
                raise DatabaseNotEmptyError()
        else:
            db_ver = db_ver[0]
            if db_ver != self.version:
                raise DatabaseVersionError(db=db_ver, target=self.version)

    def mark_as_locked(self, conn: sqlite3.Connection):
        conn.execute('CREATE TABLE IF NOT EXISTS lock (locked INTEGER)')

    def mark_as_unlocked(self, conn: sqlite3.Connection):
        conn.execute('DROP TABLE IF EXISTS lock')

    def is_locked(self, conn: sqlite3.Connection):
        exists = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name == 'lock'")
        return len(list(exists))

    def count_rows(self, conn: sqlite3.Connection):
        count = {}
        for table in self.tablemap:
            row = conn.execute(f'SELECT count(id) FROM {table}').fetchone()
            if row is None:
                row = [None]
            count[table] = row[0] or 0
        return count

    def get_max_rowids(self, conn: sqlite3.Connection):
        max_id = {}
        for table in self.tablemap:
            rowid = conn.execute(f'SELECT max(rowid) FROM {table}').fetchone()
            if rowid is None:
                rowid = [None]
            max_id[table] = rowid[0] or 0
        return max_id

    def attach(self, conn: sqlite3.Connection, path):
        conn.execute('ATTACH ? AS secondary', (path,))

    def detach(self, conn: sqlite3.Connection):
        conn.execute('DETACH secondary')


class Table:
    def __init__(self, name, models, creates):
        self.name = name
        self.create_stmt = creates[name]
        self.model = model = models[name]
        self.info = model[INFO]
        self.columns = columns = model[COLUMNS]
        self.rowid = rowid = model[AUTOINCREMENT][0]
        self.keys = [c for c in columns if c != rowid]
        self.primary_key = pk = model[PRIMARY_KEY]
        self.unique_comp = unique = model[UNIQUE]
        self.signature = set(chain(*unique, pk)) - {rowid}
        self.foreign_keys = model[FOREIGN_KEYS]

    @classmethod
    def associate(cls, *tables):
        tablemap = {table.name: table for table in tables}
        for table in tables:
            table: cls
            table._build_insert()
            table._build_offset_trigger()
            table._build_dedup()
            table._build_update_foreign_key(tablemap)
            table._build_foreign_key_proxy(tablemap)
            table._build_merge()
            table._build_match_pk()
            table._build_match_fk()
            table._build_restore_original()

    def _build_insert(self):
        keys = self.keys
        names = ', '.join(keys)
        subs = ', '.join([f':{c}' for c in keys])
        insert = f'INSERT INTO {self.name} ({names}) VALUES ({subs})'

        def do_insert(conn, data):
            conn.executemany(insert, data)
        self.insert = do_insert

    def _build_offset_trigger(self):
        if not self.rowid:
            self.create_offset_trigger = no_op
            self.drop_offset_trigger = no_op
            self.bind_offset = no_op
            return

        trigger_name = f'offset_{self.name}_{self.rowid}'

        def do_create(conn):
            conn.execute(stmt)

        def do_drop(conn):
            conn.execute(f'DROP TRIGGER IF EXISTS {trigger_name}')

        def bind(conn):
            nonlocal stmt
            max_id = conn.execute(f'SELECT max(rowid) FROM {self.name}')
            max_id = max_id.fetchone()[0]
            if not max_id:
                self.create_offset_trigger = no_op
                return
            stmt = (f'CREATE TRIGGER IF NOT EXISTS {trigger_name} '
                    f'AFTER INSERT ON {self.name} '
                    f'WHEN (SELECT max(rowid) FROM {self.name}) == 1 BEGIN '
                    f'UPDATE {self.name} SET {self.rowid} = {self.rowid} + {max_id} '
                    f'WHERE {self.rowid} == NEW.{self.rowid}; END;')

        stmt = None
        self.create_offset_trigger = do_create
        self.drop_offset_trigger = do_drop
        self.bind_offset = bind

    def _build_dedup(self):
        if not self.signature:
            self.dedup = no_op
            self.fast_dedup = no_op
            return

        keys = ', '.join(self.signature)
        func = self.info.get('dedup', 'min')
        comp = '<=' if func == 'max' else '>'

        select = f'SELECT {func}(rowid) FROM {self.name} GROUP BY {keys}'
        delete = (f'DELETE FROM {self.name} WHERE %s rowid NOT IN '
                  f'({select})')

        columns = list(self.columns)
        for i in range(len(columns)):
            if columns[i] == self.rowid:
                columns[i] = f'{func}({self.rowid}) AS {self.rowid}'
                break
        columns = ', '.join(columns)

        alter = f'ALTER TABLE {self.name} RENAME TO temp_dedup'
        insert = (f'INSERT INTO {self.name} ({", ".join(self.columns)}) '
                  f'SELECT {columns} AS {self.rowid} '
                  f'FROM temp_dedup GROUP BY {keys}')
        drop = 'DROP TABLE temp_dedup'

        def do_dedup(conn, offset=0, delete=delete):
            if offset:
                delete = delete % f'rowid {comp} {offset} AND'
            else:
                delete = delete % ''
            conn.execute(delete)

        def do_dedup_fast(conn):
            if len(self.primary_key) > 1 or self.primary_key[0] != self.rowid:
                return do_dedup(conn)
            conn.execute(alter)
            conn.execute(self.create_stmt)
            conn.execute(insert)
            conn.execute(drop)

        self.dedup = do_dedup
        self.fast_dedup = do_dedup_fast

    def _build_update_foreign_key(self, others):
        if not self.foreign_keys:
            self.update_fk = no_op
            self.bind_foreign_key = no_op
            return

        update_funcs = {}
        columns = []

        for local_column, remote_table, remote_column in self.foreign_keys:
            columns.append(local_column)

            local = f'{self.name}.{local_column}'
            remote = f'{remote_table}.{remote_column}'

            remote_signature = list(others[remote_table].signature)
            if len(remote_signature) != 1:
                raise NotImplementedError
            remote_signature = f'{remote_table}.{remote_signature[0]}'

            select_referred = (
                f'SELECT {local} FROM {self.name} '
                f'WHERE {self.name}.rowid = ?'
            )
            select_key = (
                f'SELECT {remote} FROM {remote_table} '
                f'WHERE {remote_signature} == ?'
            )
            update = (
                f'UPDATE {self.name} '
                f'SET {local_column} = ? '
                f'WHERE {self.name}.rowid == ?'
            )
            delete = (
                f'DELETE FROM {self.name} '
                f'WHERE {self.name}.rowid == ?'
            )

            def do_update(conn, rowid, update=update, delete=delete,
                          select1=select_referred, select2=select_key):
                try:
                    referred = conn.execute(select1, (rowid,))
                    referred = referred.fetchone()[0]
                    key = conn.execute(select2, (referred,))
                    key = key.fetchone()[0]
                    conn.execute(update, (key, rowid))
                except sqlite3.IntegrityError:
                    conn.execute(delete, (rowid,))

            update_funcs[local_column] = do_update

        def do_update(conn, fkid, rowid):
            update_funcs[fkid](conn, rowid)
        self.update_fk = do_update

        def bind(conn):
            for row in conn.execute(f'PRAGMA foreign_key_list({self.name})'):
                update_funcs[row['id']] = update_funcs[row['from']]
        self.bind_foreign_key = bind

    def _build_foreign_key_proxy(self, others):
        if not self.foreign_keys:
            self.create_proxy = no_op
            self.drop_proxy = no_op
            self.drop_temp_index = no_op
            return

        view_name = f'proxy_{self.name}'
        trigger_name = f'foreign_cascade_{self.name}'

        columns = set(self.keys)

        local_columns = set()
        static_columns = set()
        subqueries = {}
        indices = {}

        for local_column, remote_table, remote_column in self.foreign_keys:
            remote_signature = list(others[remote_table].signature)
            if len(remote_signature) != 1:
                raise NotImplementedError
            remote_signature = remote_signature[0]
            local = f'NEW.{local_column}'
            remote = f'{remote_table}.{remote_column}'
            referenced = f'{remote_table}.{remote_signature}'

            local_columns.add(local_column)
            subqueries[local_column] = (
                f'coalesce('
                f'(SELECT {remote} FROM {remote_table} '
                f'WHERE {referenced} == {local} '
                f'ORDER BY rowid LIMIT 1), '
                f'{local})'
            )
            index_name = f'tmp_ix_{remote_table}_{remote_signature}'
            indices[index_name] = (
                f'CREATE INDEX IF NOT EXISTS {index_name} '
                f'ON {remote_table} ({remote_signature});'
            )

        static_columns = columns - local_columns
        subqueries.update({c: f'NEW.{c}' for c in static_columns})
        columns = list(columns)
        subqueries = [subqueries[c] for c in columns]

        create_trigger = (
            f'CREATE TRIGGER IF NOT EXISTS {trigger_name} '
            f'INSTEAD OF INSERT ON {view_name} BEGIN '
            f'INSERT INTO {self.name} ({", ".join(columns)}) '
            f"VALUES ({', '.join(subqueries)}); END;"
        )
        drop_trigger = f'DROP TRIGGER IF EXISTS {trigger_name}'

        create_view = f'CREATE VIEW IF NOT EXISTS {view_name} AS SELECT * FROM {self.name};'
        drop_view = f'DROP VIEW IF EXISTS {view_name};'

        indices = [(v, f'DROP INDEX IF EXISTS {k}') for k, v in indices.items()]

        def create(conn):
            conn.execute(create_view)
            conn.execute(create_trigger)
            for create_index, _ in indices:
                conn.execute(create_index)

        def drop(conn):
            conn.execute(drop_trigger)
            conn.execute(drop_view)

        def drop2(conn):
            for _, drop_index in indices:
                conn.execute(drop_index)

        name = self.name
        self.name = view_name
        self._build_insert()
        self.name = name

        self.create_proxy = create
        self.drop_proxy = drop
        self.drop_temp_index = drop2

    def _build_merge(self):
        columns = self.columns if not self.foreign_keys else self.keys
        names = ', '.join(columns)
        insert = (f'INSERT INTO {self.name} ({names}) '
                  f'SELECT {names} FROM secondary.{self.name}')

        def do_merge(conn):
            conn.execute(insert)
        self.merge_attached = do_merge

    def _build_match_pk(self):
        if self.foreign_keys or not self.primary_key or not self.signature:
            self.match_primary_keys = no_op
            self.dedup_primary_keys = no_op
            return

        main_table = f'main_{self.name}'
        temp_table_name = f'original_{self.name}'
        secondary_table = f'secondary.{self.name}'
        temp_table = f'secondary.{temp_table_name}'

        auto_inc_column = self.rowid
        var_columns = list(set(self.primary_key) - {auto_inc_column})
        const_columns = self.columns.keys() - var_columns - {auto_inc_column}
        columns = [*const_columns, *var_columns,
                   *[f'_{c}_' for c in var_columns],
                   *[f'exists_{c}' for c in var_columns]]

        create_columns = [*[f'{c} BLOB' for c in const_columns],
                          *[f'{c} INTEGER' for c in var_columns],
                          *[f'_{c}_ INTEGER' for c in var_columns],
                          *[f'exists_{c} INTEGER' for c in var_columns]]
        select_columns = [
            *[f'{temp_table}.{c} AS {c}' for c in const_columns],
            *[f'coalesce({main_table}.{c}, {temp_table}.{c}) AS {c}' for c in var_columns],
            *[f'{temp_table}.{c} AS _{c}_' for c in var_columns],
            *[f'{main_table}.{c} AS exists_{c}' for c in var_columns],
        ]
        join_columns = [f'{main_table}.{c} == {temp_table}.{c}' for c in self.signature]
        delete_where = [f'exists_{c} IS NOT NULL' for c in var_columns]

        if auto_inc_column:
            cols = [auto_inc_column, f'_{auto_inc_column}_', f'exists_{auto_inc_column}']
            columns.extend(cols)
            create_columns.extend([f'{c} INTEGER' for c in cols])
            select_columns.extend([
                f'coalesce({main_table}.{auto_inc_column}, {temp_table}.{auto_inc_column} + ?) '
                f'AS {auto_inc_column}',
                f'{temp_table}.{auto_inc_column} AS _{auto_inc_column}_',
                f'{main_table}.{auto_inc_column} AS exists_{auto_inc_column}',
            ])
            delete_where.append(f'exists_{auto_inc_column} IS NOT NULL')

        alter = f'ALTER TABLE {secondary_table} RENAME TO {temp_table_name}'
        create = f'CREATE TABLE {secondary_table} ({", ".join(create_columns)})'
        insert = (f'INSERT INTO {secondary_table} ({", ".join(columns)}) '
                  f'SELECT {", ".join(select_columns)} '
                  f'FROM {temp_table} '
                  f'LEFT JOIN {self.name} AS {main_table} '
                  f'ON {" AND ".join(join_columns)}')
        delete = (f'DELETE FROM {secondary_table} '
                  f'WHERE {" OR ".join(delete_where)}')

        def do_match(conn):
            values = ()
            if auto_inc_column:
                max_id = conn.execute(f'SELECT max(rowid) FROM {self.name}')
                max_id = max_id.fetchone()[0]
                max_id = max_id or 0
                values = (max_id,)
            conn.execute(alter)
            conn.execute(create)
            conn.execute(insert, values)

        def do_dedup(conn):
            conn.execute(delete)

        self.match_primary_keys = do_match
        self.dedup_primary_keys = do_dedup

    def _build_match_fk(self):
        if not self.foreign_keys:
            self.match_foreign_keys = no_op
            return

        temp_table_name = f'original_{self.name}'
        secondary_table = f'secondary.{self.name}'
        temp_table = f'secondary.{temp_table_name}'

        auto_inc_column = self.rowid
        local_columns = []
        create_columns = []
        select_columns = []
        joins = []

        for local_column, remote_table, remote_column in self.foreign_keys:
            local_columns.append(local_column)
            create_columns.append(f'{local_column} INTEGER')

            remote_table_name = f'lc_{local_column}_rm_{remote_table}_{remote_column}'

            select_columns.append(f'{remote_table_name}.{remote_column} AS {local_column}')
            joins.append(
                f'LEFT JOIN secondary.{remote_table} AS {remote_table_name} '
                f'ON {remote_table_name}._{remote_column}_ == {temp_table}.{local_column}',
            )

        static_columns = list(self.columns.keys() - set(local_columns) - {auto_inc_column})
        columns = [*local_columns, *static_columns]
        create_columns.extend([f'{c} BLOB' for c in static_columns])
        select_columns.extend([f'{temp_table}.{c} AS {c}' for c in static_columns])

        if auto_inc_column:
            columns.append(auto_inc_column)
            create_columns.append(f'{auto_inc_column} INTEGER')
            select_columns.append(f'{temp_table}.{auto_inc_column} + ? AS {auto_inc_column}')

        alter = f'ALTER TABLE {secondary_table} RENAME TO {temp_table_name}'
        create = f'CREATE TABLE {secondary_table} ({", ".join(create_columns)})'
        insert = (f'INSERT INTO {secondary_table} ({", ".join(columns)}) '
                  f'SELECT {", ".join(select_columns)} '
                  f'FROM {temp_table} '
                  + ' '.join(joins))

        def do_match(conn):
            values = ()
            if auto_inc_column:
                max_id = conn.execute(f'SELECT max(rowid) FROM {self.name}')
                max_id = max_id.fetchone()[0]
                max_id = max_id or 0
                values = (max_id,)
            conn.execute(alter)
            conn.execute(create)
            conn.execute(insert, values)
        self.match_foreign_keys = do_match

    def _build_restore_original(self):
        temp_table = f'original_{self.name}'

        exists = ('SELECT name FROM sqlite_master '
                  "WHERE type == 'table' AND name == ?")
        drop = f'DROP TABLE {self.name}'
        restore = f'ALTER TABLE {temp_table} RENAME TO {self.name}'

        def do_restore(conn):
            table_exists = list(conn.execute(exists, (temp_table,)))
            if table_exists:
                conn.execute(drop)
                conn.execute(restore)
        self.restore_original = do_restore

    def __str__(self):
        return f'<Table {self.name}>'


class DatabaseVersionError(TypeError):
    def __init__(self, db, target, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = db
        self.target = target

    def __str__(self):
        return f'Cannot write to database of version {self.db}; currently supported version: {self.target}'

    def __reduce__(self):
        return self.__class__, (self.db, self.target)


class DatabaseNotEmptyError(ValueError):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __str__(self):
        return 'Database already has other data and cannot be used in this program.'
