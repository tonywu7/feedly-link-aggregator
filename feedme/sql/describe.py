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

import sys
from importlib.util import module_from_spec, spec_from_file_location
from typing import Dict, List

import simplejson as json
from sqlalchemy import Table
from sqlalchemy.dialects import sqlite
from sqlalchemy.schema import (CreateIndex, CreateTable, Index,
                               PrimaryKeyConstraint, UniqueConstraint)


def describe_model(table):
    config = {
        'columns': {},
        'info': table.info,
        'autoincrement': [None],
        'primary_key': set(),
        'unique': set(),
        'foreign_keys': set(),
    }
    for name, column in table.columns.items():
        config['columns'][name] = column.info
        if column.autoincrement is True:
            config['autoincrement'] = (name,)
    for constraint in table.constraints:
        cols = tuple(sorted(c.name for c in constraint.columns))
        if isinstance(constraint, PrimaryKeyConstraint):
            config['primary_key'] = cols
        if isinstance(constraint, UniqueConstraint):
            s = config.setdefault('unique', set())
            s.add(cols)
    for index in table.indexes:
        if index.unique:
            cols = tuple(sorted(c.name for c in index.columns))
            s = config.setdefault('unique', set())
            s.add(cols)
    fks = []
    for constraint in table.foreign_key_constraints:
        column = constraint.column_keys[0]
        foreign_column = list(constraint.columns[column].foreign_keys)[0].column
        fks.append((column, foreign_column.table.name, foreign_column.name))
    config['foreign_keys'] = fks
    return config


def create_table(table: Table):
    stmt = CreateTable(table).compile(dialect=sqlite.dialect())
    stmt = str(stmt).replace('TABLE', 'TABLE IF NOT EXISTS').strip()
    return stmt


def create_index(table: Table):
    creates = {}
    for index in table.indexes:
        index: Index
        stmt = CreateIndex(index).compile(dialect=sqlite.dialect())
        stmt = str(stmt).replace('INDEX', 'INDEX IF NOT EXISTS').strip()
        creates[index.name] = stmt
    return creates


def describe_database(path, out):
    spec = spec_from_file_location('schema', path)
    schema = module_from_spec(spec)
    spec.loader.exec_module(schema)

    tables: List[Table] = schema.tables
    tablemap: Dict[str, Table] = {t.name: t for t in tables}
    meta = {}
    meta['order'] = [t.name for t in tables]

    version = schema.version
    meta['version'] = version
    vers = meta['versioning'] = {}
    vers['create'] = create_table(schema.__Version__.__table__)
    vers['insert'] = 'INSERT OR REPLACE INTO __version__ (version) VALUES (?)'

    init: List[str] = schema.init
    tables_create = {}
    indices_create = {}
    for t in tables:
        tables_create[t.name] = create_table(t)
        indices_create.update(create_index(t))
    meta['init'] = init
    meta['tables'] = tables_create
    meta['indices'] = indices_create

    models = meta['models'] = {}
    for name, table in tablemap.items():
        models[name] = describe_model(table)

    with open(out, 'w+') as f:
        json.dump(meta, f, iterable_as_array=True)


if __name__ == '__main__':
    describe_database(sys.argv[1], sys.argv[2])
