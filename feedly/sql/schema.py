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

from pathlib import Path

import simplejson as json
from sqlalchemy import MetaData, types
from sqlalchemy.dialects import sqlite
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.schema import (Column, CreateIndex, CreateTable, DropIndex,
                               ForeignKey, Index, PrimaryKeyConstraint,
                               UniqueConstraint)
from sqlalchemy.sql import select

metadata = MetaData(
    naming_convention={
        'ix': 'ix_%(table_name)s_%(column_0_N_name)s',
        'uq': 'uq_%(table_name)s_%(column_0_N_name)s',
        'ck': 'ck_%(table_name)s_%(column_0_N_name)s',
        'fk': 'fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s',
        'pk': 'pk_%(table_name)s',
    },
)
RESTRICT = 'RESTRICT'

CWD = Path(Path(__file__).parent)


class BaseDefaults:
    @declared_attr
    def __tablename__(self):
        return self.__name__.lower()


Base = declarative_base(metadata=metadata, cls=BaseDefaults)


class __Version__(Base):
    version = Column(types.String(), primary_key=True)


class URL(Base):
    id = Column(types.Integer(), primary_key=True, autoincrement=True)
    url = Column(types.String(), nullable=False)

    @declared_attr
    def __table_args__(self):
        return (Index(None, 'url', unique=True),)


class Keyword(Base):
    id = Column(types.Integer(), primary_key=True, autoincrement=True)
    keyword = Column(types.String(), nullable=False)

    @declared_attr
    def __table_args__(self):
        return (Index(None, 'keyword', unique=True),)


class Item(Base):
    id = Column(types.Integer(), primary_key=True, autoincrement=True)

    url = Column(ForeignKey('url.id', ondelete=RESTRICT, onupdate=RESTRICT), nullable=False)
    source = Column(ForeignKey('url.id', ondelete=RESTRICT, onupdate=RESTRICT), nullable=False)

    title = Column(types.String())
    author = Column(types.String())
    published = Column(types.DateTime())
    updated = Column(types.DateTime())
    crawled = Column(types.Float())

    @declared_attr
    def __table_args__(self):
        return (Index(None, 'url', unique=True),)


class Hyperlink(Base):
    id = Column(types.Integer(), primary_key=True, autoincrement=True)
    source_id = Column(ForeignKey('url.id', ondelete=RESTRICT, onupdate=RESTRICT), nullable=False)
    target_id = Column(ForeignKey('url.id', ondelete=RESTRICT, onupdate=RESTRICT), nullable=False)
    element = Column(types.String(), nullable=False)

    @declared_attr
    def __table_args__(self):
        return (Index(None, 'source_id', 'target_id', 'element', unique=True),)


class Feed(Base):
    id = Column(types.Integer(), primary_key=True, autoincrement=True)
    url_id = Column(ForeignKey('url.id', ondelete=RESTRICT, onupdate=RESTRICT), nullable=False)
    title = Column(types.Text(), nullable=False)
    dead = Column(types.Boolean())

    @declared_attr
    def __table_args__(self):
        return (Index(None, 'url_id', unique=True),)


class Tagging(Base):
    id = Column(types.Integer(), primary_key=True, autoincrement=True)
    url_id = Column(ForeignKey('url.id', ondelete=RESTRICT, onupdate=RESTRICT), nullable=False)
    keyword_id = Column(ForeignKey('keyword.id', ondelete=RESTRICT, onupdate=RESTRICT), nullable=False)

    @declared_attr
    def __table_args__(self):
        return (Index(None, 'url_id', 'keyword_id', unique=True),)


class Summary(Base):
    id = Column(types.Integer(), primary_key=True, autoincrement=True)
    url_id = Column(ForeignKey('url.id', ondelete=RESTRICT, onupdate=RESTRICT), nullable=False)
    markup = Column(types.Text(), nullable=False)

    @declared_attr
    def __table_args__(self):
        return Index(None, 'url_id', unique=True), {'info': {'dedup': 'max'}}


class Webpage(Base):
    id = Column(types.Integer(), primary_key=True, autoincrement=True)
    url_id = Column(ForeignKey('url.id', ondelete=RESTRICT, onupdate=RESTRICT), nullable=False)
    markup = Column(types.Text(), nullable=False)

    @declared_attr
    def __table_args__(self):
        return Index(None, 'url_id', unique=True), {'info': {'dedup': 'max'}}


models = [m for m in Base._decl_class_registry.values() if isinstance(m, type) and issubclass(m, Base)]
tables = {m.__tablename__: m.__table__ for m in models}

table_sequence = [URL, Keyword, Item, Hyperlink, Feed, Tagging, Summary, Webpage]


def inspect_identity(table):
    config = {'columns': {}, 'info': table.info}
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


def export_pragma():
    with open(CWD / 'commands' / 'pragma.sql', 'w') as f:
        f.write('PRAGMA foreign_keys = ON;\nPRAGMA journal_mode = WAL;\n')


def export_version():
    with open(CWD / 'commands' / 'version.sql', 'w') as f:
        stmt = CreateTable(__Version__.__table__).compile(dialect=sqlite.dialect())
        stmt = str(stmt).replace('TABLE', 'TABLE IF NOT EXISTS')
        f.write(stmt)
        f.write(';\n')


def export_schema():
    with open(CWD / 'commands' / 'create-tables.sql', 'w') as f:
        for table in table_sequence:
            table = tables[table.__tablename__]
            stmt = CreateTable(table).compile(dialect=sqlite.dialect())
            stmt = str(stmt).replace('TABLE', 'TABLE IF NOT EXISTS')
            f.write(stmt)
            f.write(';\n')


def export_indices():
    with open(CWD / 'commands' / 'create-indices.sql', 'w') as f1,\
         open(CWD / 'commands' / 'drop-indices.sql', 'w') as f2:

        for table in tables.values():
            for index in table.indexes:
                stmt = CreateIndex(index).compile(dialect=sqlite.dialect())
                stmt = str(stmt).replace('INDEX', 'INDEX IF NOT EXISTS')
                f1.write(stmt)
                f1.write(';\n')
                stmt = DropIndex(index).compile(dialect=sqlite.dialect())
                stmt = str(stmt).replace('INDEX', 'INDEX IF EXISTS')
                f2.write(stmt)
                f2.write(';\n')


def export_selectall():
    for name, table in tables.items():
        with open(CWD / 'commands' / f'selectall_{name}.sql', 'w') as f:
            sql = select('*').select_from(table)
            f.write(str(sql))


def export_identities():
    config = {}
    for name, table in tables.items():
        config[name] = inspect_identity(table)
    with open(CWD / 'metadata' / 'models.json', 'w') as f:
        json.dump(config, f, iterable_as_array=True, sort_keys=True)


def export_tables():
    tablenames = [m.__tablename__ for m in table_sequence]
    with open(CWD / 'metadata' / 'tables.json', 'w') as f:
        json.dump(tablenames, f, iterable_as_array=True, sort_keys=True)


if __name__ == '__main__':
    export_pragma()
    export_version()
    export_schema()
    export_indices()
    export_identities()
    export_tables()
