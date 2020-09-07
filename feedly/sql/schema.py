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
from sqlalchemy.schema import (Column, CreateIndex, CreateTable, ForeignKey,
                               Index, PrimaryKeyConstraint, UniqueConstraint)
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
    url = Column(types.String(), unique=True, nullable=False)

    @declared_attr
    def __table_args__(self):
        return (Index(None, 'id', 'url'),)


class Item(Base):
    id = Column(types.Integer(), primary_key=True, autoincrement=True)
    hash = Column(types.String(length=40), unique=True, nullable=False)

    url = Column(ForeignKey('url.id'), nullable=False)
    source = Column(ForeignKey('url.id'), nullable=False)

    author = Column(types.String())
    published = Column(types.DateTime())
    updated = Column(types.DateTime())
    crawled = Column(types.Float())

    @declared_attr
    def __table_args__(self):
        return (Index(None, 'id', 'hash'),)


class Feed(Base):
    url_id = Column(ForeignKey('url.id'), primary_key=True)
    title = Column(types.Text(), nullable=False)


class Keyword(Base):
    id = Column(types.Integer(), primary_key=True, autoincrement=True)
    keyword = Column(types.String(), unique=True, nullable=False)


class Tagging(Base):
    item_id = Column(ForeignKey('item.id'), primary_key=True)
    keyword_id = Column(ForeignKey('keyword.id'), primary_key=True)


class Markup(Base):
    id = Column(types.Integer(), primary_key=True, autoincrement=True)
    item_id = Column(ForeignKey('item.id'), nullable=False)
    type = Column(types.String(), nullable=False)
    markup = Column(types.Text(), nullable=False)

    @declared_attr
    def __table_args__(self):
        return (Index(None, 'item_id', 'type', unique=True),)


class Hyperlink(Base):
    source_id = Column(ForeignKey('url.id'), primary_key=True)
    target_id = Column(ForeignKey('url.id'), primary_key=True)
    html_tag = Column(types.String(), nullable=False)


models = [m for m in Base._decl_class_registry.values() if isinstance(m, type) and issubclass(m, Base)]
tables = {m.__tablename__: m.__table__ for m in models}


def inspect_identity(table):
    config = {}
    for name, column in table.columns.items():
        if column.autoincrement is True:
            config['autoincrement'] = (name,)
            break
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
    return config


def export_pragma():
    with open(CWD.joinpath('commands', 'init_1_pragma.sql'), 'w') as f:
        f.write('PRAGMA foreign_keys = ON;\nPRAGMA journal_mode=WAL;\n')


def export_schema():
    with open(CWD.joinpath('commands', 'init_2_tables.sql'), 'w') as f:
        for table in tables.values():
            stmt = CreateTable(table).compile(dialect=sqlite.dialect())
            stmt = str(stmt).replace('TABLE', 'TABLE IF NOT EXISTS')
            f.write(stmt)
            f.write(';\n')


def export_indices():
    with open(CWD.joinpath('commands', 'init_3_indices.sql'), 'w') as f:
        for table in tables.values():
            for index in table.indexes:
                stmt = CreateIndex(index).compile(dialect=sqlite.dialect())
                stmt = str(stmt).replace('INDEX', 'INDEX IF NOT EXISTS')
                f.write(stmt)
                f.write(';\n')


def export_selectall():
    for name, table in tables.items():
        with open(CWD.joinpath('commands', f'selectall_{name}.sql'), 'w') as f:
            sql = select('*').select_from(table)
            f.write(str(sql))


def export_identities():
    config = {}
    for name, table in tables.items():
        config[name] = inspect_identity(table)
    with open(CWD.joinpath('metadata', 'identity.json'), 'w') as f:
        json.dump(config, f, iterable_as_array=True, sort_keys=True)


if __name__ == '__main__':
    export_pragma()
    export_schema()
    export_indices()
    export_identities()
