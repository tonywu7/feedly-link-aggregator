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

from sqlalchemy import MetaData, types
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.schema import Column, ForeignKey, Index

SCHEMA_VERSION = '0.10.6'

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
CASCADE = 'CASCADE'


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

    url = Column(ForeignKey('url.id', ondelete=RESTRICT, onupdate=CASCADE), nullable=False)
    source = Column(ForeignKey('url.id', ondelete=RESTRICT, onupdate=CASCADE), nullable=False)

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
    source_id = Column(ForeignKey('url.id', ondelete=RESTRICT, onupdate=CASCADE), nullable=False)
    target_id = Column(ForeignKey('url.id', ondelete=RESTRICT, onupdate=CASCADE), nullable=False)
    element = Column(types.String(), nullable=False)

    @declared_attr
    def __table_args__(self):
        return (Index(None, 'source_id', 'target_id', 'element', unique=True),)


class Feed(Base):
    id = Column(types.Integer(), primary_key=True, autoincrement=True)
    url_id = Column(ForeignKey('url.id', ondelete=RESTRICT, onupdate=CASCADE), nullable=False)
    title = Column(types.Text(), nullable=False)
    dead = Column(types.Boolean())

    @declared_attr
    def __table_args__(self):
        return (Index(None, 'url_id', unique=True),)


class Tagging(Base):
    id = Column(types.Integer(), primary_key=True, autoincrement=True)
    url_id = Column(ForeignKey('url.id', ondelete=RESTRICT, onupdate=CASCADE), nullable=False)
    keyword_id = Column(ForeignKey('keyword.id', ondelete=RESTRICT, onupdate=CASCADE), nullable=False)

    @declared_attr
    def __table_args__(self):
        return (Index(None, 'url_id', 'keyword_id', unique=True),)


class Summary(Base):
    id = Column(types.Integer(), primary_key=True, autoincrement=True)
    url_id = Column(ForeignKey('url.id', ondelete=RESTRICT, onupdate=CASCADE), nullable=False)
    markup = Column(types.Text(), nullable=False)

    @declared_attr
    def __table_args__(self):
        return (
            Index(None, 'url_id', unique=True),
            {'info': {'dedup': 'max', 'onconflict': 'REPLACE'}},
        )


class Webpage(Base):
    id = Column(types.Integer(), primary_key=True, autoincrement=True)
    url_id = Column(ForeignKey('url.id', ondelete=RESTRICT, onupdate=CASCADE), nullable=False)
    markup = Column(types.Text(), nullable=False)

    @declared_attr
    def __table_args__(self):
        return (
            Index(None, 'url_id', unique=True),
            {'info': {'dedup': 'max', 'onconflict': 'REPLACE'}},
        )


models = [URL, Keyword, Item, Hyperlink, Feed, Tagging, Summary, Webpage]
tables = [m.__table__ for m in models]

version = SCHEMA_VERSION
init = ['PRAGMA foreign_keys = ON', 'PRAGMA journal_mode = WAL']
