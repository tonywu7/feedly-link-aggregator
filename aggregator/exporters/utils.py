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
import operator
import os
import sqlite3
from functools import wraps
from pathlib import Path

from scrapy.utils.url import url_is_from_any_domain

from ..datastructures import labeled_sequence
from ..sql.db import db
from ..sql.functions import register_all

log = logging.getLogger('exporter.utils')


def subdomain(x, y):
    return x == y or x[-(len(y) + 1):] == f'.{y}'


filter_ops = {
    'is': operator.eq,
    'under': subdomain,
    'startswith': str.startswith,
    'endswith': str.endswith,
    'contains': operator.contains,
    'gt': operator.gt,
    'ge': operator.ge,
    'lt': operator.lt,
    'le': operator.le,
}
sql_ops = {
    ('is', 'None'): ('"%(column)s" IS NULL', '%s'),
    ('is', 'True'): ('"%(column)s" == 1', '%s'),
    ('is', 'False'): ('"%(column)s" == 0', '%s'),
    'is': ('"%(column)s" == :%(id)d', '%s'),
    'under': ('subdomain("%(column)s", :%(id)d)', '%s'),
    'startswith': ('"%(column)s" LIKE :%(id)d', '%s%%'),
    'endswith': ('"%(column)s" LIKE :%(id)d', '%%%s'),
    'contains': ('"%(column)s" LIKE :%(id)d', '%%%s%%'),
    'gt': ('"%(column)s" > :%(id)d', '%s'),
    'ge': ('"%(column)s" >= :%(id)d', '%s'),
    'lt': ('"%(column)s" < :%(id)d', '%s'),
    'le': ('"%(column)s" <= :%(id)d', '%s'),
}
equivalencies = [('==', 'is'), ('in', 'contains'), ('>', 'gt'), ('<', 'lt'), ('>=', 'ge'), ('<=', 'le')]
for k, v in equivalencies:
    filter_ops[k] = filter_ops[v]
    sql_ops[k] = sql_ops[v]


def build_where_clause(includes=None, excludes=None):
    if not includes and not excludes:
        return '1', (), set()
    values = []
    includes = includes or []
    excludes = excludes or []
    clauses = []
    required_columns = set()
    for prefix, criteria in (('', includes), ('NOT ', excludes)):
        for key, op, val in criteria:
            required_columns.add(key)
            op = sql_ops.get((op, val), sql_ops[op])
            values.append(op[1] % (val,))
            value_id = len(values)
            clauses.append(prefix + op[0] % {'column': key, 'id': value_id})
    clauses = ' AND '.join(clauses)
    values = labeled_sequence(values, start=1, as_str=True)
    return clauses, values, required_columns


class MappingFilter:
    def __init__(self):
        self.filters = []

    def includes(self, key, op, val):
        self.filters.append(lambda row, x=key, y=val, op=filter_ops[op]: op(row[x], y))

    def excludes(self, key, op, val):
        self.filters.append(lambda row, x=key, y=val, op=filter_ops[op]: not op(row[x], y))

    def __call__(self, item):
        return all(f(item) for f in self.filters)


def with_db(exporter):
    @wraps(exporter)
    def e(wd, *args, **kwargs):
        wd = Path(wd)
        output = wd / 'out'
        os.makedirs(output, exist_ok=True)

        db_path = wd / 'index.db'
        if not db_path.exists():
            raise FileNotFoundError(f'index.db not found in {wd}')

        conn = sqlite3.connect(db_path, isolation_level=None)
        if db.is_locked(conn):
            log.error('Database was left in a partially consistent state.')
            log.error('Run `python -m aggregator check-db` to fix it first.')
            return 1

        conn.row_factory = sqlite3.Row
        db.verify_version(conn)
        register_all(conn)

        try:
            exporter(conn, wd, output, *args, **kwargs)
        finally:
            conn.close()
    return e


def filter_by_domains(ls, exclude=False):
    domains = []
    for key, op, val in ls:
        if key != 'domain' or op != 'under':
            log.warning(f'Unknown filter {key} {op}')
            continue
        domains.append(val)
    return lambda u: url_is_from_any_domain(u, domains) ^ exclude
