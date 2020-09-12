import operator
import os
import sqlite3
from functools import wraps
from pathlib import Path

from ..sql import SCHEMA_VERSION
from ..sql.functions import register_all
from ..sql.utils import verify_version


def subdomain(x, y):
    return x == y or x[-(len(y) + 1):] == f'.{y}'


filter_ops = {
    'is': operator.eq,
    'under': subdomain,
}
sql_ops = {
    'is': '"%(column)s" == :%(id)d',
    'under': 'subdomain("%(column)s", :%(id)d)',
}


def build_where_clause(includes=None, excludes=None):
    if not includes and not excludes:
        return 'TRUE', ()
    values = []
    includes = includes or []
    excludes = excludes or []
    clauses = []
    for prefix, criteria in (('', includes), ('NOT ', excludes)):
        for key, op, val in criteria:
            values.append(val)
            value_id = len(values)
            clauses.append(prefix + sql_ops[op] % {'column': key, 'id': value_id})
    clauses = ' AND '.join(clauses)
    values = dict(zip(range(1, len(values) + 1), values))
    values = {str(k): v for k, v in values.items()}
    return clauses, values


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
    def e(wd: Path, *args, **kwargs):
        output = wd.joinpath('out')
        os.makedirs(output, exist_ok=True)

        db_path = wd.joinpath('index.db')
        if not db_path.exists():
            raise FileNotFoundError(f'index.db not found in {wd}')

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        verify_version(conn, SCHEMA_VERSION)
        register_all(conn)

        try:
            exporter(conn, wd, output, *args, **kwargs)
        finally:
            conn.close()
    return e