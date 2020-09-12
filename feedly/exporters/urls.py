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
from pathlib import Path

from .exporters import MappingCSVExporter, MappingLineExporter
from .utils import build_where_clause, with_db


SELECT = """
SELECT
    feed.url AS "feed:url",
    source.url AS "source:url",
    target.url AS "target:url",
    hyperlink.html_tag AS "html_tag",
    %(urlexpansions)s
FROM
    hyperlink
    JOIN url AS source ON source.id == hyperlink.source_id
    JOIN url AS target ON target.id == hyperlink.target_id
    JOIN item ON hyperlink.source_id == item.url
    JOIN url AS feed ON item.source == feed.id
"""
expansions = []
columns = ('feed', 'source', 'target')
for column in columns:
    for attr in ('scheme', 'netloc', 'path'):
        expansions.append(f'urlsplit({column}.url, "{attr}") AS "{column}:{attr}"')
expansions = ', '.join(expansions)
SELECT = SELECT % {'urlexpansions': expansions}


@with_db
def export(conn: sqlite3.Connection, wd: Path, output: Path, include, exclude, fmt, key='target:url', format='lines'):
    where, values = build_where_clause(include, exclude)
    select = f'{SELECT} WHERE {where}'

    cls, args = {
        'lines': (MappingLineExporter, (key, output, fmt)),
        'csv': (MappingCSVExporter, (None, output, fmt)),
    }[format]

    with cls(*args) as exporter:
        for row in conn.execute(select, values):
            exporter.write(row)
