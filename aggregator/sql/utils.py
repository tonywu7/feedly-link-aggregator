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


def bulk_fetch(cur, size=100000, log=None):
    i = 0
    rows = cur.fetchmany(size)
    while rows:
        for row in rows:
            i += 1
            yield row
        if log:
            log.info(f'Fetched {i} rows.')
        rows = cur.fetchmany(size)


def offset_fetch(conn, stmt, table, *, values=(), size=100000, log=None):
    i = 0
    offset = 0
    max_id = conn.execute(f'SELECT max(rowid) FROM {table}').fetchone()[0]
    if not max_id:
        raise StopIteration
    while offset <= max_id:
        limited = stmt % {'offset': (
            f'{table}.rowid IN '
            f'(SELECT rowid FROM {table} '
            f'ORDER BY rowid LIMIT {size} OFFSET {offset})'
        )}
        rows = conn.execute(limited, values)
        for row in rows:
            i += 1
            yield row
        if log and i:
            log.info(f'Fetched {i} rows.')
        offset += size
