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
import sqlite3

import simplejson as json

from ..utils import colored as _
from ..utils import read_jsonlines
from . import SCHEMA_VERSION
from .identity import load_identity_config
from .utils import create_all, select_max_rowid, verify_version

log = logging.getLogger('feedly.db.streamreader')

BREAK = object()

TABLES = ['url', 'keyword', 'item', 'hyperlink', 'feed', 'tagging', 'summary', 'webpage']


def table_consumer(get_id, translate, get_remote_keys):
    def consumer(table_name, identity_keys):
        ident = identity_keys[table_name]
        rows = []

        while True:
            data = yield
            if data is BREAK:
                break
            rows.append(data)
        yield

        if translate:
            remote_maps = get_remote_keys(identity_keys)
            for row in rows:
                translate(row, *remote_maps)

        rows = {get_id(row): row for row in rows}
        rows = [row for key, row in rows.items() if key not in ident]
        yield rows
    return consumer


def consume_stream(db_path, stream, batch_count):
    conn = sqlite3.connect(db_path)

    create_all(conn)
    verify_version(conn, SCHEMA_VERSION)

    inserts, ident_funcs, remote_funcs = load_identity_config()
    consumer_funcs = {table: table_consumer(ident_funcs[table][1], *remote_funcs[table]) for table in TABLES}

    stream_iter = read_jsonlines(stream, paginate=batch_count)
    line_no = 1
    item_no = 0
    num_records = 0
    stream_has_data = True
    while stream_has_data:

        identity_keys = {table: funcs[0](conn) for table, funcs in ident_funcs.items()}
        consumers = {table: create(table, identity_keys) for table, create in consumer_funcs.items()}

        for c in consumers.values():
            c.send(None)

        while True:
            try:
                line_no, item_no, data = next(stream_iter)
                if data is None:
                    break

                for rowtype, rows in data.items():
                    for row in rows:
                        consumers[rowtype].send(row)

            except StopIteration:
                stream_has_data = False
                break
            except json.JSONDecodeError as e:
                log.error(e)
                log.error(f'Corrupted data on line {line_no} in stream. Aborting!')
                return

        for c in consumers.values():
            c.send(BREAK)

        with conn:
            for table in TABLES:
                consumer = consumers.pop(table)
                insert = inserts[table]

                select_identity = ident_funcs[table][0]
                rowid = select_max_rowid(conn, table)

                values = next(consumer)
                conn.executemany(insert, values)
                identity_keys[table].update(select_identity(conn, rowid))

                num_records += len(values)
                log.info(f'  {table}: {len(identity_keys[table])} (+{len(values)})')
                consumer.close()
                del values
                del consumer

        log.info(_(f'Loaded {item_no + 1} items', color='green'))
        log.info(_(f'Saved {num_records} new records in total', color='green'))

    conn.close()
