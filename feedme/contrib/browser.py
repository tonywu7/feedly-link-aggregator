import logging
import os
import random
import re
import sqlite3
from pathlib import Path

import aiofiles
import aiohttp
from aiohttp import web
from more_itertools import chunked

from ..exporters.urls import CTE, SELECT, build_ctes, build_where_clause
from ..sql.db import db
from ..sql.functions import register_all
from ..sql.utils import offset_fetch

SUFFIX = re.compile(r'_\d+\.(jpg|png|gif)$', re.IGNORECASE)


class ResourceIterator:
    def __init__(self, conn: sqlite3.Connection, pattern: str):
        self.conn = conn
        self.log = logging.getLogger('iterator')
        self.pattern = pattern

    def get_row_iterator(self):
        cte, column_maps = build_ctes(CTE)
        keys = ('target:url',)
        includes = [('tag', 'is', 'img'),
                    ('source:netloc', 'contains', self.pattern),
                    ('target:netloc', 'under', 'media.tumblr.com')]
        where, values, _ = build_where_clause(includes, [])
        columns = ', '.join([f'{v} AS "{k}"' for k, v in column_maps.items()])
        column_keys = ', '.join([f'"{k}"' for k in keys])

        select = SELECT % {'columns': columns}
        select = f'{cte}{select} WHERE %(offset)s AND {where} GROUP BY {column_keys}'

        fetch = offset_fetch(self.conn, select, 'hyperlink', values=values, log=self.log, size=200000)
        return fetch

    def __iter__(self):
        while True:
            fetch = self.get_row_iterator()
            for chunk in chunked(fetch, 10000):
                random.shuffle(chunk)
                yield from chunk


class ResourceIteratorApp(web.Application):
    def __init__(self, *args, index: Path, pattern: str = 'tumblr.com', **kwargs):
        super().__init__(*args, **kwargs)

        db_path = index / 'index.db'
        conn = sqlite3.connect(db_path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        db.verify_version(conn)
        register_all(conn)
        self.iterator = iter(ResourceIterator(conn, pattern))

        self.output = index / 'cache'
        os.makedirs(self.output, exist_ok=True)

        self.add_routes([
            web.get('/', self.index),
        ])

        self.client = aiohttp.ClientSession(headers={'User-Agent': 'curl/7.64.1'})
        self.on_cleanup.append(self.close)

    async def index(self, req: web.Request):
        row = next(self.iterator)
        url = row['target:url']
        url = re.sub(SUFFIX, r'_1280.\g<1>', url)
        url = url.replace('http://', 'https://')
        async with self.client.get(url) as res:
            data = await res.read()
            output = self.output / f'{row["source:netloc"]}/{row["target:path"]}'
            os.makedirs(output.parent, exist_ok=True)
            async with aiofiles.open(output, 'wb+') as f:
                await f.write(data)
            return web.Response(body=data, content_type=res.content_type)

    async def close(self, *args, **kwargs):
        await self.client.close()


def run_app(index, pattern):
    index = Path(index)
    app = ResourceIteratorApp(index=index, pattern=pattern)
    web.run_app(app, host='0.0.0.0', port=5000)
