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
from pathlib import Path

import igraph

from .utils import with_db
from ..sql.utils import bulk_fetch

log = logging.getLogger('graph-exporter')


def create_hyperlink_graph(db):
    SELECT = """
    SELECT
        source.url AS "source",
        target.url AS "target",
        hyperlink.html_tag AS "html_tag",
        item.published AS "timestamp"
    FROM
        hyperlink
        JOIN url AS source ON source.id == hyperlink.source_id
        JOIN url AS target ON target.id == hyperlink.target_id
        JOIN item ON hyperlink.source_id == item.url
    """
    vertices = {}
    edges = {}

    log.info('Reading database...')
    for row in bulk_fetch(db.execute(SELECT), log=log):
        src = row['source']
        dst = row['target']
        vertices[src] = True
        vertices[dst] = True
        edges[(src, dst)] = (row['html_tag'], row['timestamp'])
    log.info('Finished reading database...')

    log.info('Creating graph...')
    g = igraph.Graph(directed=True)
    vertex_ids = {k: i for k, i in zip(vertices, range(len(vertices)))}
    edges = {(vertex_ids[t[0]], vertex_ids[t[1]]): v for t, v in edges.items()}
    g.add_vertices(len(vertices))
    g.add_edges(edges)
    g.vs['name'] = list(vertices)
    g.es['type'], g.es['timestamp'] = tuple(zip(*edges.values()))

    return g


def create_domain_graph(db):
    SELECT = """
    WITH domains AS (
        SELECT
            url.id AS id,
            url.url AS url,
            urlsplit(url.url, 'netloc') AS domain
        FROM
            url
    ),
    edges AS (
        SELECT
            hyperlink.source_id AS source,
            hyperlink.target_id AS target,
            hyperlink.html_tag AS tag
        FROM
            hyperlink
    )
    SELECT
        src.domain AS source,
        dst.domain AS target,
        edges.tag AS tag,
        count(edges.tag) AS count
    FROM
        edges
        JOIN domains AS src ON edges.source == src.id
        JOIN domains AS dst ON edges.target == dst.id
    GROUP BY
        source,
        target,
        tag
    """
    vertices = {}
    edges = {}
    attrs = set()

    log.info('Reading database...')
    for row in bulk_fetch(db.execute(SELECT), log=log):
        src = row['source']
        dst = row['target']
        vertices[src] = True
        vertices[dst] = True
        tag = row['tag']
        attrs.add(tag)
        counts = edges.setdefault((src, dst), {})
        counts[tag] = counts.get(tag, 0) + row['count']
    log.info('Finished reading database...')

    log.info('Creating graph...')
    g = igraph.Graph(directed=True)
    vertex_ids = {k: i for k, i in zip(vertices, range(len(vertices)))}
    edges = {(vertex_ids[t[0]], vertex_ids[t[1]]): v for t, v in edges.items()}
    g.add_vertices(len(vertices))
    g.add_edges(edges)
    g.vs['name'] = list(vertices)
    attrs = {a: tuple(v.get(a, 0) for v in edges.values()) for a in attrs}
    for k, t in attrs.items():
        g.es[k] = t

    return g


@with_db
def export(conn: sqlite3.Connection, wd: Path, output: Path, fmt='index.graphml', graph_type='hyperlink', *args, **kwargs):
    reader = {
        'hyperlink': create_hyperlink_graph,
        'domain': create_domain_graph,
    }[graph_type]
    g = reader(conn)

    log.info('Writing...')
    with open(output.joinpath(fmt), 'w+') as f:
        g.save(f, format='graphml')
