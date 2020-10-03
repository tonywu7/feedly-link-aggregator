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
from collections import defaultdict
from pathlib import Path

import igraph

from ..datastructures import labeled_sequence
from ..sql.utils import offset_fetch
from .utils import filter_by_domains, with_db

log = logging.getLogger('exporter.graph')


def filter_vertices(g, vertex_ids, include, exclude):
    if include or exclude:
        log.info('Filtering graph')
        vertex_ids = {f'http://{k}': i for k, i in vertex_ids.items()}
        if include:
            vertex_ids = {k: i for k, i in vertex_ids.items()
                          if filter_by_domains(include)(k)}
        if exclude:
            vertex_ids = {k: i for k, i in vertex_ids.items()
                          if filter_by_domains(exclude, True)(k)}
        g = g.subgraph(vertex_ids.values())
    return g


def create_hyperlink_graph(db: sqlite3.Connection, include=None, exclude=None):
    SELECT = """
    SELECT
        source.url AS "source",
        target.url AS "target",
        hyperlink.element AS "tag",
        item.published AS "timestamp"
    FROM
        hyperlink
        JOIN url AS source ON source.id == hyperlink.source_id
        JOIN url AS target ON target.id == hyperlink.target_id
        JOIN item ON hyperlink.source_id == item.url
    WHERE
        %(offset)s
    """
    vertices = {}
    edges = {}
    log.debug(SELECT)

    log.info('Reading database')
    for row in offset_fetch(db, SELECT, 'hyperlink', log=log):
        src = row['source']
        dst = row['target']
        vertices[src] = True
        vertices[dst] = True
        edges[(src, dst)] = (row['tag'], row['timestamp'])
    log.info('Finished reading database')

    log.info('Creating graph')
    g = igraph.Graph(directed=True)
    vertex_ids = labeled_sequence(vertices, key=False)
    edges = {(vertex_ids[t[0]], vertex_ids[t[1]]): v for t, v in edges.items()}
    g.add_vertices(len(vertices))
    g.add_edges(edges)
    g.vs['name'] = list(vertices)
    g.es['type'], g.es['timestamp'] = tuple(zip(*edges.values()))
    log.info(f'|V| = {g.vcount()}; |E| = {g.ecount()}')
    g = filter_vertices(g, vertex_ids, include, exclude)
    return g


def create_domain_graph(db: sqlite3.Connection, include=None, exclude=None):
    temp = """
    CREATE TEMP TABLE domains (id INTEGER, domain VARCHAR)
    """
    index = """
    CREATE INDEX temp_ix_domains ON domains (id)
    """
    insert_domains = """
    INSERT INTO domains
    SELECT url.id AS id, urlsplit(url.url, 'netloc') AS domain
    FROM url
    """
    count_domains = """
    SELECT domains.domain, count(domains.domain)
    FROM domains
    GROUP BY domains.domain
    """

    select_pairs = """
    SELECT
        src.domain AS source,
        dst.domain AS target,
        hyperlink.element AS tag,
        count(hyperlink.element) AS count
    FROM
        hyperlink
        JOIN domains AS src ON hyperlink.source_id == src.id
        JOIN domains AS dst ON hyperlink.target_id == dst.id
    WHERE %(offset)s
    GROUP BY
        source,
        target,
        tag
    """
    db.execute('BEGIN EXCLUSIVE')
    db.execute(temp)

    log.info('Building domain list')
    db.execute(insert_domains)
    db.execute(index)

    vertices = {}
    edges = defaultdict(lambda: defaultdict(int))
    attrs = set()

    log.info('Counting domains')
    for domain, count in db.execute(count_domains):
        vertices[domain] = count

    log.info('Fetching hyperlinks')
    for row in offset_fetch(db, select_pairs, 'hyperlink', size=500000, log=log):
        src = row['source']
        dst = row['target']
        tag = row['tag']
        attrs.add(tag)
        edges[(src, dst)][tag] += row['count']

    db.rollback()

    log.info('Creating graph')
    g = igraph.Graph(directed=True)
    vertex_ids = labeled_sequence(vertices, key=False)
    edges = {(vertex_ids[t[0]], vertex_ids[t[1]]): v for t, v in edges.items()}
    g.add_vertices(len(vertices))
    g.add_edges(edges)
    g.vs['name'] = list(vertices)
    g.vs['weight'] = list(vertices.values())
    attrs = {a: tuple(v.get(a, 0) for v in edges.values()) for a in attrs}
    for k, t in attrs.items():
        g.es[k] = t
    g = filter_vertices(g, vertex_ids, include, exclude)
    log.info(f'|V| = {g.vcount()}; |E| = {g.ecount()}')
    return g


@with_db
def export(conn: sqlite3.Connection, wd: Path, output: Path,
           fmt='index.graphml', graphtype='hyperlink',
           include=None, exclude=None, *args, **kwargs):

    reader = {
        'hyperlink': create_hyperlink_graph,
        'domain': create_domain_graph,
    }[graphtype]
    g = reader(conn, include, exclude)
    log.info('Writing')
    with open(output / fmt, 'w+') as f:
        g.save(f, format='graphml')
    log.info('Done.')


help_text = """
Export feed data as graph data.

Synopsis
--------
export ~graph~ -i <input> -o [name] [**graphtype=**~hyperlink|domain~]

Description
-----------
This exporter lets you represent scraped URL data using graph data structure.

**Requires igraph. You must install ~requirements-optional.txt~.**

Currently this exports graphs in ~GraphML~ format only.

This exporter does not support name templates.

Filters
-------

~domain~ ~under~ ...

Include/exclude websites/hyperlinks whose domain name is under the specified
domain.

Options
-------
~graphtype=[hyperlink|domain]~

    **~hyperlink~**
        **Directed, self-loop allowed**
        **Vertices**
            Each ~source~ or ~target~ URL (representing a file on a website);
            **Attributes**
                ~name~: The URL
        **Edges**
            Each hyperlink found in ~source~ pointing to ~target~;
            **Attributes**
                ~type~: The HTML element
                ~timestamp~: UTC date and time when ~source~ was published,
                  in ISO-8601 format

    **~domain~**
        **Directed, self-loop allowed**
        **Vertices**
            Domains of each URL
            **Attributes**
                ~name~: Domain name
                ~weight~: The number of files found under the domain
        **Edges**
            Each hyperlink found in ~source~ pointing to ~target~ creates an
              edge from ~source:domain~ to ~target:domain~; not repeated.
            **Attributes**
                ~<tag names...>~: Each hyperlink in ~source~ pointing to
                  ~target~ that is found on a particular HTML tag increases
                  the ~<tag>~ attribute by 1.
"""
