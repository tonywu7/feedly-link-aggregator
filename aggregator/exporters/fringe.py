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

import simplejson as json
from scrapy.utils.url import url_is_from_any_domain

from ..sql.utils import bulk_fetch
from .utils import with_db

log = logging.getLogger('exporter.fringe')


@with_db
def export_disabled(conn: sqlite3.Connection, wd: Path, output: Path,
                    include=None, exclude=None,
                    fmt='fringe.json', *args, **kwargs):

    def parse_filters(ls):
        domains = []
        for key, op, val in ls:
            if key != 'domain' or op != 'under':
                log.warning(f'Unknown filter {key} {op}')
                continue
            domains.append(val)
        return domains

    tests = []
    if include:
        includes = parse_filters(include)
        if includes:
            tests.append(lambda u: url_is_from_any_domain(u, includes))
    if exclude:
        excludes = parse_filters(exclude)
        if excludes:
            tests.append(lambda u: not url_is_from_any_domain(u, excludes))

    conn.execute('BEGIN')
    conn.executescript(
        """
        CREATE TEMP TABLE domains (id INTEGER, domain VARCHAR);

        CREATE TEMP TABLE indegrees (
            srcd VARCHAR,
            dstd VARCHAR,
            indegree INTEGER
        );

        CREATE INDEX ixd ON domains (id, domain);

        CREATE INDEX ixdg ON indegrees (dstd);

        INSERT INTO
            domains (id, domain)
        SELECT
            url.id AS id,
            urlsplit(url.url, 'netloc') AS domain
        FROM
            url;

        INSERT INTO
            indegrees (srcd, dstd, indegree)
        SELECT
            srcd.domain AS srcd,
            dstd.domain AS dstd,
            count(distinct srcd.domain) AS indegree
        FROM
            hyperlink
            JOIN domains AS srcd ON hyperlink.source_id == srcd.id
            JOIN domains AS dstd ON hyperlink.target_id == dstd.id
        GROUP BY
            dstd;
        """,
    )

    indegrees = (
        """
        WITH weights AS (
            SELECT
                domains.domain AS domain,
                count(domains.domain) AS weight
            FROM
                domains
            GROUP BY
                domain
        )
        SELECT
            indegrees.srcd AS source,
            indegrees.dstd AS target,
            indegrees.indegree AS indegree,
            weights.weight AS weight
        FROM
            indegrees
            JOIN weights ON indegrees.dstd == weights.domain
        WHERE
            indegrees.indegree == 1
        """
    )

    keywords = (
        """
        SELECT
            indegrees.dstd AS domain,
            keyword.keyword AS keyword,
            count(keyword.keyword) AS numkw
        FROM
            indegrees
            JOIN domains ON indegrees.dstd == domains.domain
            JOIN tagging ON domains.id == tagging.url_id
            JOIN keyword ON tagging.keyword_id == keyword.id
        WHERE
            indegrees.indegree == 1
        GROUP BY
            domain, keyword
        """
    )

    tags = (
        """
        SELECT
            indegrees.dstd AS domain,
            hyperlink.element AS tag,
            count(hyperlink.element) AS numtag
        FROM
            hyperlink
            JOIN domains ON hyperlink.source_id == domains.id
            JOIN indegrees ON domains.domain == indegrees.dstd
        WHERE
            indegrees.indegree == 1
        GROUP BY
            domain,
            tag
        """
    )

    sites = {}

    log.info('Calculating indegrees...')
    for r in bulk_fetch(conn.execute(indegrees), log=log):
        info = sites[r['target']] = {}
        info['referrer'] = r['source']
        info['weight'] = r['weight']
        info['keywords'] = {}
        info['tags'] = {}

    log.info('Getting keyword info...')
    for r in bulk_fetch(conn.execute(keywords), log=log):
        sites[r['domain']]['keywords'][r['keyword']] = r['numkw']

    log.info('Getting HTML tag info...')
    for r in bulk_fetch(conn.execute(tags), log=log):
        sites[r['domain']]['tags'][r['tag']] = r['numtag']

    sites = {f'http://{k}': v for k, v in sites.items()
             if all(t(f'http://{k}') for t in tests)}
    with open(output / fmt, 'w+') as f:
        json.dump(sites, f)

    conn.rollback()


help_text = """
Export a list of websites that are on the fringe in a feed network.

That is, if you are using the cluster spider, this exporter will export the list
of websites that are not crawled due to the spider hitting the depth limit
— the outermost nodes.

(In graph terms, this exporter exports all vertices whose indegree is 1.)

Note that websites that are on the "fringe" because they were filtered out
by the domain filter (instead of hitting the depth limit) will also be included
here. To only export sites under a certain domain, use ~+f domain under ...~,
to exclude a certain domain, use ~-f domain under ...~.
"""
