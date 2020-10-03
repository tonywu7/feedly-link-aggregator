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

import simplejson as json

from .utils import filter_by_domains, with_db

log = logging.getLogger('exporter.uncharted')


@with_db
def export(conn: sqlite3.Connection, wd: Path, output: Path,
           include=None, exclude=None,
           fmt='uncharted.json', *args, **kwargs):

    temp = """
    CREATE TEMP TABLE domains (id INTEGER, domain VARCHAR)
    """
    index = """
    CREATE INDEX temp_ix_domains ON domains (id)
    """
    insert_domains = """
    INSERT INTO domains
    SELECT url.id AS id, 'http://' || urlsplit(url.url, 'netloc') AS domain
    FROM url
    """
    count_domains = """
    SELECT domains.domain, count(domains.domain)
    FROM domains
    GROUP BY domains.domain
    """
    select_feeds = """
    SELECT domains.domain
    FROM feed
    JOIN domains ON feed.url_id == domains.id
    GROUP BY domains.domain
    """
    select_keywords = """
    SELECT domains.domain, keyword.keyword, count(keyword.keyword)
    FROM tagging
    JOIN domains ON tagging.url_id == domains.id
    JOIN keyword ON tagging.keyword_id == keyword.id
    GROUP BY domains.domain, keyword.keyword
    """
    select_hyperlinks = """
    SELECT src.domain, dst.domain, count(src.domain)
    FROM hyperlink
    JOIN domains AS src ON hyperlink.source_id == src.id
    JOIN domains AS dst ON hyperlink.target_id == dst.id
    GROUP BY src.domain, dst.domain
    """

    conn.execute('BEGIN EXCLUSIVE')
    conn.execute(temp)

    log.info('Building domain list')
    conn.execute(insert_domains)
    conn.execute(index)

    domains = defaultdict(lambda: {
        'page_count': 0,
        'keywords': defaultdict(int),
        'referrers': defaultdict(int),
    })
    log.info('Counting domains')
    for domain, count in conn.execute(count_domains):
        domains[domain]['page_count'] = count

    log.info('Counting keywords')
    for domain, keyword, count in conn.execute(select_keywords):
        domains[domain]['keywords'][keyword] += count

    log.info('Counting referrers')
    for src, dst, count in conn.execute(select_hyperlinks):
        domains[dst]['referrers'][src] += count

    log.info('Filtering')
    for feed in conn.execute(select_feeds):
        del domains[feed[0]]

    if include:
        domains = {k: v for k, v in domains.items()
                   if filter_by_domains(include)(k)}

    if exclude:
        domains = {k: v for k, v in domains.items()
                   if filter_by_domains(exclude, True)(k)}

    with open(output / fmt, 'w+') as f:
        json.dump(domains, f)

    conn.rollback()
    log.info('Done.')


help_text = """
Export a list of websites that are "uncharted" — websites that were not scraped
as RSS feeds during a crawl, but were recorded in the database because other
feeds mentioned them.

That is, if you are using the cluster spider, this exporter will export the list
of websites that are not crawled due to the spider hitting the depth limit
— the outermost nodes.

Note that websites that are "uncharted" because they were filtered out by the
domain filter (instead of hitting the depth limit) or because they were not
RSS feeds in the first place will also be included here. To only export sites
under a certain domain, use ~+f domain under ...~, to exclude a certain domain,
use ~-f domain under ...~.
"""
