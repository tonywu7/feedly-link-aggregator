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
from urllib.parse import quote, unquote

from ..sql.utils import offset_fetch
from ..utils import pathsafe
from .exporters import MappingCSVExporter, MappingLineExporter
from .utils import build_where_clause, with_db

log = logging.getLogger('exporter.url')


def build_ctes(select):
    urlexpansions = []
    url_tables = ('feed', 'source', 'target')
    url_attrs = ['scheme', 'netloc', 'path', 'query']
    for attr in url_attrs:
        urlexpansions.append(f"""urlsplit(url.url, '{attr}') AS "{attr}" """)
    url_attrs.append('url')
    urlexpansions = ', '.join(urlexpansions)

    dateexpansions = []
    date_attrs = ['year', 'month', 'day', 'hour', 'minute', 'second']
    date_substr = [(1, 4), (6, 2), (9, 2), (12, 2), (15, 2), (18, 2)]
    for attr, range_ in zip(date_attrs, date_substr):
        start, length = range_
        dateexpansions.append(f"""CAST(substr(item.published, {start}, {length}) AS INTEGER) AS "{attr}" """)
    date_attrs.append('date')
    dateexpansions = ', '.join(dateexpansions)

    column_maps = {}
    for table in url_tables:
        for attr in url_attrs:
            column_maps[f'{table}:{attr}'] = f'{table}.{attr}'
    for attr in date_attrs:
        column_maps[f'published:{attr}'] = f'items.{attr}'

    column_maps['tag'] = 'hyperlink.element'
    column_maps['source:title'] = 'items.title'
    column_maps['feed:title'] = 'feed_info.title'
    column_maps['feed:isdead'] = 'feed_info.dead'

    return select % {'urlexpansions': urlexpansions, 'dateexpansions': dateexpansions}, column_maps


CTE = """
WITH urlsplits AS (
    SELECT
        url.id AS id,
        url.url AS url,
        %(urlexpansions)s
    FROM
        url
),
items AS (
    SELECT
        item.url AS url,
        item.source AS source,
        item.title AS title,
        item.author AS author,
        item.published AS date,
        %(dateexpansions)s
    FROM
        item
)
"""

SELECT = """
SELECT
    %(columns)s
FROM
    hyperlink
    JOIN urlsplits AS source ON source.id == hyperlink.source_id
    JOIN urlsplits AS target ON target.id == hyperlink.target_id
    JOIN items ON hyperlink.source_id == items.url
    JOIN feed AS feed_info ON items.source == feed_info.url_id
    JOIN urlsplits AS feed ON items.source == feed.id
"""


@with_db
def export(
    conn: sqlite3.Connection, wd: Path, output: Path, fmt='urls.txt',
    include=None, exclude=None, key=None, format='lines', escape=None,
):
    cte, column_maps = build_ctes(CTE)

    if format == 'lines':
        keys = (key,) if key else ('target:url',)
    else:
        keys = set(key.split(',')) if key else list(column_maps.keys())

    where, values, _ = build_where_clause(include, exclude)

    columns = ', '.join([f'{v} AS "{k}"' for k, v in column_maps.items()])
    column_keys = ', '.join([f'"{k}"' for k in keys])

    select = SELECT % {'columns': columns}
    select = f'{cte}{select} WHERE %(offset)s AND {where} GROUP BY {column_keys}'
    log.debug(select)

    escape_func = {
        'percent': quote,
        'replace': pathsafe,
        'unquote': unquote,
        'unquote-replace': lambda s: pathsafe(unquote(s)),
    }
    escape_func = escape_func.get(escape)

    formatters = {
        'lines': (MappingLineExporter, (keys[0], output, fmt, escape_func)),
        'csv': (MappingCSVExporter, (keys, output, fmt, escape_func)),
    }
    cls, args = formatters[format]

    log.info('Reading database...')
    with cls(*args) as exporter:
        for row in offset_fetch(conn, select, 'hyperlink',
                                values=values, log=log, size=200000):
            exporter.write(row)
    log.info('Done.')


help_text = """
Select and export URLs in various formats.

Synopsis
--------
export ~urls~ -i <input> [**-o** ~name or template~] [[**+f|-f** ~filter~]...]
  [**key=**~attrs...~] [**format=**~lines|csv~]
  [**escape=**~none|percent|replace|unquote|unquote-replace~]

Description
-----------
This exporter lets you select and export URLs found in scraped data.

By default, it exports all URLs found in scraped HTML markups. You can export
other data such as dates or domain names by specifying the **key=** additional
option (see below).

If there already exist some exported data, running this exporter again will
append to existing data.

Options
-------
This exporter supports the following parameters, specified as `key=value` pairs,
in addition to the exporter options:

    ~format=lines|csv~
        Output format. Default is ~lines~.

    ~key=[...]~
        What data to export, specified as one or more comma-separated attribute
          names (see **~Available attributes~**).
        If format is ~lines~, you may only choose one attribute,
          e.g. `key=target:netloc`.
        If format is ~csv~, you may export multiple attributes,
          e.g. `key=source:netloc,tag`
        Default is ~target:url~ for ~lines~, and ~all attributes~ for ~csv~.

    ~escape=none|percent|replace|unquote|unquote-replace~
        Escape filenames. This is useful if you want URL path names to be part
          of the filename.
        ~percent~ will use URL percent-encodings. For example, space characters
          will be encoded as `%20`.
        ~replace~ will aggressively replace all punctuations and characters not
          in the ISO-8859-1 encoding with `-`.
        ~unquote~ is the inverse of ~percent~: replace all percent-encoded
          characters with the original ones.
        ~unquote-replace~ first unquotes the filename, then uses ~replace~ on it
        Default (when unspecified) is ~none~.

    Example
    -------
        `python -m export urls -i input -o out.txt format=csv` \\
            `key=source:netloc,tag`

Output Template
---------------
Instead of specifying a regular path for the **-o/--output** option, you may
also specify a naming template. This allows you to sort URLs to different files
based on some varying attributes such as domain name.

Templates are specified as Python %-format strings with named placeholders e.g.
`%(target:netloc)s.txt`. You can also use any modifier that Python supports,
such as `%(target:url).10s.txt`.

    Examples
    --------
    `export urls ... -o "%(source:netloc)s.txt"`
        Sorts URLs into files named with the domain name of the feed on which
          the URL is found.

    `export urls ... -o "%(target:netloc).6s-%(published:year)s.txt"`
        Name files using domain name the hyperlink is pointing to and the date
          info of scraped feed articles.

    Slashes are also supported:
    `export urls -i data -o "%(feed:title)s/%(tag)s/%(target:netloc)s.csv"`
        will results in a folder hierarchy that may look like:

        `./data/out/`
            `xkcd.com/`
                `img/`
                    `imgs.xkcd.com.csv`
                    `xkcd.com.csv`
                    `...`
                `a/`
                    `itunes.apple.com.csv`
                    `www.barnesandnoble.com.csv`
                    `...`

See **~Available attributes~** for a list of available placeholders.

Filters
-------
You can filter URLs based on URL components such as domain names and protocols,
as well as feed attributes such as names and dates published.

Filters are specified using the **--include/--exclude** options
(shorthands **+f/-f**).

Each filter is a space-separated tuple ~attr predicate value~, where ~attr~ is
one of the **~available attributes~** to test against, ~value~ is the value for
testing, and ~predicate~ is one of the following:

    ~is~
        Equivalent to `==`

    ~gt~, ~ge~, ~lt~, ~le~
        For integer types (such as date values).
        Equivalent to `>`, `>=`, `<`, `<=`.

    ~startswith~, ~endswith~, ~contains~
        For string types.
        Equivalent to `str.startswith`, `str.endswith`, and the `in` operator.

    ~under~
        Only for domain name attributes (~...:netloc~)
        True if the tested value is or is a subdomain of ~value~, and
        False otherwise.

**+f/-f** can be specified multiple times to enable multiple filters. Only URLs
that pass all filters are exported.

    Examples
    --------
    `export urls ... +f source:netloc is xkcd.com`
        Select URLs that are found in markups from xkcd.com

    `export urls ... -f target:netloc is google.com`
        Select URLs that are NOT pointing to google.com

    `export urls ... +f target:path startswith /wp-content`
        Select URLs whose path components begin with "/wp-content".
        Note that URL paths always include the leading / and are %-encoded
          e.g. if you want to specify a path with spaces,
          you will need to use `%20`.

    `export urls ... \\`
    `  +f tag is img \\`
    `  +f source:netloc is staff.tumblr.com \\`
    `  +f target:netloc under media.tumblr.com \\`
    `  +f published:year lt 2017`
        Select image URLs pointing to domains under "media.tumblr.com"
          from posts from "staff.tumblr.com" that are before 2017.

Available attributes
--------------------
Each attribute is in the form of either ~object~ or ~object:key~.

    Objects
    -------
    **URL objects:** ~source~, ~target~, ~feed~

        ~source~ is the URL to the webpage containing the HTML markup. It is
          returned by Feedly.
        ~target~ is the URL found in HTML tags in the ~source~'s markup.
        ~feed~ is the feed URL.
            (That is, ~source~, which is scraped from ~feed~, contains a
              hyperlink that points to ~target~).

        **Keys**: For each kind of URL object, the following keys are available:

            ~url~: The complete URL.
            ~scheme~: The protocol of the URL e.g. `http` and `https`.
            ~netloc~: The domain name of the URL e.g. `example.org`.
            ~path~: The path of the URL, with the beginning slash.
            ~query~: Query string of the URL without `?`, if any
            (These are the attribute names from the
              ~`urllib.parse.urlsplit`~ namedtuple.)

            Example
            -------
            For a ~feed~ `https://xkcd.com/atom.xml`, which has a webpage
            `https://xkcd.com/937/`, which is the ~source~, which contains a
            ~target~ image `https://imgs.xkcd.com/comics/tornadoguard.png`:

            ~target:url~ is `https://imgs.xkcd.com/comics/tornadoguard.png`
            ~target:netloc~ is `imgs.xkcd.com`
            ~source:netloc~ is `xkcd.com`
            ~feed:netloc~ is `xkcd.com`
            ~source:path~ is `/937/`
            ~feed:path~ is `/atom.xml`

    **HTML tag object**: ~tag~

        This is the HTML markup tag on which a ~target~ URL is found.
          It has no keys.
        This could be `a` (HTML anchor tag, clickable link),
          `img` (image tag), `source` (audio/video source tag), etc.

    **Datetime object**: ~published~

        Date and time at which a ~source~ is published. All ~target~s from a
          ~source~ will have the same ~published~ date.
        All dates are in UTC time.

        **Keys**:

            ~date~
                The complete datetime string in the ISO-8601 format, such as
                  `1970-01-01T00:00:00+00:00`.
                Can be used with string operators such as ~startswith~:
                  `published:date startswith 1970-01`.
            ~year~, ~month~, ~day~, ~hour~, ~minute~, ~second~
                Each is an integer. ~hour~ is in 24-hour format.

            Example
            -------
            For the ~source~ `https://xkcd.com/937/` which was published on
              `Fri, 12 Aug 2011 at 04:05:04 GMT`,
            ~published:date~ is `2011-08-12T04:05:04+00:00`
            ~published:year~ is `2011`
            ~published:month~ is `8`.

    **Titles**: ~source:title~, ~feed:title~

        In addition to representing URLs, ~source~ and ~feed~ also has a
        ~:title~ key which is the title of the post and the feed, respectively.

    **Feed status**: ~feed:isdead~

        Whether a feed is dead (unreachable or no longer updated) or alive.
        Value can be `True` (dead), `False` (alive), or `None` (unknown).

"""
