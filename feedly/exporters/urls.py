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

from ..sql.utils import bulk_fetch
from ..utils import pathsafe
from .exporters import MappingCSVExporter, MappingLineExporter
from .utils import build_where_clause, with_db

log = logging.getLogger('url-exporter')


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
        keys = key and set(key.split(','))

    where, values, _ = build_where_clause(include, exclude)

    columns = ', '.join([f'{v} AS "{k}"' for k, v in column_maps.items()])
    column_keys = ', '.join([f'"{k}"' for k in keys])

    select = SELECT % {'columns': columns}
    select = f'{cte}{select} WHERE {where} GROUP BY {column_keys}'
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
        for row in bulk_fetch(conn.execute(select, values), log=log):
            exporter.write(row)
    log.info('Done.')


help_text = """
Select and export URLs in various formats.

Synopsis
--------
export _urls_ -i <input> [**-o** _name or template_] [[**+f|-f** _filter_]...]
  [**key=**_attrs..._] [**format=**_lines|csv_]
  [**escape=**_none|percent|replace|unquote|unquote-replace_]

Description
-----------
This exporter lets you select and export URLs found in scraped data.

By default, it exports all URLs found in scraped HTML markups. This can be
changed by specifying the **key=** additional option (see below).

If there already exist some exported data, running this exporter again will
append to existing data.

Options
-------
This exporter supports the following parameters, specified as `key=value` pairs,
in addition to the exporter options:

    _format=lines|csv_
        Output format. Default is _lines_.

    _key=[...]_
        What data to export, specified as one or more comma-separated attribute
          names (see **_Available attributes_**).
        If format is _lines_, you may only choose one attribute,
          e.g. `key=target:netloc`.
        If format is _csv_, you may export multiple attributes,
          e.g. `key=source:netloc,tag`
        Default is _target:url_ for _lines_, and _all attributes_ for _csv_.

    _escape=none|percent|replace|unquote|unquote-replace_
        Escape filenames. This is useful if you want URL path names to be part
          of the filename.
        _percent_ will use URL percent-encodings. For example, space characters
          will be encoded as `%20`.
        _replace_ will aggressively replace all punctuations and characters not
          in the ISO-8859-1 encoding with `-`.
        _unquote_ is the inverse of _percent_: replace all percent-encoded
          characters with the original ones.
        _unquote-replace_ first unquotes the filename, then uses _replace_ on it
        Default (when unspecified) is _none_.

    Example:
        `python -m export urls -i input -o out.txt format=csv` \\
            `key=source:netloc,tag`

Output Template
---------------
In stead of specifying a regular path for the **-o/--output** option, you may
also specify a naming template. This allows you to sort URLs to different files
based on some varying attributes such as domain name.

Templates are specified as Python %-format strings with named placeholders e.g.
`%(target:netloc)s.txt`. You can also use any modifier Python that supports,
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

See **_Available attributes_** for a list of available placeholders.

Filters
-------
You can filter URLs based on URL components such as domain names and protocols,
as well as feed attributes such as names and dates published.

Filters are specified using the **--include/--exclude** options
(shorthands **+f/-f**).

Each filter is a space-separated tuple _attr predicate value_, where _attr_ is
one of the **_available attributes_** to test against, _value_ is the value for
testing, and _predicate_ is one of the following:

    _is_
        Equivalent to `==`

    _gt_, _ge_, _lt_, _le_
        For integer types (such as date values).
        Equivalent to `>`, `>=`, `<`, `<=`.

    _startswith_, _endswith_, _contains_
        For string types.
        Equivalent to `str.startswith`, `str.endswith`, and the `in` operator.

    _under_
        Only for domain name attributes (_...:netloc_)
        True if the tested value is or is a subdomain of _value_, and
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
Each attribute is in the form of either _object_ or _object:key_.

    Objects
    -------
    **URL objects:** _source_, _target_, _feed_

        _source_ is the URL to the webpage containing the HTML markup. It is
          returned by Feedly.
        _target_ is the URL found in HTML tags in the _source_'s markup.
        _feed_ is the feed URL.
            (That is, _source_, which is scraped from _feed_, contains a
              hyperlink that points to _target_).

        **Keys**: For each kind of URL object, the following keys are available:

            _url_: The complete URL.
            _scheme_: The protocol of the URL e.g. `http` or `https`.
            _netloc_: The domain name of the URL e.g. `example.org`.
            _path_: The path of the URL, with the beginning slash.
            _query_: Query string of the URL without `?`, if any
            (These are the attribute names from the
              _`urllib.parse.urlsplit`_ namedtuple.)

            Example
            -------
            For a _feed_ `https://xkcd.com/atom.xml`, which has a webpage
            `https://xkcd.com/937/`, which is the _source_, which contains a
            _target_ image `https://imgs.xkcd.com/comics/tornadoguard.png`:

            _target:url_ is `https://imgs.xkcd.com/comics/tornadoguard.png`
            _target:netloc_ is `imgs.xkcd.com`
            _source:netloc_ is `xkcd.com`
            _feed:netloc_ is `xkcd.com`
            _source:path_ is `/937/`
            _feed:path_ is `/atom.xml`

    **HTML tag object**: _tag_

        This is the HTML markup tag on which a _target_ URL is found.
          It has no keys.
        This could be `a` (HTML anchor tag, clickable link),
          `img` (image tag), `source` (audio/video source tag), etc.

    **Datetime object**: _published_

        Date and time at which a _source_ is published. All _target_s from a
          _source_ will have the same _published_ date.
        All dates are in UTC time.

        **Keys**:

            _date_
                The complete datetime string in the ISO-8601 format, such as
                  `1970-01-01T00:00:00+00:00`.
                Can be used with string operators such as _startswith_:
                  `published:date startswith 1970-01`.
            _year_, _month_, _day_, _hour_, _minute_, _second_
                Each is an integer. _hour_ is in 24-hour format.

            Example
            -------
            For the _source_ `https://xkcd.com/937/` which was published on
              `Fri, 12 Aug 2011 at 04:05:04 GMT`,
            _published:date_ is `2011-08-12T04:05:04+00:00`
            _published:year_ is `2011`
            _published:month_ is `8`.

    **Titles**: _source:title_, _feed:title_

        In addition to representing URLs, _source_ and _feed_ also has a
        _:title_ key which is the title of the post and the feed, respectively.
"""
