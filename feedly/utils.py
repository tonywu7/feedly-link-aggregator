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

from __future__ import annotations

import click
import logging
import re
import time
from collections.abc import MutableMapping, MutableSequence, MutableSet
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import wraps
from hashlib import sha1
from textwrap import dedent, indent
from typing import Any, Dict, List, Tuple, Union
from urllib.parse import SplitResult, urlsplit

import simplejson as json
from scrapy.http import TextResponse

from .datastructures import KeywordCollection, KeywordStore

JSONType = Union[str, bool, int, float, None, List['JSONType'], Dict[str, 'JSONType']]
JSONDict = Dict[str, JSONType]

log = logging.getLogger('feedly.utils')


def parse_html(domstring, url='about:blank') -> TextResponse:
    return TextResponse(url=url, body=domstring, encoding='utf8')


def is_http(u):
    return isinstance(u, str) and urlsplit(u).scheme in {'http', 'https'}


def is_absolute_http(u):
    if not isinstance(u, str):
        return False
    s = urlsplit(u)
    return s.scheme in {'http', 'https'} or s.scheme == '' and s.netloc


def ensure_protocol(u, protocol='http'):
    s = urlsplit(u)
    return u if s.scheme else f'{protocol}:{u}'


def domain_parents(domain: str) -> Tuple[str]:
    parts = domain.split('.')
    return tuple('.'.join(parts[-i:]) for i in range(len(parts), 1, -1))


def no_scheme(url: SplitResult) -> str:
    return url.geturl()[len(f'{url.scheme}:'):]


def path_only(url: SplitResult) -> str:
    return url.geturl()[len(f'{url.scheme}://{url.netloc}'):]


def json_converters(value: Any) -> JSONType:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(type(value))


def load_jsonlines(file) -> List[JSONDict]:
    return [json.loads(line) for line in file.read().split('\n') if line]


def datetime_converters(dt: Union[str, int, float, datetime], tz=timezone.utc) -> datetime:
    if isinstance(dt, datetime):
        return dt
    if isinstance(dt, str):
        return datetime.fromisoformat(dt)
    if isinstance(dt, (int, float)):
        try:
            return datetime.fromtimestamp(dt, tz=tz)
        except ValueError:
            return datetime.fromtimestamp(dt / 1000, tz=tz)
    raise TypeError('dt must be of type str, int, float, or datetime')


def sha1sum(s: Union[str, bytes]) -> str:
    if isinstance(s, str):
        s = s.encode()
    return sha1(s).hexdigest()


def ensure_collection(supplier):
    def converter(obj):
        if obj is None:
            return supplier()
        return supplier(obj)
    return converter


def falsy(v):
    return v in {0, None, False, '0', 'None', 'none', 'False', 'false', 'null', 'undefined', 'NaN'}


def wait(t):
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < t:
        time.sleep(0.1)


@contextmanager
def watch_for_timing(name, limit):
    start = time.perf_counter()
    try:
        yield
    finally:
        duration = time.perf_counter() - start
        if duration > limit:
            logging.getLogger('profiler.timing').warn(f'[Timing violation] {name} took {duration * 1000:.0f}ms; desired time is {limit * 1000:.0f}ms.')


def guard_json(text: str) -> JSONDict:
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        log.error(e)
        return {}


def build_urls(base, templates):
    parsed = urlsplit(base)
    specifiers = {
        **parsed._asdict(),
        'network_path': no_scheme(parsed),
        'path_query': path_only(parsed),
        'original': parsed.geturl(),
    }
    return [t % specifiers for t in templates]


def compose_mappings(*mappings):
    base = {}
    base.update(mappings[0])
    for m in mappings[1:]:
        for k, v in m.items():
            if k in base and type(base[k]) is type(v):
                if isinstance(v, MutableMapping):
                    base[k] = compose_mappings(base[k], v)
                elif isinstance(v, MutableSet):
                    base[k] |= v
                elif isinstance(v, MutableSequence):
                    base[k].extend(v)
                else:
                    base[k] = v
            else:
                base[k] = v
    return base


class HyperlinkStore(KeywordStore):
    TARGET_ATTRS = {'src', 'href', 'data-src', 'data-href'}

    def __init__(self, serialized: JSONDict = None):
        super().__init__()
        self._index: Dict[int, str]
        if serialized:
            self._deserialize(serialized)

    def _deserialize(self, dict_: JSONDict):
        for k, v in dict_.items():
            hash_ = hash(k)
            self._index[hash_] = k
            self._taggings[hash_] = {c: set(ls) for c, ls in v.items()}

    def parse_html(self, source, markup, **kwargs):
        markup = parse_html(markup)
        for attrib in self.TARGET_ATTRS:
            elements = markup.css(f'[{attrib}]')
            for tag in elements:
                url = tag.attrib.get(attrib)
                if not is_absolute_http(url):
                    continue
                url = ensure_protocol(url)

                keywords: KeywordCollection = {
                    'source': {source},
                    'domain': set(domain_parents(urlsplit(url).netloc)),
                    'tag': set(),
                }
                keywords['tag'].add(tag.xpath('name()').get())
                self.put(url, **keywords, **kwargs)


SIMPLEJSON_KWARGS = {
    'ensure_ascii': True,
    'default': json_converters,
    'for_json': True,
    'iterable_as_array': True,
}


def stylize(pattern, **styles):
    def wrapper(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            for s in func(*args, **kwargs):
                yield re.sub(pattern, lambda m: click.style(m.group(1), **styles), s)
        return wrapped
    return wrapper


def markdown_inline(func):
    @stylize(re.compile(r'`(.*?)`'), fg='green')
    @stylize(re.compile(r'_(.*?)_'), fg='blue', underline=True)
    @stylize(re.compile(r'\*\*(.*?)\*\*'), fg='yellow', bold=True)
    def f(*args, **kwargs):
        yield from func(*args, **kwargs)
    return f


@markdown_inline
def numpydoc2click(doc: str):
    PARA = re.compile(r'((?:.+\n)+)')
    PARA_WITH_HEADER = re.compile(r'(^ *)(.+)\n(?:\s*(?:-+|=+))\n((?:.+\n)+)')
    paragraphs = list(reversed(PARA.findall(dedent(doc))))
    yield paragraphs.pop()
    while paragraphs:
        p = paragraphs.pop()
        match = PARA_WITH_HEADER.match(p)
        if match:
            indentation, header, p = match.group(1), match.group(2), match.group(3)
            if not indentation:
                header = header.upper()
            yield indent(click.style(header, bold=True), indentation)
            yield '\n'
            yield indent(p, '    ')
        else:
            yield indent(p, '    ')
        yield '\n'


get_help_gen = markdown_inline(lambda ctx: (yield ctx.get_help()))


def get_help(ctx):
    return next(get_help_gen(ctx))
