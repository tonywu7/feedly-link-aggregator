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

import time
from collections.abc import MutableSequence, MutableSet, MutableMapping
from datetime import datetime, timezone
from hashlib import sha1
from typing import Any, Dict, List, Tuple, Union
from urllib.parse import SplitResult, urlsplit

from scrapy.http import TextResponse

from .datastructures import KeywordCollection, KeywordStore

JSONType = Union[str, bool, int, float, None, List['JSONType'], Dict[str, 'JSONType']]
JSONDict = Dict[str, JSONType]


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


def json_converters(value: Any) -> JSONType:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(type(value))


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


def domain_parents(domain: str) -> Tuple[str]:
    parts = domain.split('.')
    return tuple('.'.join(parts[-i:]) for i in range(len(parts), 1, -1))


def ensure_collection(supplier):
    def converter(obj):
        if obj is None:
            return supplier()
        return supplier(obj)
    return converter


def path_only(url: SplitResult) -> str:
    return url.geturl()[len(f'{url.scheme}://{url.netloc}'):]


def falsy(v):
    return v in {0, None, False, '0', 'None', 'none', 'False', 'false', 'null', 'undefined', 'NaN'}


def wait(t):
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < t:
        time.sleep(0.1)


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
            html_tags = markup.css(f'[{attrib}]')
            for tag in html_tags:
                url = tag.attrib.get(attrib)
                if not is_absolute_http(url):
                    continue
                url = ensure_protocol(url)

                keywords: KeywordCollection = {
                    'source': {source},
                    'domain': set(domain_parents(urlsplit(url).netloc)),
                    'tag': set(),
                    'id': set(),
                    'class': set(),
                }
                keywords['tag'].add(tag.xpath('name()').get())
                keywords['id'] |= set(tag.xpath('@id').getall())
                keywords['class'] |= set(tag.xpath('@class').getall())
                self.put(url, **keywords, **kwargs)
