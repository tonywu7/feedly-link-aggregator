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

from collections.abc import Hashable
from datetime import datetime, timezone
from hashlib import sha1
from typing import Any, Dict, Generic, List, Set, TypeVar, Union
from urllib.parse import urlsplit

from scrapy.http import TextResponse

JSONType = Union[str, bool, int, float, None, List['JSONType'], Dict[str, 'JSONType']]
JSONDict = Dict[str, JSONType]

Keywords = Set[Hashable]
KeywordCollection = Dict[Hashable, Hashable]


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


T = TypeVar('T', bound=Hashable)


class KeywordStore(Generic[T]):
    def __init__(self):
        self._index: Dict[int, T] = {}
        self._taggings: Dict[int, KeywordCollection] = {}

    def getall(self, **kws: Dict[Hashable, Hashable]) -> T:
        for hash_, keywords in self._taggings.items():
            match = True
            for category, keyword in kws.items():
                if keyword not in keywords.get(category, {}):
                    match = False
                    break
            if match:
                yield self._index[hash_]

    def put(self, item: T, **kws: KeywordCollection):
        hash_ = hash(item)
        self._index[hash_] = item
        taggings = self._taggings.setdefault(hash_, {})
        for category, kwset in kws.items():
            keywords = taggings.setdefault(category, set())
            keywords |= kwset

    def __len__(self):
        return len(self._index)
