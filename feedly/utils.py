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

import logging
import string
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from hashlib import sha1
from logging.handlers import QueueListener
from multiprocessing import Queue
from typing import Any, Dict, List, Set, TypeVar, Union
from urllib.parse import urlsplit

import simplejson as json
from scrapy.http import Request, TextResponse

from .datastructures import KeywordCollection, KeywordStore
from .urlkit import domain_parents, ensure_protocol, is_absolute_http

try:
    from termcolor import colored
except ImportError:
    def colored(t, *args, **kwargs):
        return t

JSONType = Union[str, bool, int, float, None, List['JSONType'], Dict[str, 'JSONType']]
JSONDict = Dict[str, JSONType]
SpiderOutput = List[Union[JSONDict, Request]]

log = logging.getLogger('main.utils')


class QueueListenerWrapper:
    def __init__(self):
        self.queue = None
        self.listener = None

    def enable(self):
        if self.queue:
            return self.queue
        self.queue = Queue()
        self.listener = QueueListener(self.queue, *logging.getLogger().handlers, respect_handler_level=True)
        self.listener.start()
        return self.queue

    def disable(self):
        if not self.queue:
            return
        self.listener.stop()
        self.queue = None
        self.listener = None

    def start(self):
        if not self.listener:
            return
        if not self.listener._thread:
            self.listener.start()
        return self.queue

    def stop(self):
        if not self.listener:
            return
        if self.listener._thread:
            self.listener.stop()
        return self.queue


LOG_LISTENER = QueueListenerWrapper()


def parse_html(domstring, url='about:blank') -> TextResponse:
    return TextResponse(url=url, body=domstring, encoding='utf8')


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
def watch_for_timing(name, limit=0):
    start = time.perf_counter()
    try:
        yield
    finally:
        duration = time.perf_counter() - start
        message = None
        level = None
        if limit and duration > limit:
            message = colored(
                f'[Performance violation] {name} took {duration * 1000:.0f}ms; desired time is {limit * 1000:.0f}ms.',
                color='yellow',
            )
            level = logging.INFO
        elif not limit:
            message = f'{name} took {duration * 1000:.0f}ms'
            level = logging.DEBUG
        if message:
            logging.getLogger('profiler.timing').log(level, message)


def guard_json(text: str) -> JSONDict:
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        log.error(e)
        return {}


def read_jsonlines(f, *, delimiter='\0\n', on_error='raise', paginate=100000, on_paginate=None):
    i = 0
    k = 0
    p = paginate - 1

    next_line = f.readline()
    while next_line:
        i += 1

        if next_line == delimiter:
            k += 1
            next_line = f.readline()
            if paginate and k == p:
                p += paginate
                yield i, k, on_paginate
            continue

        try:
            yield i, k, json.loads(next_line.rstrip())

        except json.JSONDecodeError:
            if on_error == 'raise':
                raise
            elif on_error == 'continue':
                continue
            else:
                raise StopIteration

        next_line = f.readline()


PATH_UNSAFE = ''.join(set(string.punctuation + ' ') - set('-_/.'))


def aggressive_replace_chars(s, encoding='latin_1'):
    return s.encode(encoding, 'replace').decode(encoding, 'ignore')


def replace_unsafe_chars(s, repl='-', chars=PATH_UNSAFE):
    for c in chars:
        if c in s:
            s = s.replace(c, repl)
    return s


def pathsafe(s):
    return replace_unsafe_chars(aggressive_replace_chars(s))


SIMPLEJSON_KWARGS = {
    'ensure_ascii': True,
    'default': json_converters,
    'for_json': True,
    'iterable_as_array': True,
}


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


T = TypeVar('T')


def findpath(start: T, dest: T, segments: Dict[T, Set[T]], path: List[T]) -> bool:
    path.append(start)

    if start not in segments:
        path.pop()
        return False

    next_routes = segments.get(start, set()) - set(path)
    if dest in next_routes:
        path.append(dest)
        return True

    for r in next_routes:
        found = findpath(r, dest, segments, path)
        if found:
            return found

    path.pop()
    return False
