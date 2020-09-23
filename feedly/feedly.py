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

from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import SplitResult, quote, urlsplit

import attr
from attr.converters import optional
from attr.validators import instance_of

from . import utils
from .datastructures import Keywords
from .utils import HyperlinkStore, JSONDict

API_BASE = {
    'scheme': 'https',
    'netloc': 'cloud.feedly.com',
    'fragment': '',
}
API_ENDPOINTS = {
    'streams': '/v3/streams/contents',
    'search': '/v3/search/feeds',
    'feeds': '/v3/feeds/',
}


def build_api_url(endpoint, argument='', **params):
    if endpoint not in API_ENDPOINTS:
        raise ValueError(f'{endpoint} API is not supported')
    url = {**API_BASE, 'path': API_ENDPOINTS[endpoint] + quote(argument, safe='')}
    url['query'] = '&'.join([f'{quote(k)}={quote(str(v))}' for k, v in params.items()])
    return SplitResult(**url).geturl()


def get_feed_uri(s):
    return s.split('/', 1)[1]


def lowercase_set(iterable=None):
    if not iterable:
        return set()
    return {str(k).lower() for k in iterable}


@attr.s(kw_only=True, frozen=True)
class FeedlyEntry:
    url: str = attr.ib(validator=instance_of(str))
    source: str = attr.ib(repr=False)
    published: datetime = attr.ib(converter=utils.datetime_converters)
    updated: datetime = attr.ib(default=None, converter=optional(utils.datetime_converters), repr=False)

    keywords: Keywords = attr.ib(converter=utils.ensure_collection(lowercase_set), factory=lowercase_set, repr=False)
    author: Optional[str] = attr.ib(default='', repr=False)
    title: Optional[str] = attr.ib(default='', repr=False)

    markup: Dict[str, str] = attr.ib(factory=dict, repr=False)
    hyperlinks: HyperlinkStore = attr.ib(factory=HyperlinkStore, repr=False)

    @classmethod
    def from_upstream(cls, item: JSONDict) -> FeedlyEntry:
        data = {}
        for name, attrib in attr.fields_dict(cls).items():
            value = item.get(name)
            if value:
                data[name] = value
        data['url'] = cls._get_page_url(item)
        data['source'] = cls._get_source_url(item)
        entry = cls(**data)
        cls._set_markup(entry, item)
        return entry

    @staticmethod
    def _get_page_url(item):
        url = urlsplit(item.get('originId', ''))
        if url.netloc:
            url = url.geturl()
        else:
            url = ''
            alt = item.get('alternate')
            if alt and alt != 'none':
                url = alt[0]['href']
        return url

    @staticmethod
    def _get_source_url(item):
        source = item.get('origin')
        if source:
            return get_feed_uri(source.get('streamId', '/'))
        else:
            return ''

    @staticmethod
    def _set_markup(entry, item):
        content = item.get('content', item.get('summary'))
        if content:
            content = content.get('content')
        if content:
            entry.add_markup('summary', content)

    @staticmethod
    def _filter_attrib(attrib: attr.Attribute, value: Any) -> bool:
        return attrib.name[0] != '_'

    def add_markup(self, name, markup):
        self.markup[name] = markup
        self.hyperlinks.parse_html(self.url, markup)

    def for_json(self) -> JSONDict:
        dict_ = attr.asdict(self, filter=self._filter_attrib)
        return dict_
