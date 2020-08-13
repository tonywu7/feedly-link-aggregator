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
from urllib.parse import urlsplit

import attr
from attr.converters import optional

from . import utils
from .utils import JSONDict, Keywords, KeywordCollection


def ensure_collection(supplier):
    def converter(obj):
        if obj is None:
            return supplier()
        return supplier(obj)
    return converter


@attr.s(kw_only=True, frozen=True)
class FeedlyEntry:
    _id: str = attr.ib(default=None)
    id_hash: str = attr.ib(default=attr.Factory(lambda s: s._id and utils.sha1sum(s._id), takes_self=True))

    source: str = attr.ib()
    published: datetime = attr.ib(converter=utils.datetime_converters)
    updated: datetime = attr.ib(default=None, converter=optional(utils.datetime_converters))

    keywords: Keywords = attr.ib(converter=ensure_collection(set), factory=set)
    author: Optional[str] = attr.ib(default=None)

    markup: Dict[str, str] = attr.ib(factory=dict)

    @classmethod
    def from_upstream(cls, item: JSONDict) -> FeedlyEntry:
        data = {}
        for name, attrib in attr.fields_dict(cls).items():
            value = item.get(name)
            if value:
                data[name] = value
        data['id'] = item['id']
        data['source'] = item.get('originId')
        entry = cls(**data)

        return entry

    @staticmethod
    def _filter_attrib(attrib: attr.Attribute, value: Any) -> bool:
        return attrib.name[0] != '_'

    def for_json(self) -> JSONDict:
        dict_ = attr.asdict(self, filter=self._filter_attrib)
        return dict_


class HyperlinkStore(utils.KeywordStore):
    TARGET_ATTRS = {'src', 'href', 'data-src', 'data-href'}

    def __init__(self, serialized: JSONDict = None):
        super().__init__()
        if serialized:
            self._deserialize(serialized)

    def _deserialize(self, dict_: JSONDict):
        for k, v in dict_.items():
            hash_ = hash(k)
            self._index[hash_] = k
            self._taggings[hash_] = {c: set(ls) for c, ls in v.items()}

    def parse_html(self, source, markup):
        markup = utils.parse_html(markup)
        for attrib in self.TARGET_ATTRS:
            html_tags = markup.css(f'[{attrib}]')
            for tag in html_tags:
                url = tag.attrib.get(attrib)
                if not utils.is_absolute_http(url):
                    continue
                url = utils.ensure_protocol(url)

                keywords: KeywordCollection = {
                    'source': {source},
                    'domain': {urlsplit(url).netloc},
                    'tag': set(),
                    'id': set(),
                    'class': set(),
                }
                keywords['tag'].add(tag.xpath('name()').get())
                keywords['id'] |= set(tag.xpath('@id').getall())
                keywords['class'] |= set(tag.xpath('@class').getall())
                self.put(url, **keywords)

    def for_json(self):
        return {item: self._taggings[hash_] for hash_, item in self._index.items()}
