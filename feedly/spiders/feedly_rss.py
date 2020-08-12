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

import json
import logging
import os
from pathlib import Path
from urllib.parse import SplitResult, quote

from scrapy import Spider, signals
from scrapy.http import Request, TextResponse

from ..utils import JSONDict

log = logging.getLogger('feedly.spider')


class FeedlyRssSpider(Spider):
    name = 'feedly_rss'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'ITEM_PIPELINES': {
            'feedly.pipelines.FeedlyItemPipeline': 300,
            'feedly.pipelines.FeedlyExternalResourcePipeline': 400,
        },
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider: FeedlyRssSpider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def __init__(self, name=None, *, output: str, feed=None, stream_type='feed', ranked='oldest', count=500, **kwargs):
        super().__init__(name=name, **kwargs)

        output = Path(output)
        os.makedirs(output.resolve(), exist_ok=True)
        self.output = output

        index = self._read_file('index.json')
        index: JSONDict = json.loads(index) if index else {'items': {}}
        self.stream_id = index.setdefault('uri', feed)
        self.stream_type = index.setdefault('type', stream_type)
        if not self.stream_id:
            raise ValueError('No feed URL supplied, and an "index.json" with a valid URL is not found in the output directory.')

        self.index: JSONDict = index

        self.api_base_url = {
            'scheme': 'https',
            'netloc': 'feedly.com',
            'path': '/v3/streams/contents',
            'fragment': '',
        }
        self.api_base_params = {
            'count': count,
            'ranked': ranked,
            'similar': 'true',
            'unreadOnly': 'false',
        }

    def get_streams_url(self, **params):
        stream_endpoint = f'{self.stream_type}/{self.stream_id}'
        url = {**self.api_base_url}
        query = {
            'streamId': stream_endpoint,
            **self.api_base_params,
            **params,
        }
        url['query'] = '&'.join([f'{quote(k)}={quote(str(v))}' for k, v in query.items()])
        return SplitResult(**url).geturl()

    def start_requests(self):
        return [Request(self.get_streams_url(), callback=self.parse)]

    def parse(self, response: TextResponse):
        try:
            res: JSONDict = json.loads(response.text)
        except json.JSONDecodeError as e:
            log.error(e)

        for k in {'direction', 'alternate'}:
            self.index.setdefault(k, res.get(k))

        for entry in res.get('items', []):
            yield entry

        cont = res.get('continuation')
        if cont:
            yield response.follow(self.get_streams_url(continuation=cont))

    def spider_closed(self, spider):
        self._write_file('index.json', json.dumps(self.index, ensure_ascii=False))

    def _read_file(self, path, mode='r'):
        rpath = self.output.joinpath(path)
        if rpath.exists():
            with open(rpath, mode) as f:
                return f.read()
        else:
            self._write_file(path, '')
            return ''

    def _write_file(self, path, content, mode='w'):
        rpath = self.output.joinpath(path).resolve()
        os.makedirs(rpath.parent, exist_ok=True)
        with open(rpath, mode) as f:
            f.write(content)
