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
from pathlib import Path
from urllib.parse import SplitResult, quote, unquote

import simplejson as json
from scrapy import Spider, signals
from scrapy.http import Request, TextResponse

from ..items import FeedlyEntry, HyperlinkStore
from ..utils import JSONDict, json_converters

log = logging.getLogger('feedly.spider')


class FeedlyRssSpider(Spider):
    name = 'link_aggregator'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'ITEM_PIPELINES': {
            'feedly.pipelines.FeedlyItemPipeline': 300,
            'feedly.pipelines.SaveIndexPipeline': 900,
        },
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider: FeedlyRssSpider = super().from_crawler(crawler, *args, **kwargs)
        spider.stats = crawler.stats
        spider.stats.set_value('item_milestone', 0)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def __init__(self, name=None, *, output: str, feed=None, stream_type='feed', ranked='oldest', count=1000, flush_limit=5000, **kwargs):
        super().__init__(name=name, **kwargs)

        output = Path(output)
        self.output = output

        if feed:
            feed = unquote(feed)
        index = {
            'type': stream_type,
            'items': {},
            'resources': HyperlinkStore(),
        }
        if output.exists():
            with open(output.resolve(), 'r') as f:
                index = json.load(f)
            existing_feed = index.get('feed_origin')
            if feed and existing_feed != feed:
                raise ValueError(f'Found existing crawl data of a different feed: {existing_feed}')

            index['items'] = {k: FeedlyEntry(**v) for k, v in index['items'].items()}
            index['resources'] = HyperlinkStore(index['resources'])

        self.stream_id = index.setdefault('feed_origin', feed)
        self.stream_type = index.setdefault('type', stream_type)
        if not self.stream_id:
            raise ValueError('No feed URL supplied, and no existing crawl data with a valid feed URL found')

        self.index: JSONDict = index
        self._flush_limit = flush_limit

        self.api_base_url = {
            'scheme': 'https',
            'netloc': 'cloud.feedly.com',
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

        for entry in res.get('items', []):
            yield entry

        cont = res.get('continuation')
        if cont:
            yield response.follow(self.get_streams_url(continuation=cont))

    def _flush(self):
        log.info(f'Saving progress ... got {len(self.index["items"])} items, {len(self.index["resources"])} external links')
        with open(self.output.resolve(), 'w') as f:
            json.dump(
                self.index, f,
                ensure_ascii=False, default=json_converters, for_json=True,
                iterable_as_array=True,
            )

    def spider_closed(self, spider):
        self._flush()
