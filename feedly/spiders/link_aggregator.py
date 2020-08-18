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
from time import sleep
from typing import List, Union
from urllib.parse import unquote

import simplejson as json
from scrapy import Spider
from scrapy.exceptions import CloseSpider
from scrapy.http import Request, TextResponse

from .. import feedly
from ..feedly import FeedlyEntry
from ..datastructures import HyperlinkStore
from ..utils import JSONDict

log = logging.getLogger('feedly.spiders')


def _guard_json(text: str) -> JSONDict:
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        log.error(e)


class FeedlyRssSpider(Spider):
    name = 'link_aggregator'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'SPIDER_MIDDLEWARES': {
            'scrapy.spidermiddlewares.depth.DepthMiddleware': 100,
            'feedly.spiders.link_aggregator.FeedEntryMiddleware': 800,
            'feedly.spiders.link_aggregator.FeedResourceMiddleware': 900,
        },
        'ITEM_PIPELINES': {
            'feedly.pipelines.PeriodicSavePipeline': 900,
        },
    }

    def __init__(self, name=None, *, output: str, feed: str, ranked='oldest', count=1000, flush_watermark=5000, overwrite=False, **kwargs):
        super().__init__(name=name, **kwargs)

        output = Path(output)
        if output.exists() and not overwrite:
            self.logger.error(f'{output} already exists, not overwriting.')
            raise CloseSpider('file_exists')
        self.output = output

        index = {
            'items': {},
            'resources': HyperlinkStore(),
        }
        self.index: JSONDict = index
        self._query = unquote(feed)
        self._flush_watermark = int(flush_watermark)

        self.api_base_params = {
            'count': int(count),
            'ranked': ranked,
            'similar': 'true',
            'unreadOnly': 'false',
        }

    def search_for_feed(self, query, callback, **kwargs):
        return Request(
            feedly.build_api_url('search', query=query),
            callback=self.parse_search_result,
            cb_kwargs={'callback': callback},
            **kwargs,
        )

    def parse_search_result(self, response: TextResponse, *, callback):
        res = _guard_json(response.text)
        if not res.get('results'):
            yield from callback([])
            return
        yield from callback([feed['feedId'].split('/', 1) for feed in res['results']])

    def start_requests(self):
        return [self.search_for_feed(self._query, self.start_feed)]

    def start_feed(self, feed):
        if not feed:
            self.logger.critical(f'Cannot find a feed from Feedly using the query `{self._query}`')
            sleep(5)
            return
        if len(feed) > 1:
            msg = [
                f'Found more than one possible feeds using the query `{self._query}`:',
                *['  ' + f[1] for f in feed],
                'Please run scrapy again using one of the values above. Crawler will now close.',
            ]
            self.logger.critical('\n'.join(msg))
            sleep(5)
            return
        feed = f'{feed[0][0]}/{feed[0][1]}'
        yield from self.next_page({'id': feed}, callback=self.parse)

    def next_page(self, previous, **kwargs):
        feed = previous['id']
        params = {}
        cont = previous.get('continuation')
        if cont:
            params['continuation'] = cont
        yield Request(self.get_streams_url(feed, **params), **kwargs)

    def get_streams_url(self, feed_id, **params):
        return feedly.build_api_url('streams', streamId=feed_id, **self.api_base_params, **params)

    def parse(self, response: TextResponse):
        res = _guard_json(response.text)
        for item in res.get('items', []):
            entry = FeedlyEntry.from_upstream(item)
            if entry:
                yield entry

        yield from self.next_page(res, callback=self.parse)


class FeedEntryMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        m = cls()
        m.stats = crawler.stats
        m.stats.set_value('rss/item_count', 0)
        m.stats.set_value('rss/item_milestone', 0)
        return m

    def process_spider_output(self, response, result, spider):
        for item in result:
            if not isinstance(item, FeedlyEntry):
                yield item
                continue
            self.stats.inc_value('rss/item_count')
            spider.index['items'][item.id_hash] = item


class FeedResourceMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        m = cls()
        m.stats = crawler.stats
        m.stats.set_value('rss/resource_count', 0)
        return m

    def process_spider_output(self, response: TextResponse, result: List[Union[FeedlyEntry, Request]], spider: FeedlyRssSpider):
        store = spider.index['resources']
        for item in result:
            if not isinstance(item, FeedlyEntry):
                yield item
                continue
            item: FeedlyEntry
            for k, v in item.markup.items():
                store.parse_html(
                    item.url, v,
                    feedly_id=item.id_hash,
                    feedly_keyword=item.keywords,
                )
            self.stats.set_value('rss/resource_count', len(store))
            yield item
