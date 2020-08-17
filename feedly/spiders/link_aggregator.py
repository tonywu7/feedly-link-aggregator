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
from typing import Tuple
from urllib.parse import SplitResult, quote, unquote

import simplejson as json
from scrapy import Spider, signals
from scrapy.exceptions import CloseSpider
from scrapy.http import Request, TextResponse

from ..items import HyperlinkStore
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

    API_BASE = {
        'scheme': 'https',
        'netloc': 'cloud.feedly.com',
        'fragment': '',
    }
    API_ENDPOINTS = {
        'streams': '/v3/streams/contents',
        'search': '/v3/search/feeds',
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider: FeedlyRssSpider = super().from_crawler(crawler, *args, **kwargs)
        spider.stats = crawler.stats
        spider.stats.set_value('item_milestone', 0)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def __init__(self, name=None, *, output: str, feed, ranked='oldest', count=1000, flush_limit=5000, overwrite=False, **kwargs):
        super().__init__(name=name, **kwargs)

        output = Path(output)
        if output.exists() and not overwrite:
            log.error(f'{output} already exists, not overwriting.')
            raise CloseSpider('file_exists')
        self.output = output

        index = {
            'items': {},
            'resources': HyperlinkStore(),
        }

        self._query = unquote(feed)

        self.index: JSONDict = index
        self._flush_limit = flush_limit

        self.api_base_params = {
            'count': count,
            'ranked': ranked,
            'similar': 'true',
            'unreadOnly': 'false',
        }

    def start_requests(self):
        return [Request(self.build_api_call('search', query=self._query), callback=self.set_feed_id)]

    def set_feed_id(self, response) -> Tuple[str, str]:
        res = self._guard_json(response.text)

        if not res.get('results'):
            log.critical(f'Cannot find a feed from Feedly using the query `{self._query}`')
            return
        results = [feed['feedId'].split('/', 1) for feed in res['results']]
        if len(results) > 1:
            msg = [
                f'Found more than one possible feeds using the query `{self._query}`:',
                *['  ' + feed[1] for feed in results],
                'Please run scrapy again using one of the values above.',
            ]
            log.critical('\n'.join(msg))
            return

        self.stream_type, self.stream_id = results[0]
        self.index['feed_origin'] = self.stream_id
        self.index['type'] = self.stream_type
        yield Request(self.get_streams_url(), callback=self.parse)

    def build_api_call(self, endpoint, **params):
        if endpoint not in self.API_ENDPOINTS:
            raise ValueError(f'{endpoint} API is not supported')
        url = {**self.API_BASE, 'path': self.API_ENDPOINTS[endpoint]}
        url['query'] = '&'.join([f'{quote(k)}={quote(str(v))}' for k, v in params.items()])
        return SplitResult(**url).geturl()

    def get_streams_url(self, **params):
        stream_endpoint = f'{self.stream_type}/{self.stream_id}'
        return self.build_api_call('streams', streamId=stream_endpoint, **self.api_base_params)

    def _guard_json(self, text: str) -> JSONDict:
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            log.error(e)

    def parse(self, response: TextResponse):
        res = self._guard_json(response.text)

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
