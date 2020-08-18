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
from typing import Tuple
from urllib.parse import unquote

import simplejson as json
from scrapy import Spider, signals
from scrapy.exceptions import CloseSpider
from scrapy.http import Request, TextResponse

from .. import feedly
from ..feedly import FeedlyEntry
from ..datastructures import HyperlinkStore
from ..utils import JSONDict, json_converters

log = logging.getLogger('feedly.spider')


class FeedlyRssSpider(Spider):
    name = 'link_aggregator'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'SPIDER_MIDDLEWARES': {
            'scrapy.spidermiddlewares.depth.DepthMiddleware': 100,
            'feedly.middlewares.FeedlyItemMiddleware': 300,
        },
        'ITEM_PIPELINES': {
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

    def __init__(self, name=None, *, output: str, feed: str, ranked='oldest', count=1000, flush_limit=5000, overwrite=False, **kwargs):
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
        self._flush_limit = int(flush_limit)

        self.api_base_params = {
            'count': int(count),
            'ranked': ranked,
            'similar': 'true',
            'unreadOnly': 'false',
        }

    def start_requests(self):
        return [Request(feedly.build_api_url('search', query=self._query), callback=self.start_feed)]

    def start_feed(self, response) -> Tuple[str, str]:
        res = self._guard_json(response.text)

        if not res.get('results'):
            log.critical(f'Cannot find a feed from Feedly using the query `{self._query}`')
            sleep(5)
            return
        results = [feed['feedId'].split('/', 1) for feed in res['results']]
        if len(results) > 1:
            msg = [
                f'Found more than one possible feeds using the query `{self._query}`:',
                *['  ' + feed[1] for feed in results],
                'Please run scrapy again using one of the values above. Crawler will now close.',
            ]
            log.critical('\n'.join(msg))
            sleep(5)
            return

        self.stream_type, self.stream_id = results[0]
        self.index['feed_origin'] = self.stream_id
        self.index['type'] = self.stream_type
        yield Request(self.get_streams_url(), callback=self.parse)

    def get_streams_url(self, **params):
        stream_endpoint = f'{self.stream_type}/{self.stream_id}'
        return feedly.build_api_url('streams', streamId=stream_endpoint, **self.api_base_params)

    def _guard_json(self, text: str) -> JSONDict:
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            log.error(e)

    def parse(self, response: TextResponse):
        res = self._guard_json(response.text)

        for item in res.get('items', []):
            entry = self.save_item(item)
            if entry:
                yield entry

        cont = res.get('continuation')
        if cont:
            yield response.follow(self.get_streams_url(continuation=cont))

    def save_item(self, item):
        try:
            entry = FeedlyEntry.from_upstream(item)
        except Exception as e:
            log.warn(exc_info=e)
            return

        index = self.index
        index['items'][entry.id_hash] = entry
        store: HyperlinkStore = index['resources']
        for k in {'content', 'summary'}:
            content = item.get(k)
            if content:
                content = content.get('content')
            if content:
                store.parse_html(
                    entry.source, content,
                    feedly_id={entry.id_hash},
                    feedly_keyword=entry.keywords,
                )
                entry.markup[k] = content

        visual = item.get('visual')
        if visual:
            u = visual.get('url')
            if u and u != 'none':
                store.put(u, tag={'img'})

        return entry

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
