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

import os
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pprint import pformat
from typing import Optional, Union

from scrapy import Spider
from scrapy.exceptions import CloseSpider
from scrapy.http import Request, TextResponse
from scrapy.signals import spider_opened

from ..feedly import FeedlyEntry, build_api_url, get_feed_uri
from ..requests import ProbeFeed, RequestFinished, ResumeRequest
from ..urlkit import build_urls, select_templates
from ..utils import LOG_LISTENER, JSONDict
from ..utils import colored as _
from ..utils import guard_json


class FeedlyRSSSpider(Spider, ABC):
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
    }

    class SpiderConfig:
        OUTPUT = f'./crawl.{datetime.now().strftime("%Y%m%d%H%M%S")}'

        RSS = 'https://xkcd.com/atom.xml'
        RSS_TEMPLATES = {}

        DOWNLOAD_ORDER = 'oldest'
        DOWNLOAD_PER_BATCH = 1000

        ENABLE_SEARCH = False
        ACCESS_TOKEN = None

        STREAM_ID_PREFIX = 'feed/'

        DATABASE_CACHE_SIZE = 100000

    SELECTION_STRATS = {
        'dead': {None: 1, True: 1, False: 0},
        'alive': {None: 1, True: 0, False: 1},
        'dead+': {None: 1, True: 1, False: -128},
        'alive+': {None: 1, True: -128, False: 1},
        'all': {None: 1, True: 1, False: 1},
    }
    LOGSTATS_ITEMS = ['rss/page_count']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider: FeedlyRSSSpider = super().from_crawler(crawler, *args, config=crawler.settings, **kwargs)
        spider.stats = crawler.stats
        crawler.signals.connect(spider.open_spider, spider_opened)
        return spider

    def __init__(self, *, name=None, config, **kwargs):
        super().__init__(name=name, **kwargs)

        output_dir = config['OUTPUT']
        os.makedirs(output_dir, exist_ok=True)

        self.api_base_params = {
            'count': int(config['DOWNLOAD_PER_BATCH']),
            'ranked': config['DOWNLOAD_ORDER'],
            'similar': 'true',
            'unreadOnly': 'false',
        }
        self.config = config
        self.resume_iter = None

    def open_spider(self, spider):
        conf = self.config['SPIDER_CONFIG']
        self.logger.info(f'Spider parameters:\n{pformat(conf.copy_to_dict())}')

    @abstractmethod
    def start_requests(self):
        yield ResumeRequest(callback=self.resume_crawl)

    def resume_crawl(self, response):
        may_resume = False
        meta = response.meta
        freezer = meta.get('freezer', None)
        if freezer is not None:
            requests = freezer.defrost(self)
            try:
                req = next(requests)
            except StopIteration:
                pass
            else:
                may_resume = self.ask_if_resume(freezer)

        if not may_resume:
            feed = self.config['RSS']
            freezer.dump_info({'crawling': feed})
            yield self.probe_feed(feed, meta={'reason': 'user_specified', 'depth': 1})
            return

        self.logger.info(_('Resuming crawl.', color='cyan'))
        self.resume_iter = requests
        yield req

    def ask_if_resume(self, freezer):
        feed = self.config['RSS']
        resume_feed = freezer.load_info().get('crawling')
        if resume_feed != feed:
            self.logger.info(_('Found unfinished crawl job:', color='cyan'))
            self.logger.info(_(f"Continue crawling '{resume_feed}'?", color='cyan'))
            self.logger.info(_(f"Start new crawl with '{feed}'?", color='cyan'))
            self.logger.info(_('Or exit?', color='cyan'))
            action = 'x'
        else:
            action = 'c'

        LOG_LISTENER.stop()
        while action not in 'cse':
            action = input('(continue/start/exit) [c]: ')[:1]
        LOG_LISTENER.start()

        if action == 'e':
            raise CloseSpider()
        elif action == 's':
            freezer.clear()
            freezer.dump_info({'crawling': feed})
            return False
        return True

    def get_streams_url(self, feed_id: str, **params) -> str:
        params = {**self.api_base_params, **params}
        return build_api_url('streams', streamId=feed_id, **params)

    def probe_feed(self, query: str, derive: bool = True, source: Optional[Request] = None, **kwargs):
        templates = self.config['RSS_TEMPLATES']
        if derive and templates:
            try:
                urls = build_urls(query, *select_templates(query, templates))
            except ValueError:
                self.logger.debug(f'No template for {query}')
                urls = [query]
        else:
            urls = [query]

        prefix = self.config['STREAM_ID_PREFIX']
        meta = kwargs.pop('meta', {})
        meta['try_feeds'] = {f'{prefix}{u}': None for u in urls}
        meta['feed_url'] = query
        return ProbeFeed(url=query, callback=self.start_feeds, meta=meta, source=source, **kwargs)

    def start_feeds(self, response: TextResponse):
        meta = response.meta
        yield RequestFinished(meta={**meta})

        feeds = meta.get('valid_feeds')
        if feeds is None:
            feeds = meta.get('try_feeds', {})
        if not len(feeds) and meta['reason'] == 'user_specified':
            self.logger.info(f'No valid RSS feed can be found using `{meta["feed_url"]}` and available feed templates.')
            self.logger.critical('No feed to crawl!')

        yield from self.filter_feeds(feeds, meta)
        yield from self.get_feed_info(feeds, meta)

    def filter_feeds(self, feeds, meta):
        for feed in feeds:
            yield self.next_page({'id': feed}, meta=meta, initial=True)

    def get_feed_info(self, feeds, meta):
        feed_info = meta.get('feed_info', {})
        for feed, info in feed_info.items():
            yield {'source': info, 'dead': feeds.get(feed)}

    def next_page(self, data: JSONDict, response: Optional[TextResponse] = None, initial: bool = False, **kwargs) -> Union[JSONDict, Request]:
        feed = data['id']

        if response:
            meta = {**response.meta}
        else:
            meta = {}
        meta.update(kwargs.pop('meta', {}))
        if not initial:
            meta['no_filter'] = True
            meta.pop('inc_depth', None)

        feed_url = get_feed_uri(feed)
        meta['feed_url'] = feed_url

        meta['pkey'] = (feed_url, 'main')
        meta['_persist'] = 'add'

        params = {}
        cont = data.get('continuation')
        if cont:
            params['continuation'] = cont
            meta['reason'] = 'continuation'
        elif not initial:
            meta['_persist'] = 'remove'
            return RequestFinished(meta=meta)

        depth = meta.get('depth')
        reason = meta.get('reason')
        self.logger.debug(f'initial={initial} depth={depth} reason={reason} {feed}')

        url = self.get_streams_url(feed, **params)
        if response:
            return response.request.replace(url=url, meta=meta, **kwargs)
        else:
            return Request(url, callback=self.parse_feed, meta=meta, **kwargs)

    def parse_feed(self, response: TextResponse):
        if not response:
            return

        data = guard_json(response.text)
        items = data.get('items')
        source = response.meta['feed_url']
        if items:
            response.meta['valid_feed'] = True
            if response.meta.get('reason') != 'continuation':
                self.logger.info(_(f'Got new feed: {source}', color='green'))

        for item in items:
            entry = FeedlyEntry.from_upstream(item)
            if not entry.source:
                entry.source = {'feed': source}
            if not entry:
                continue
            self.stats.inc_value('rss/page_count')

            depth = response.meta.get('depth', 0)

            yield {
                'item': entry,
                'depth': depth,
                'time_crawled': time.time(),
            }

        yield self.next_page(data, response=response)
