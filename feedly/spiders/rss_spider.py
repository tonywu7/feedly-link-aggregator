# MIT License
#
# Copyright (c) 2020 # MIT License
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
import os
import re
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from pprint import pformat
from typing import List
from urllib.parse import unquote, urlsplit

import simplejson as json
from scrapy import Spider
from scrapy.exceptions import IgnoreRequest
from scrapy.http import TextResponse
from scrapy.signals import spider_opened
from scrapy_promise import Promise, fetch
from twisted.python.failure import Failure

from .. import feedly, utils
from ..config import Config
from ..exceptions import FeedExhausted
from ..feedly import FeedlyEntry
from ..utils import HyperlinkStore, JSONDict

log = logging.getLogger('feedly.spiders')


def _guard_json(text: str) -> JSONDict:
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        log.error(e)


class FeedlyRSSSpider(Spider, ABC):
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'SPIDER_MIDDLEWARES': {
            'scrapy.spidermiddlewares.depth.DepthMiddleware': None,
            'feedly.middlewares.ConditionalDepthSpiderMiddleware': 100,
        },
    }

    class SpiderConfig:
        OUTPUT = f'./crawl.{datetime.now().strftime("%Y%m%d%H%M%S")}'

        FEED = 'https://xkcd.com/atom.xml'
        FEED_TEMPLATES = {
            r'.*': {
                '%(original)s': 999}}

        DOWNLOAD_ORDER = 'oldest'
        DOWNLOAD_PER_BATCH = 1000

        FUZZY_SEARCH = False
        ACCESS_TOKEN = None

        STREAM_ID_PREFIX = 'feed/'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.stats = crawler.stats
        crawler.signals.connect(spider.open_spider, spider_opened)
        return spider

    def __init__(self, name=None, profile=None, **kwargs):
        super().__init__(name=name, **kwargs)

        kwargs = {k.upper(): v for k, v in kwargs.items()}
        config = Config()
        config.from_object(self.SpiderConfig)
        if profile:
            config.from_pyfile(profile)
        config.merge(kwargs)

        output_dir = Path(config['OUTPUT'])
        config['OUTPUT'] = output_dir
        os.makedirs(output_dir, exist_ok=True)

        config.set('FEED', unquote(config.get('FEED')))

        templates = {re.compile(k): v for k, v in config['FEED_TEMPLATES'].items()}
        config['FEED_TEMPLATES'] = templates

        index = {
            'items': {},
            'resources': HyperlinkStore(),
        }
        self.index: JSONDict = index

        self.api_base_params = {
            'count': int(config['DOWNLOAD_PER_BATCH']),
            'ranked': config['DOWNLOAD_ORDER'],
            'similar': 'true',
            'unreadOnly': 'false',
        }
        self.config = config

        self.logstats_items = ['rss/page_count']

    def open_spider(self, spider):
        self.logger.info(f'Spider parameters:\n{pformat(self.config.copy_to_dict())}')

    @abstractmethod
    def start_requests(self):
        query = self.config['FEED']
        return self.start_feed(query, meta={'reason': 'user_specified', 'depth': 1})

    def get_streams_url(self, feed_id, **params):
        return feedly.build_api_url('streams', streamId=feed_id, **self.api_base_params, **params)

    def start_feed(self, query, **kwargs):
        prefix = self.config['STREAM_ID_PREFIX']
        url_templates = self.config['FEED_TEMPLATES']

        effective_templates = {k: v for k, v in url_templates.items() if k.match(query)}
        query_parsed = urlsplit(query)
        specifiers = {
            **query_parsed._asdict(),
            'network_path': utils.no_scheme(query_parsed),
            'path_query': utils.path_only(query_parsed),
            'original': query_parsed.geturl(),
        }
        urls = {t % specifiers: None for _, t in sorted(
            (priority, tmpl)
            for pattern, tmpls in effective_templates.items()
            for tmpl, priority in tmpls.items()
        )}

        meta = kwargs.pop('meta', {})
        meta = {**meta, 'inc_depth': True}

        token = self.config.get('ACCESS_TOKEN')
        if token:
            meta['auth'] = token

        starting_pages = [self.next_page({'id': f'{prefix}{u}'},
                                         meta={**meta, 'query': query},
                                         initial=True, **kwargs)
                          for u in urls]
        return Promise.all(*starting_pages)

    def start_search(self, query, **kwargs):
        def search(responses: List[TextResponse]):
            empty_feeds = [p['empty_feed'] for p in responses if 'empty_feed' in p]
            if len(empty_feeds) != len(responses):
                return
            self.logger.info(f'No valid RSS feed can be found using `{query}` and available feed templates.')

            if self.config.getbool('FUZZY_SEARCH'):
                self.logger.info(f'Searching Feedly for {query}')
                meta = {**empty_feeds[0].meta}
                meta.update(kwargs.pop('meta', {}))
                meta['reason'] = 'search'

                return fetch(
                    feedly.build_api_url('search', query=query),
                    priority=-1,
                    meta=meta,
                    **kwargs,
                ).then(self.parse_search_result)
        return search

    def next_page(self, data, response: TextResponse = None, initial=False, **kwargs):
        feed = data['id']

        if response:
            meta = {**response.meta}
        else:
            meta = {}
        meta.update(kwargs.pop('meta', {}))
        if not initial:
            meta['no_filter'] = True
            meta.pop('inc_depth', None)
        meta['feed_url'] = feedly.get_feed_uri(feed)

        params = {}
        cont = data.get('continuation')
        if cont:
            params['continuation'] = cont
            meta['reason'] = 'continuation'
        elif not initial:
            raise FeedExhausted(response)

        self.logger.debug(f'initial={initial} depth={meta.get("depth")} reason={meta["reason"]} {feed}')
        url = self.get_streams_url(feed, **params)
        if response:
            request = fetch(url, base=response.request, meta=meta, **kwargs)
        else:
            request = fetch(url, meta=meta, **kwargs)
        return request.then(self.parse_feed).catch(self.close_feed)

    def parse_feed(self, response: TextResponse):
        if not response:
            return

        data = _guard_json(response.text)
        items = data.get('items')
        if items:
            response.meta['valid_feed'] = True
            if response.meta.get('reason') != 'continuation':
                self.logger.info(f'Got new RSS feed at {response.meta["feed_url"]}')

        for item in items:
            entry = FeedlyEntry.from_upstream(item)
            if not entry:
                continue
            self.stats.inc_value('rss/page_count')

            depth = response.meta.get('depth', 0)
            store = utils.HyperlinkStore()
            for k, v in entry.markup.items():
                store.parse_html(
                    entry.url, v,
                    keyword={entry.id_hash},
                    feedly_id=entry.keywords,
                )

            yield {
                'entry': entry,
                'urls': store,
                'depth': depth,
                'time_crawled': time.time(),
            }

        return self.next_page(data, response=response)

    def close_feed(self, exc: FeedExhausted):
        if isinstance(exc, Failure):
            if isinstance(exc.value, IgnoreRequest):
                return {}
        if isinstance(exc, FeedExhausted):
            response = exc.response
            if response and response.meta.get('valid_feed'):
                return {}
            self.logger.debug(f'Empty feed {response.meta.get("feed_url")}')
            return {'empty_feed': response}
        raise exc

    def log_exception(self, exc: Failure):
        if isinstance(exc, Failure):
            exc = exc.value
        self.logger.error(exc, exc_info=True)

    def parse_search_result(self, response: TextResponse):
        if not response:
            return

        res = _guard_json(response.text)
        if not res.get('results'):
            return response, []
        return response, [feed['feedId'] for feed in res['results']]

    def digest_feed_export(self, stream) -> JSONDict:
        self.logger.info('Digesting crawled data, this may take a while...')
