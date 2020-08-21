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
import re
from datetime import datetime
from functools import wraps
from pathlib import Path
from pprint import pformat
from typing import Dict, List, Pattern, Union
from urllib.parse import unquote, urlsplit

import simplejson as json
from scrapy import Spider
from scrapy.exceptions import CloseSpider
from scrapy.http import Request, TextResponse
from scrapy.signals import spider_opened

from .. import feedly
from ..config import Config
from ..exceptions import FeedExhausted
from ..feedly import FeedlyEntry
from ..utils import JSONDict, HyperlinkStore, falsy, no_scheme, path_only, wait

log = logging.getLogger('feedly.spiders')


def _guard_json(text: str) -> JSONDict:
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        log.error(e)


def with_authorization(func):
    @wraps(func)
    def add_header(self: FeedlyRSSSpider, *args, **kwargs):
        g = func(self, *args, **kwargs)
        if self.token:
            for req in g:
                if isinstance(req, Request):
                    req.headers['Authorization'] = f'OAuth {self.token}'
                yield req
        else:
            yield from g
    return add_header


class FeedlyRSSSpider(Spider):
    name = 'feed_content'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'SPIDER_MIDDLEWARES': {
            'scrapy.spidermiddlewares.depth.DepthMiddleware': None,
            'feedly.middlewares.ConditionalDepthMiddleware': 100,
            'feedly.middlewares.FeedCompletionMiddleware': 500,
            'feedly.spiders.rss_spider.FeedResourceMiddleware': 800,
            'feedly.spiders.rss_spider.FeedEntryMiddleware': 900,
        },
    }

    class SpiderConfig:
        OUTPUT = f'./{datetime.now().strftime("%Y%m%d%H%M%S")}.crawl.json'
        OVERWRITE = False

        FEED = 'https://xkcd.com/atom.xml'
        FEED_TEMPLATES = {
            r'.*': {
                '%(original)s': 999,
            },
        }

        DOWNLOAD_ORDER = 'oldest'
        DOWNLOAD_PER_BATCH = 1000

        FUZZY_SEARCH = False
        ACCESS_TOKEN = None

        STREAM_ID_PREFIX = 'feed/'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
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

        output = Path(config['OUTPUT'])
        if output.exists() and falsy(config['OVERWRITE']):
            self.logger.error(f'{output} already exists, not overwriting.')
            raise CloseSpider('file_exists')
        self.output = config['OUTPUT'] = output

        index = {
            'items': {},
            'resources': HyperlinkStore(),
        }
        self.index: JSONDict = index

        self.query = config['FEED'] = unquote(config['FEED'])
        self.fuzzy = config['FUZZY_SEARCH']
        self.token = config['ACCESS_TOKEN']

        templates = {re.compile(k): v for k, v in config['FEED_TEMPLATES'].items()}
        config['FEED_TEMPLATES'] = templates
        self.url_templates: Dict[Pattern[str], JSONDict] = templates

        self.statspipeline_config = {
            'logstats': {
                'rss/page_count': 100,
                'rss/resource_count': 500,
            },
            'autosave': 'rss/page_count',
        }

        self.api_base_params = {
            'count': int(config['DOWNLOAD_PER_BATCH']),
            'ranked': config['DOWNLOAD_ORDER'],
            'similar': 'true',
            'unreadOnly': 'false',
        }
        self.config = config

    def open_spider(self, spider):
        self.logger.info(f'Spider parameters:\n{pformat(self.config)}')

    def start_requests(self):
        return self.try_feeds(
            self.query,
            prefix=self.config['STREAM_ID_PREFIX'],
            search_callback=self.single_feed_only_with_prompt,
            meta={'reason': 'user_specified'},
        )

    def get_streams_url(self, feed_id, **params):
        return feedly.build_api_url('streams', streamId=feed_id, **self.api_base_params, **params)

    @with_authorization
    def try_feeds(self, query, *, prefix='feed/', search_callback, search_kwargs=None, **kwargs):
        effective_templates = {k: v for k, v in self.url_templates.items() if k.match(query)}
        query_parsed = urlsplit(query)
        specifiers = {
            **query_parsed._asdict(),
            'network_path': no_scheme(query_parsed),
            'path_query': path_only(query_parsed),
            'original': query_parsed.geturl(),
        }
        urls = {t % specifiers: None for _, t in sorted(
            (priority, tmpl)
            for pattern, tmpls in effective_templates.items()
            for tmpl, priority in tmpls.items()
        )}

        for u in urls:
            yield from self.next_page(
                {'id': f'{prefix}{u}'}, callback=self.parse_feed,
                meta={
                    **kwargs.pop('meta', {}),
                    'inc_depth': True,
                    'feed_url': u,
                    'feed_query': query,
                    'feed_candidates': urls,
                    'search_callback': search_callback,
                    'search_kwargs': search_kwargs or {},
                },
                initial=True,
                **kwargs,
            )

    def parse_search_result(self, response: TextResponse, *, callback, **kwargs):
        res = _guard_json(response.text)
        if not res.get('results'):
            yield from callback([], **kwargs)
            return
        yield from callback([feed['feedId'] for feed in res['results']], **kwargs)

    def single_feed_only_with_prompt(self, feed, **kwargs):
        if not feed:
            self.logger.critical(f'Cannot find a feed from Feedly using the query `{self.query}`')
            wait(5)
            return
        if len(feed) > 1:
            msg = [
                f'Found more than one possible feeds using the query `{self.query}`:',
                *['  ' + f[1] for f in feed],
                'Please run scrapy again using one of the values above. Crawler will now close.',
            ]
            self.logger.critical('\n'.join(msg))
            wait(5)
            return
        feed = feed[0]
        self.logger.info(f'Loading from {feed}')
        yield from self.next_page({'id': feed}, callback=self.parse_feed, initial=True, **kwargs)

    @with_authorization
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

        params = {}
        cont = data.get('continuation')
        if cont:
            params['continuation'] = cont
            meta['reason'] = 'continuation'
        elif not initial:
            raise FeedExhausted(response)

        url = self.get_streams_url(feed, **params)
        if response:
            yield response.request.replace(url=url, meta=meta, **kwargs)
            return
        yield Request(url, meta=meta, **kwargs)

    def parse_feed(self, response: TextResponse):
        data = _guard_json(response.text)
        items = data.get('items')
        if items:
            yield {'valid_feed': response}
            if response.meta.get('reason') != 'continuation':
                self.logger.info(f'Got new RSS feed at {response.meta["feed_url"]}')

        for item in items:
            entry = FeedlyEntry.from_upstream(item)
            if entry:
                yield entry
        yield from self.next_page(data, callback=self.parse_feed, response=response)

    def parse(self, response, **kwargs):
        return


class FeedEntryMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        m = cls()
        m.stats = crawler.stats
        return m

    def process_spider_output(self, response, result, spider):
        for item in result:
            if isinstance(item, FeedlyEntry):
                self.stats.inc_value('rss/page_count')
                spider.index['items'][item.id_hash] = item
            yield item


class FeedResourceMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        m = cls()
        m.stats = crawler.stats
        return m

    def process_spider_output(self, response: TextResponse, result: List[Union[FeedlyEntry, Request]], spider: FeedlyRSSSpider):
        store = spider.index['resources']
        for item in result:
            if isinstance(item, FeedlyEntry):
                item: FeedlyEntry
                for k, v in item.markup.items():
                    store.parse_html(
                        item.url, v,
                        feedly_id=item.id_hash,
                        feedly_keyword=item.keywords,
                    )
                self.stats.set_value('rss/resource_count', len(store))
            yield item
