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
from datetime import datetime, timezone
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
from ..exceptions import FeedExhausted
from ..feedly import FeedlyEntry
from ..utils import JSONDict, HyperlinkStore, falsy, compose_mappings, wait

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

    DEFAULT_CONFIG = {
        'output': f'{datetime.now(tz=timezone.utc).isoformat()}.crawl.json',
        'feed': 'https://xkcd.com/atom.xml',
        'ranked': 'oldest',
        'count': 1000,
        'overwrite': False,
        'token': None,
        'fuzzy': False,
        'templates': {
            '.*': {
                '%(original)s': 999,
            },
        },
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.open_spider, spider_opened)
        return spider

    def __init__(self, name=None, profile=None, **kwargs):
        super().__init__(name=name, **kwargs)

        if profile:
            with open(profile, 'r') as f:
                profile: JSONDict = json.load(f)
        else:
            profile = {}
        config = compose_mappings(self.DEFAULT_CONFIG, profile, kwargs)

        output = Path(config['output'])
        if output.exists() and falsy(config['overwrite']):
            self.logger.error(f'{output} already exists, not overwriting.')
            raise CloseSpider('file_exists')
        self.output = output

        index = {
            'items': {},
            'resources': HyperlinkStore(),
        }
        self.index: JSONDict = index

        self._query = unquote(config['feed'])

        t = config.get('templates', '{}')
        if isinstance(t, str):
            config['templates'] = json.loads(t)
        self._url_templates: Dict[Pattern[str], JSONDict] = {re.compile(k): v for k, v in config['templates'].items()}

        self._fuzzy = config.get('fuzzy')

        self._statspipeline_config = {
            'logstats': {
                'rss/page_count': 1000,
                'rss/resource_count': 5000,
            },
            'autosave': 'rss/page_count',
        }


        self.token = config['token']
        self.api_base_params = {
            'count': int(config['count']),
            'ranked': config['ranked'],
            'similar': 'true',
            'unreadOnly': 'false',
        }
        self._config = config

    def open_spider(self, spider):
        self.logger.info(f'Spider parameters:\n{pformat(self._config)}')

    def start_requests(self):
        return self.try_feed_urls(self._query, search_callback=self.single_feed_only_with_prompt)

    def get_streams_url(self, feed_id, **params):
        return feedly.build_api_url('streams', streamId=feed_id, **self.api_base_params, **params)

    @with_authorization
    def try_feed_urls(self, query, *, search_callback, search_kwargs=None, **kwargs):
        effective_templates = {k: v for k, v in self._url_templates.items() if k.match(query)}
        query_parsed = urlsplit(query)
        specifiers = {**query_parsed._asdict(), 'original': query_parsed.geturl()}
        urls = [t % specifiers for _, t in sorted(
            (priority, tmpl)
            for pattern, tmpls in effective_templates.items()
            for tmpl, priority in tmpls.items()
        )]

        for u in urls:
            yield from self.next_page(
                {'id': f'feed/{u}'}, callback=self.parse_feed,
                meta={
                    **kwargs.pop('meta', {}),
                    'inc_depth': True,
                    'candidate_id': u,
                    'candidate_for': query,
                    'search_callback': search_callback,
                    'search_kwargs': search_kwargs or {},
                }, initial=True,
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
            self.logger.critical(f'Cannot find a feed from Feedly using the query `{self._query}`')
            wait(5)
            return
        if len(feed) > 1:
            msg = [
                f'Found more than one possible feeds using the query `{self._query}`:',
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
            meta.pop('inc_depth', None)

        params = {}
        cont = data.get('continuation')
        if cont:
            params['continuation'] = cont
        elif not initial:
            raise FeedExhausted(response)

        url = self.get_streams_url(feed, **params)
        if response:
            yield response.request.replace(url=url, meta=meta)
            return
        yield Request(url, meta=meta, **kwargs)

    def parse_feed(self, response: TextResponse):
        data = _guard_json(response.text)
        items = data.get('items')
        if items:
            yield {'valid_feed': response}

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
