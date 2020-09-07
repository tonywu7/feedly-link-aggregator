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
import sqlite3
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
from scrapy.http import Request, TextResponse
from scrapy.signals import spider_opened
from scrapy_promise import Promise, fetch
from twisted.python.failure import Failure

from .. import feedly, utils
from ..config import Config
from ..exceptions import FeedExhausted
from ..feedly import FeedlyEntry
from ..settings import __version__
from ..sql import utils as db_utils
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
            'feedly.spiders.rss_spider.SQLFormatSpiderMiddleware': 200,
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
        source = response.meta["feed_url"]
        if items:
            response.meta['valid_feed'] = True
            if response.meta.get('reason') != 'continuation':
                self.logger.info(f'Got new RSS feed at {source}')

        for item in items:
            entry = FeedlyEntry.from_upstream(item)
            if not entry.source:
                entry.source = {'feed': source}
            if not entry:
                continue
            self.stats.inc_value('rss/page_count')

            depth = response.meta.get('depth', 0)
            store = utils.HyperlinkStore()
            for k, v in entry.markup.items():
                store.parse_html(entry.url, v)

            yield {
                'item': entry,
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

    def digest_feed_export(self, stream):
        self.logger.info('Digesting crawled data, this may take a while...')

        conn = sqlite3.connect(self.config['OUTPUT'].joinpath('index.db'))
        cursor = conn.cursor()

        db_utils.create_all(conn, cursor)
        db_utils.verify_version(conn, cursor, __version__)
        identity_conf = db_utils.load_identity_config()

        tables = ('item', 'keyword', 'markup', 'url')
        max_row = db_utils.select_max_rowids(cursor, tables)

        def autoinc(table, staged):
            return range(max_row[table], max_row[table] + len(staged))

        identities = {}
        for table, conf in identity_conf.items():
            identities[table] = db_utils.select_identity(cursor, table, conf)

        self.logger.info('Existing records:')
        for name, rows in identities.items():
            if name[0] != '_':
                self.logger.info(f'  {name}: {len(rows)}')

        ids_url = identities['url']
        ids_item = identities['item']
        ids_keyword = identities['keyword']

        hyperlinks = {}
        items = {}
        feeds = {}
        markup = {}
        taggings = []

        next_line = stream.readline()
        while next_line:
            data: JSONDict = json.loads(next_line.rstrip())
            rowtype = data.pop('__')

            if rowtype == 'url':
                id_ = ((data['url'],),)
                ids_url.setdefault(id_, None)
            if rowtype == 'keyword':
                id_ = ((data['keyword'],),)
                ids_keyword.setdefault(id_, None)
            if rowtype == 'item':
                id_ = ((data['hash'],),)
                ids_item.setdefault(id_, None)
                items[id_] = data
            if rowtype == 'hyperlink':
                hyperlinks[(data['source_id'], data['target_id'])] = data
            if rowtype == 'feed':
                feeds[data['url_id']] = data
            if rowtype == 'markup':
                markup[(data['item_id'], data['type'])] = data
            if rowtype == 'tagging':
                taggings.append(data)

            next_line = stream.readline()

        staged = [u for u, i in ids_url.items() if i is None]
        staged = {u: i for i, u in zip(autoinc('url', staged), staged)}
        ids_url.update(staged)
        staged = [(u[0][0], i) for u, i in staged.items()]
        self.logger.info(f'New URLs: {len(staged)}')
        cursor.executemany(
            'INSERT INTO url (url, id) VALUES (?, ?)', staged,
        )
        conn.commit()

        staged = [k for k, i in ids_keyword.items() if i is None]
        staged = {k: i for i, k in zip(autoinc('keyword', staged), staged)}
        ids_keyword.update(staged)
        staged = [(k[0][0], i) for k, i in staged.items()]
        cursor.executemany(
            'INSERT INTO keyword (keyword, id) VALUES (?, ?)', staged,
        )
        conn.commit()

        staged = [f for f, i in ids_item.items() if i is None]
        staged = {f: items[f] for f in staged}
        for i, item in zip(autoinc('item', staged), staged.values()):
            item['id'] = i
            item['url'] = ids_url[((item['url'],),)]
            item['source'] = ids_url[((item['source'],),)]
        ids_item.update({k: v['id'] for k, v in staged.items()})
        self.logger.info(f'New webpages: {len(staged)}')
        cursor.executemany(
            'INSERT INTO item (id, hash, url, source, author, published, updated, crawled)'
            'VALUES (:id, :hash, :url, :source, :author, :published, :updated, :crawled)',
            staged.values(),
        )
        conn.commit()

        staged = {(ids_url[((p[0],),)], ids_url[((p[1],),)]): t for p, t in hyperlinks.items()}
        staged = {p: t for p, t in staged.items() if p not in identities['hyperlink']}
        staged = [(p[0], p[1], t['html_tag']) for p, t in staged.items()]
        self.logger.info(f'New hyperlinks: {len(staged)}')
        cursor.executemany(
            'INSERT INTO hyperlink (source_id, target_id, html_tag) VALUES (?, ?, ?)', staged,
        )
        conn.commit()

        staged = {ids_url[((k,),)]: v.get('title', '') for k, v in feeds.items()}
        staged = {k: v for k, v in staged.items() if (k,) not in identities['feed']}
        staged = [(u, t) for u, t in staged.items()]
        self.logger.info(f'New feed sources: {len(staged)}')
        cursor.executemany(
            'INSERT INTO feed (url_id, title) VALUES (?, ?)', staged,
        )
        conn.commit()

        staged = {(ids_item[((k[0],),)], k[1]): v for k, v in markup.items()}
        staged = {k: v for k, v in staged.items() if (k,) not in identities['markup']}
        staged = [(k[0], k[1], v['markup']) for k, v in staged.items()]
        cursor.executemany(
            'INSERT INTO markup (item_id, type, markup) VALUES (?, ?, ?)', staged,
        )
        conn.commit()

        staged = [(ids_item[((t['item_id'],),)], ids_keyword[((t['keyword_id'],),)]) for t in taggings]
        staged = {t for t in staged if t not in identities['tagging']}
        cursor.executemany(
            'INSERT INTO tagging (item_id, keyword_id) VALUES (?, ?)', staged,
        )
        conn.commit()

        conn.close()


class SQLFormatSpiderMiddleware:
    def process_spider_output(self, response, result, spider):
        for data in result:
            if isinstance(data, Request) or 'item' not in data:
                yield data
                continue
            item: FeedlyEntry = data['item']
            urls: utils.HyperlinkStore = data['urls']

            yield {
                '__': 'item',
                'hash': item.id_hash,
                'url': item.url,
                'source': item.source['feed'],
                'author': item.author,
                'published': item.published.isoformat(),
                'updated': item.updated.isoformat() if item.updated else None,
                'crawled': data['time_crawled'],
            }

            src = item.url
            yield {'__': 'url', 'url': src}
            for u, kws in urls.items():
                yield {'__': 'url', 'url': u}
                yield {'__': 'hyperlink', 'source_id': src, 'target_id': u, 'html_tag': list(kws['tag'])[0]}

            for k in item.keywords:
                yield {'__': 'keyword', 'keyword': k}
                yield {'__': 'tagging', 'item_id': item.id_hash, 'keyword_id': k}

            feed = item.source['feed']
            yield {'__': 'url', 'url': feed}
            yield {'__': 'feed', 'url_id': feed, 'title': item.source.get('title', '')}

            for t, m in item.markup.items():
                yield {'__': 'markup', 'item_id': item.id_hash, 'type': t, 'markup': m}
