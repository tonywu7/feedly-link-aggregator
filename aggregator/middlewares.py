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

import atexit
import gzip
import logging
import os
import pickle
import shutil
import time
from collections import deque
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import suppress
from itertools import permutations
from pathlib import Path
from threading import Event, Thread
from urllib.parse import urlsplit

import simplejson as json
from scrapy.exceptions import DontCloseSpider, IgnoreRequest, NotConfigured
from scrapy.http import Request, Response
from scrapy.signals import (request_dropped, request_scheduled, spider_closed,
                            spider_idle)
from scrapy.spidermiddlewares.depth import DepthMiddleware
from scrapy.utils.url import url_is_from_any_domain
from twisted.internet.defer import DeferredList
from twisted.python.failure import Failure

from .docs import OptionsContributor
from .feedly import build_api_url, get_feed_uri
from .requests import ProbeFeed, RequestFinished, reconstruct_request
from .utils import colored as _
from .utils import guard_json, is_rss_xml, sha1sum, wait


class ConditionalDepthSpiderMiddleware(DepthMiddleware):
    def __init__(self, maxdepth, stats, verbose_stats=False, prio=1):
        super().__init__(maxdepth, stats, verbose_stats=verbose_stats, prio=prio)

    def process_spider_output(self, response, result, spider):
        if not self.maxdepth:
            self.maxdepth = spider.config.getint('DEPTH_LIMIT')
        should_increase = []
        other_items = []
        for r in result:
            if not isinstance(r, Request):
                other_items.append(r)
                continue
            increase_in = r.meta.get('inc_depth', 0)
            if increase_in == 1:
                should_increase.append(r)
                continue
            elif increase_in > 1:
                r.meta['inc_depth'] = increase_in - 1
            other_items.append(r)
        other_items.extend(super().process_spider_output(response, should_increase, spider))
        return other_items


class FeedProbingDownloaderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        self.logger = logging.getLogger('worker.prober')
        self.test_status = settings.get('FEED_STATE_SELECT', 'all') in {'dead', 'dead+', 'alive', 'alive+'}

    async def process_request(self, request: ProbeFeed, spider):
        if not isinstance(request, ProbeFeed):
            return

        download = spider.crawler.engine.download
        meta = request.meta
        feeds = meta['try_feeds']
        query = meta['feed_url']
        valid_feeds = []

        self.logger.info(_(f'Probing {query}', color='grey'))

        queries = []
        for feed_id in feeds:
            url = spider.get_streams_url(feed_id, count=1)
            queries.append(download(Request(url, meta={'feed': feed_id}), spider))
        results = await DeferredList(queries, consumeErrors=True)

        valid_feeds = {}
        feed_info = {}
        for successful, response in results:
            if not successful:
                continue
            data = guard_json(response.text)
            if data.get('items'):
                feed = response.meta['feed']
                valid_feeds[feed] = None
                feed_info[feed] = self.feed_info(data)

        if not valid_feeds and spider.config.getbool('ENABLE_SEARCH'):
            response = await spider.crawler.engine.download(Request(build_api_url('search', query=query)), spider)
            data = guard_json(response.text)
            if data.get('results'):
                for feed in data['results']:
                    feed_id = feed['feedId']
                    valid_feeds[feed_id] = None
                    feed_info[feed_id] = self.feed_info(data)

        if self.test_status:
            await self.probe_feed_status(valid_feeds, download, spider)

        meta['valid_feeds'] = valid_feeds
        meta['feed_info'] = feed_info
        del meta['try_feeds']
        if meta.get('inc_depth'):
            meta['depth'] -= 1
        return Response(url=request.url, request=request)

    def feed_info(self, data):
        return {
            'url': get_feed_uri(data['id']),
            'title': data.get('title', ''),
        }

    async def probe_feed_status(self, feeds, download, spider):
        requests = []
        for feed in feeds:
            feed = feed
            requests.append(download(Request(
                get_feed_uri(feed), method='HEAD', meta={
                    'url': feed,
                    'max_retry_times': 2,
                    'download_timeout': 10,
                }), spider))

        results = await DeferredList(requests, consumeErrors=True)

        for successful, response in results:
            if isinstance(response, Failure):
                response = response.value.response
            if not successful:
                dead = True
            elif response.status not in {200, 206, 405}:
                dead = True
            elif not is_rss_xml(response):
                dead = True
            else:
                dead = False
            feeds[response.meta['url']] = dead


class RequestPersistenceDownloaderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        instance = cls(crawler.settings)
        instance.crawler = crawler
        crawler.signals.connect(instance.eager_process, request_scheduled)
        crawler.signals.connect(instance._remove_request, request_dropped)
        crawler.signals.connect(instance._close, spider_closed)
        return instance

    def __init__(self, settings):
        self.logger = logging.getLogger('worker.request')
        self.closing = Event()
        self.thread = Thread(None, target=self.worker, name='RequestPersistenceThread',
                             args=(self.closing,), daemon=True)
        self.future = None

        self._jobdir = settings['JOBDIR']
        output = settings['OUTPUT']
        self.archive_path = output / 'scheduled' / 'freezer'
        self.freezer = RequestFreezer(self.archive_path)
        self.thread.start()
        atexit.register(self._close)
        atexit.register(self._rmjobdir)

    def worker(self, closing: Event):
        while not closing.wait(20):
            try:
                self.archive()
            except Exception as e:
                self.logger.error(e, exc_info=True)
        self.archive()

    def archive(self):
        self.freezer.flush()

    def eager_process(self, request, spider):
        with suppress(IgnoreRequest):
            self.process_request(request, spider, True)

    def process_request(self, request: Request, spider, idempotent=False):
        meta = request.meta
        action = meta.get('_persist', None)
        if not action:
            if request.url == 'https://httpbin.org/status/204':
                raise ValueError
            return

        if action == 'release' and not idempotent:
            return self._continue(meta, request, spider)
        if action == 'add':
            self.freezer.add(request)
            if not idempotent:
                del meta['_persist']
            return
        if action == 'remove':
            self.freezer.remove(request)
            raise IgnoreRequest()

    def process_exception(self, request: Request, exception, spider):
        if isinstance(request, RequestFinished):
            return
        if isinstance(exception, IgnoreRequest):
            return RequestFinished(meta=request.meta)

    def _remove_request(self, request: Request, spider=None):
        self.freezer.remove(request)

    def _continue(self, meta, request, spider):
        if self.archive_path.exists():
            self.logger.info('Restoring persisted requests...')
        del meta['_persist']

        meta['freezer'] = self.freezer
        return Response(url=request.url, request=request)

    def _close(self, spider=None, reason=None):
        if self.closing.is_set():
            return
        self.closing.set()
        self.thread.join(2)
        self.archive()
        num_requests = len(self.freezer)
        if num_requests:
            self.logger.info(_(f'# of requests persisted to filesystem: {num_requests}', color='cyan'))

    def _rmjobdir(self):
        with suppress(Exception):
            shutil.rmtree(Path(self._jobdir) / 'requests.queue')


class RequestFreezer:
    def __init__(self, path):
        self.wd = Path(path)
        self.path = self.wd / 'frozen'
        os.makedirs(self.path, exist_ok=True)
        self.buffer = deque()

    def add(self, request):
        key = request.meta.get('pkey')
        if not key:
            return
        for_pickle = {
            'url': request.url,
            'method': request.method,
            'callback': request.callback.__name__,
            'meta': {**request.meta, '_time_pickled': time.perf_counter()},
            'priority': request.priority,
        }
        self.buffer.append(('add', key, (request.__class__, for_pickle)))

    def remove(self, request):
        key = request.meta.get('pkey')
        if not key:
            return
        self.buffer.append(('remove', key, None))

    def flush(self):
        shelves = {}
        buffer = self.buffer
        self.buffer = deque()
        for action, key, item in buffer:
            hash_ = sha1sum(pickle.dumps(key))
            label = hash_[:2]
            shelf = shelves.get(label)
            if not shelf:
                shelf = shelves[label] = self.open_shelf(label)
            if action == 'add':
                shelf[hash_] = item
            if action == 'remove':
                shelf.pop(hash_, None)
        self.persist(shelves)
        del shelves
        del buffer

    def open_shelf(self, shelf, path=None):
        path = path or self.path / shelf
        try:
            with gzip.open(path) as f:
                return pickle.load(f)
        except Exception:
            return {}

    def persist(self, shelves, path=None):
        path = path or self.path
        for shelf, items in shelves.items():
            shelf = path / shelf
            with gzip.open(shelf, 'wb') as f:
                pickle.dump(items, f)

    def copy(self, src, dst):
        def cp(shelf):
            srcd = self.open_shelf(shelf, src / shelf)
            if not srcd:
                return
            dstd = self.open_shelf(shelf, dst / shelf)
            dstd.update(srcd)
            self.persist({shelf: dstd}, dst)

        with ThreadPoolExecutor(max_workers=32) as executor:
            executor.map(cp, [i + j for i, j in self.names()])

    def defrost(self, spider):
        info = self.load_info()
        defroster_path = self.wd / 'defrosting'
        if defroster_path.exists():
            self.copy(self.path, defroster_path)
            shutil.rmtree(self.path)
        else:
            shutil.move(self.path, defroster_path)
        os.makedirs(self.path)
        self.dump_info(info)

        defroster = RequestDefroster(defroster_path)
        for cls, kwargs in defroster:
            yield reconstruct_request(cls, spider, **kwargs)
        shutil.rmtree(defroster_path)

    def clear(self):
        shutil.rmtree(self.path)
        self.path.mkdir()

    def load_info(self):
        info = {}
        with suppress(EOFError, FileNotFoundError,
                      json.JSONDecodeError, gzip.BadGzipFile):
            with open(self.path / 'info.json') as f:
                return json.load(f)
        return info

    def dump_info(self, info):
        with open(self.path / 'info.json', 'w+') as f:
            json.dump(info, f)

    def names(self):
        return permutations('0123456789abcdef', 2)

    def __len__(self):
        length = 0
        for i, j in self.names():
            shelf = i + j
            length += len(self.open_shelf(shelf))
        return length

    def iter_keys(self):
        for i, j in self.names():
            shelf = i + j
            shelf = self.open_shelf(shelf)
            yield from shelf


class RequestDefroster(RequestFreezer):
    def __init__(self, path):
        self.path = Path(path)

    def __iter__(self):
        for i, j in self.names():
            name = i + j
            shelf = self.open_shelf(name)
            yield from shelf.values()
            with suppress(FileNotFoundError):
                os.unlink(self.path / name)


class RequestDefrosterSpiderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        instance = cls()
        instance.crawler = crawler
        crawler.signals.connect(instance.leftover_requests, spider_idle)
        return instance

    def __init__(self):
        self.resume_iter = None

    def process_spider_output(self, response, result, spider):
        if spider.resume_iter:
            self.resume_iter = spider.resume_iter
            spider.resume_iter = None
        if self.resume_iter:
            self.defrost_in_batch(spider)
        return result

    def defrost_in_batch(self, spider, batch=100, maxsize=1000):
        i = 0
        while not batch or i < batch:
            try:
                self.crawler.engine.crawl(next(self.resume_iter), spider)
                i += 1
            except StopIteration:
                self.resume_iter = None
                break

    def leftover_requests(self, spider):
        if self.resume_iter:
            self.defrost_in_batch(spider)
            raise DontCloseSpider()


class OffsiteFeedSpiderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        self.domains = settings.get('FOLLOW_DOMAINS')
        if not self.domains:
            raise NotConfigured()

    def process_spider_output(self, response, result, spider):
        for r in result:
            if not isinstance(r, Request):
                yield r
                continue
            feed_url = r.meta.get('feed_url')
            if not feed_url or url_is_from_any_domain(feed_url, self.domains):
                yield r


class DerefItemSpiderMiddleware:
    def process_spider_output(self, response, result, spider):
        for r in result:
            if not isinstance(r, Request):
                yield r
                continue
            r.meta.pop('source_item', None)
            yield r


class FetchSourceSpiderMiddleware(OptionsContributor):
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        self.logger = logging.getLogger('worker.fetchsource')
        self.scrape_source = settings.getbool('SCRAPE_SOURCE_PAGE', False)
        if not self.scrape_source:
            raise NotConfigured()

    def process_spider_output(self, response, result, spider):
        for data in result:
            if isinstance(data, Request) or 'item' not in data or 'source_fetched' in data:
                yield data
                continue
            item = data['item']
            yield Request(
                item.url, callback=self.parse_source,
                errback=self.handle_source_failure,
                meta={'data': data},
            )

    def parse_source(self, response):
        meta = response.meta
        data = meta['data']
        data['source_fetched'] = True
        item = data['item']
        with suppress(AttributeError):
            if response.status >= 400:
                self.logger.debug(f'Dropping {response}')
                raise AttributeError
            body = response.text
            item.add_markup('webpage', body)
        yield data

    def handle_source_failure(self, failure: Failure):
        self.logger.debug(failure)
        request = failure.request
        data = request.meta['data']
        data['source_fetched'] = True
        yield data

    @staticmethod
    def _help_options():
        return {
            'SCRAPE_SOURCE_PAGE': """
            Whether or not to download and process the source webpage of a feed item.
            Default is `False`.

            If disabled, spider will only process HTML snippets returned by Feedly, which contain
            mostly article summaries and sometimes images/videos, and will therefore only
            extract URLs from them.

            If enabled, then in addition to that, spider will also
            download a copy of the webpage from the source website of the feed which could
            contain many more hyperlinks, although the original webpage may not exist anymore.
            """,
        }


class CrawledItemSpiderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        path = settings['OUTPUT'] / 'crawled_items.txt'
        if path.exists():
            with open(path, 'r') as f:
                self.crawled_items |= set(f.read().split('\n'))
        else:
            raise NotConfigured()

    def process_spider_output(self, response, result, spider):
        for data in result:
            if isinstance(data, Request) or 'item' not in data:
                yield data
                continue
            item = data['item']
            if item.url not in self.crawled_items:
                yield data


class AuthorizationDownloaderMiddleware:
    def process_request(self, request: Request, spider):
        if request.headers.get('Authorization'):
            return
        auth = request.meta.get('auth')
        if auth:
            request = request.copy()
            request.headers['Authorization'] = f'OAuth {auth}'
            return request


class HTTPErrorDownloaderMiddleware:
    def __init__(self, crawler):
        self.crawler = crawler
        self.log = logging.getLogger('worker.ratelimiting')

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_response(self, request, response: Response, spider):
        if response.status == 401:
            self.log.warn('Server returned HTTP 401 Unauthorized.')
            self.log.warn('This is because you are accessing an API that requires authorization, and')
            self.log.warn('your either did not provide, or provided a wrong access token.')
            self.log.warn(f'URL: {request.url}')
            raise IgnoreRequest()
        if response.status == 429 and urlsplit(request.url) == 'cloud.feedly.com':
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                retry_after = int(retry_after)
                self.log.warn('Server returned HTTP 429 Too Many Requests.')
                self.log.warn('Either your IP address or your developer account is being rate-limited.')
                self.log.warn(f'Retry-After = {retry_after}s')
                self.log.warn(f'Scrapy will now pause for {retry_after}s')
                spider.crawler.engine.pause()
                to_sleep = retry_after * 1.2
                try:
                    wait(to_sleep)
                except KeyboardInterrupt:
                    self.crawler.engine.unpause()
                    raise
                spider.crawler.engine.unpause()
                self.log.info('Resuming crawl.')
                return request.copy()
            else:
                self.log.critical('Server returned HTTP 429 Too Many Requests.')
                self.log.critical('Either your IP address or your developer account is being rate-limited.')
                self.log.critical('Crawler will now stop.')
                self.crawler.engine.close_spider(spider, 'rate_limited')
                raise IgnoreRequest()
        return response
