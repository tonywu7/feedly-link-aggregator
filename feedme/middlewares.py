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

import logging
from contextlib import suppress
from urllib.parse import urlsplit

from scrapy.exceptions import DontCloseSpider, IgnoreRequest, NotConfigured
from scrapy.http import Request, Response
from scrapy.signals import spider_idle
from scrapy.spidermiddlewares.depth import DepthMiddleware
from scrapy.utils.url import url_is_from_any_domain
from twisted.internet.defer import DeferredList
from twisted.python.failure import Failure

from .docs import OptionsContributor
from .feedly import build_api_url, get_feed_uri
from .requests import ProbeFeed
from .signals import request_finished, show_stats
from .utils import colored as _
from .utils import guard_json, is_rss_xml, wait


class ConditionalDepthSpiderMiddleware(DepthMiddleware):
    @classmethod
    def from_crawler(cls, crawler):
        crawler.signals.send_catch_log(show_stats, names=['request_depth_max'])
        return super().from_crawler(crawler)

    def __init__(self, maxdepth, stats, verbose_stats=True, prio=1):
        super().__init__(maxdepth, stats, verbose_stats=verbose_stats, prio=prio)

    def process_spider_output(self, response, result, spider):
        if self.maxdepth is None:
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
            if increase_in > 1:
                r.meta['inc_depth'] = increase_in - 1
            other_items.append(r)
        other_items.extend(super().process_spider_output(response, should_increase, spider))
        return other_items


class FeedProbingDownloaderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        crawler.signals.send_catch_log(show_stats, names=[
            'feedprober/attempts',
            'feedprober/successful',
            'feedprober/valid_feeds',
        ])
        return cls(crawler)

    def __init__(self, crawler):
        self.logger = logging.getLogger('worker.prober')
        self.test_status = crawler.settings.get('SELECT_FEED_STATE', 'all') in {'dead', 'dead+', 'alive', 'alive+'}
        self.stats = crawler.stats

    async def process_request(self, request: ProbeFeed, spider):
        if not isinstance(request, ProbeFeed):
            return

        download = spider.crawler.engine.download
        meta = request.meta
        feeds = meta['try_feeds']
        query = meta['feed_url']
        valid_feeds = []

        self.logger.info(_(f'Probing {query}', color='grey'))
        self.stats.inc_value('feedprober/attempts')

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

        if valid_feeds:
            self.stats.inc_value('feedprober/successful')
            self.stats.inc_value('feedprober/valid_feeds', len(valid_feeds))

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
            requests.append(download(Request(
                get_feed_uri(feed), method='HEAD', meta={
                    'url': feed,
                    'max_retry_times': 0,
                    'download_timeout': 20,
                }), spider))

        results = await DeferredList(requests, consumeErrors=True)

        for successful, response in results:
            if isinstance(response, Failure):
                if not isinstance(response.value, Request):
                    continue
                response = response.value
                dead = True
            elif not successful:
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
        return cls(crawler)

    def __init__(self, crawler):
        self.signals = crawler.signals

    def process_exception(self, request: Request, exception, spider):
        self.signals.send_catch_log(request_finished, request=request)


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
            self.log.warning('Server returned HTTP 401 Unauthorized.')
            self.log.warning('This is because you are accessing an API that requires authorization, and')
            self.log.warning('your either did not provide, or provided a wrong access token.')
            self.log.warning(f'URL: {request.url}')
            raise IgnoreRequest()
        if response.status == 429 and urlsplit(request.url) == 'cloud.feedly.com':
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                retry_after = int(retry_after)
                self.log.warning('Server returned HTTP 429 Too Many Requests.')
                self.log.warning('Either your IP address or your developer account is being rate-limited.')
                self.log.warning(f'Retry-After = {retry_after}s')
                self.log.warning(f'Scrapy will now pause for {retry_after}s')
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
