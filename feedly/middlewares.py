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
from typing import Callable, List

from scrapy import Spider
from scrapy.exceptions import IgnoreRequest
from scrapy.http import Request, Response
from scrapy.spidermiddlewares.depth import DepthMiddleware
from scrapy.utils.url import url_is_from_any_domain
from twisted.internet.defer import DeferredList

from . import feedly
from .utils import guard_json, wait


class ConditionalDepthSpiderMiddleware(DepthMiddleware):
    def __init__(self, maxdepth, stats, verbose_stats=False, prio=1):
        super().__init__(maxdepth, stats, verbose_stats=verbose_stats, prio=0)

    def process_spider_output(self, response, result, spider):
        if not self.maxdepth:
            self.maxdepth = spider.config.getint('DEPTH_LIMIT')
        should_increase = []
        other_items = []
        for r in result:
            if isinstance(r, Request) and r.meta.get('inc_depth'):
                should_increase.append(r)
            else:
                other_items.append(r)
        other_items.extend(super().process_spider_output(response, should_increase, spider))
        return other_items


def filter_depth(request: Request, spider: Spider):
    depth = request.meta.get('depth')
    max_depth = spider.config['DEPTH_LIMIT']
    if depth is not None and max_depth is not None:
        if depth > max_depth:
            return False
    return True


def filter_domains(request: Request, spider: Spider):
    domains = spider.config['FOLLOW_DOMAINS']
    feed_url = request.meta.get('feed_url') or request.meta.get('search_query')
    if not feed_url or domains is None:
        return True
    return url_is_from_any_domain(feed_url, domains)


class FeedlyScanDownloaderMiddleware:
    async def process_request(self, request: Request, spider):
        meta = request.meta
        if 'try_feeds' not in meta:
            return

        prefix = spider.config['STREAM_ID_PREFIX']
        feeds = meta['try_feeds']
        query = meta['search_query']
        valid_feeds = []

        scans = []
        for feed_url in feeds:
            feed_id = f'{prefix}{feed_url}'
            url = spider.get_streams_url(feed_id, count=1)
            scans.append(spider.crawler.engine.download(Request(url, meta={'feed': feed_id}), spider))
        results = await DeferredList(scans)

        for successful, response in results:
            if not successful:
                continue
            data = guard_json(response.text)
            if data.get('items'):
                valid_feeds.append(response.meta['feed'])

        if not valid_feeds and spider.config.getbool('ENABLE_SEARCH'):
            response = await spider.crawler.engine.download(Request(feedly.build_api_url('search', query=query)), spider)
            data = guard_json(response.text)
            if data.get('results'):
                valid_feeds.extend(feed['feedId'] for feed in data['results'])

        meta['valid_feeds'] = valid_feeds
        del meta['try_feeds']
        if meta.get('inc_depth'):
            meta['depth'] -= 1
        return Response(url=request.url, request=request)


class RequestFilterDownloaderMiddleware:
    DEFAULT_FILTERS = {
        filter_domains: 300,
    }

    def __init__(self):
        self.tests: List[Callable[[Request, Spider], bool]] = []
        self._initialized = False

    def init(self, spider: Spider):
        tests = {**self.DEFAULT_FILTERS, **spider.config.get('REQUEST_FILTERS', {})}
        self.tests = [t[0] for t in sorted(tests.items(), key=lambda t: t[1])]
        self._initialized = True

    def process_request(self, request: Request, spider):
        if not self._initialized:
            self.init(spider)
        if request.meta.get('no_filter'):
            return
        for t in self.tests:
            result = t(request, spider)
            if not result:
                ignore = request.meta.get('if_ignore')
                if ignore:
                    ignore()
                raise IgnoreRequest()
            if isinstance(result, Request):
                result.meta['no_filter'] = True
                return result
        proceed = request.meta.get('if_proceed')
        if proceed:
            proceed()


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
        self.log = logging.getLogger('feedly.ratelimiting')

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
        if response.status == 429:
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
