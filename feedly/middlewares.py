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
from collections.abc import Mapping
from typing import Callable, List

from scrapy import Spider
from scrapy.http import Request, Response
from scrapy.exceptions import IgnoreRequest
from scrapy.spidermiddlewares.depth import DepthMiddleware
from scrapy.utils.url import url_is_from_any_domain

from . import feedly
from .exceptions import FeedExhausted


class ConditionalDepthMiddleware(DepthMiddleware):
    def process_spider_output(self, response, result, spider):
        if response.meta.get('inc_depth'):
            return super().process_spider_output(response, result, spider)
        return result


def filter_depth(request: Request, spider: Spider):
    depth = request.meta.get('depth')
    max_depth = spider.config['NETWORK_DEPTH']
    if depth is not None and max_depth is not None:
        if depth > max_depth:
            return False
    return True


def filter_domains(request: Request, spider: Spider):
    domains = spider.config['ALLOWED_DOMAINS']
    feed_url = request.meta.get('feed_url')
    if not feed_url or domains is None:
        return True
    return url_is_from_any_domain(feed_url, domains)


class RequestFilterMiddleware:
    DEFAULT_FILTERS = {
        filter_domains: 300,
        filter_depth: 700,
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
        # print({k: k(request, spider) for k in self.tests})
        if any((not t(request, spider)) for t in self.tests):
            ignore = request.meta.get('if_ignore')
            if ignore:
                ignore()
            raise IgnoreRequest()
        proceed = request.meta.get('if_proceed')
        if proceed:
            proceed()
        return None


class FeedCompletionMiddleware:
    def __init__(self):
        self.log = logging.getLogger('feedly.search')

    def process_spider_input(self, response: Response, spider):
        meta, candidates, this, query = self._unpack_meta(response)
        if candidates:
            candidates[this] = False

    def process_spider_output(self, response: Response, result, spider):
        meta, candidates, this, query = self._unpack_meta(response)
        if not candidates:
            return result
        for r in result:
            if isinstance(r, Mapping) and 'valid_feed' in r:
                candidates[this] = True
            yield r

    def process_spider_exception(self, response: Response, exception: FeedExhausted, spider):
        if not isinstance(exception, FeedExhausted):
            return

        meta, candidates, this, query = self._unpack_meta(response)
        if not candidates:
            return

        if candidates[this] is False:
            self.log.debug(f'Empty feed {this}')
        states = set(candidates.values())
        if None in states:
            return []

        if True not in states:
            self.log.info(f'No valid RSS feed can be found using `{query}` and available feed templates.')
            if getattr(spider, 'fuzzy', None):
                self.log.info('Searching via Feedly ...')
                meta['reason'] = 'search'
                callback = meta['search_callback']
                kwargs = meta.get('search_kwargs', {})
                cb_kwargs = {**kwargs.pop('cb_kwargs', {}), 'callback': callback}
                return [Request(
                    feedly.build_api_url('search', query=query),
                    callback=spider.parse_search_result,
                    cb_kwargs=cb_kwargs,
                    priority=2,
                    **kwargs,
                )]
        return []

    def _unpack_meta(self, request):
        return request.meta, request.meta.get('feed_candidates'), request.meta.get('feed_url'), request.meta.get('feed_query')


class HTTPErrorMiddleware:
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
            self.log.critical('Server returned HTTP 429 Too Many Requests.')
            self.log.critical('Either your IP address or your developer account is being rate-limited.')
            self.log.critical('Crawler will now stop.')
            self.crawler.engine.close_spider(spider, 'rate_limited')
            raise IgnoreRequest()
        return response
