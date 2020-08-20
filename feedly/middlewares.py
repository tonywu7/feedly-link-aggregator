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
from typing import Dict, List, Union

from scrapy.http import Request, Response
from scrapy.exceptions import IgnoreRequest
from scrapy.spidermiddlewares.depth import DepthMiddleware

from . import feedly, utils
from .exceptions import FeedExhausted


class ConditionalDepthMiddleware(DepthMiddleware):
    def process_spider_output(self, response, result, spider):
        if response.meta.get('inc_depth'):
            return super().process_spider_output(response, result, spider)
        return result


class FeedCompletionMiddleware:
    def __init__(self):
        self.queries: Dict[str, Dict[str, Union[None, bool]]] = {}
        self.log = logging.getLogger('feedly.search')

    def process_spider_input(self, response: Response, spider):
        meta, query, feed = self._unpack_meta(response)
        if 'candidate_for' in meta:
            self._record_query(meta)
            if self.queries[query][feed] is None:
                self.queries[query][feed] = False

    def process_spider_output(self, response: Response, result, spider):
        meta, query, feed = self._unpack_meta(response)
        for r in result:
            if not isinstance(r, Mapping) or 'valid_feed' not in r:
                yield r
            self.queries[query][feed] = True

    def process_spider_exception(self, response: Response, exception: FeedExhausted, spider):
        if not isinstance(exception, FeedExhausted):
            return
        meta, query, feed = self._unpack_meta(response)
        if not query:
            return
        if self.queries.get(query, {}).get(feed) is False:
            self.log.debug(f'Empty feed {feed}')
        states = set(self.queries[query].values())
        if None in states:
            return []
        if True not in states:
            if getattr(spider, '_fuzzy', None):
                self.log.info(f'Candidates for `{query}` exhausted with no valid feed data found.')
                self.log.info('Fuzzy searching via Feedly ...')
                callback = meta['search_callback']
                kwargs = meta.get('search_kwargs', {})
                cb_kwargs = {**kwargs.pop('cb_kwargs', {}), 'callback': callback}
                return [Request(
                    feedly.build_api_url('search', query=query),
                    callback=spider.parse_search_result,
                    cb_kwargs=cb_kwargs,
                    **kwargs,
                )]
            else:
                self.log.debug(f'Cannot find any valid RSS feeds using `{query}` and the URL templates provided.')
                self.log.debug('(Enable fuzzy search with option `-a fuzzy=True`)')
                utils.wait(5)
        return []

    def process_start_requests(self, requests: List[Request], spider):
        for r in requests:
            meta = r.meta
            if 'candidate_for' in meta:
                self._record_query(meta)
            yield r

    def _unpack_meta(self, request):
        return request.meta, request.meta.get('candidate_for'), request.meta.get('candidate_id')

    def _record_query(self, meta):
        c = self.queries.setdefault(meta['candidate_for'], {})
        c.setdefault(meta['candidate_id'], None)


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
