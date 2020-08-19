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

from scrapy.http import Response


class RateLimitMiddleware:
    def __init__(self, crawler):
        self.crawler = crawler
        self.log = logging.getLogger('feedly.ratelimiting')

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_response(self, request, response: Response, spider):
        if response.status == 429:
            self.log.critical('Server returned HTTP 429 Too Many Requests.')
            self.log.critical('Your IP address is being rate-limited.')
            self.log.critical('Crawler will now stop.')
            self.crawler.engine.close_spider(spider, 'rate_limited')
        return response
