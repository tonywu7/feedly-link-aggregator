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

from .rss_spider import FeedlyRSSSpider

from .. import feedly, utils


class SingleFeedSpider(FeedlyRSSSpider):
    name = 'single_feed'

    def start_requests(self):
        return (
            super().start_requests()
            .then(self.start_search(self.config['FEED']))
            .then(self.crawl_search_result_verbose)
            .catch(self.log_exception)
        )

    def crawl_search_result_verbose(self, _):
        if _ is None:
            return
        response, feed = _
        query = self.config.get('FEED')
        if len(feed) == 0:
            self.logger.critical(f'Cannot find a feed from Feedly using the query `{query}`')
            utils.wait(5)
            return
        if len(feed) > 1:
            msg = [
                f'Found more than one possible feeds using the query `{query}`:',
                *['  ' + feedly.get_feed_uri(f) for f in feed],
                'Please run scrapy again using one of the values above. Crawler will now close.',
            ]
            self.logger.critical('\n'.join(msg))
            utils.wait(5)
            return
        feed = feed[0]
        self.logger.info(f'Loading from {feed}')
        return self.next_page({'id': feed}, response=response, initial=True)
