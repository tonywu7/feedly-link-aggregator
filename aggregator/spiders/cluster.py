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
from urllib.parse import urlsplit

from scrapy.crawler import Crawler
from scrapy.exceptions import NotConfigured
from scrapy.http import Request, TextResponse

from ..datastructures import compose_mappings
from ..docs import OptionsContributor
from ..feedly import FeedlyEntry
from ..requests import RequestFinished
from ..utils import SpiderOutput
from ..utils import colored as _
from .base import FeedlyRSSSpider


class ExplorationSpiderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def __init__(self, crawler: Crawler):
        if crawler.spidercls is not FeedClusterSpider:
            raise NotConfigured()

        self.stats = crawler.stats
        self.logger = logging.getLogger('explore')
        self._discovered = set()
        self._finished = set()

    def process_spider_output(self, response: TextResponse, result: SpiderOutput, spider):
        depth = response.meta.get('depth', 0)
        self.stats.max_value('cluster/5_maxdepth', depth)
        for data in result:
            if isinstance(data, RequestFinished):
                self.update_finished(data)
            if isinstance(data, Request):
                yield data
                continue
            if 'item' in data:
                item = data['item']
                self.stats.inc_value('rss/page_count')
                yield from self.process_item(response, item, depth, spider)
            yield data

    def process_item(
        self, response: TextResponse,
        item: FeedlyEntry, depth: int,
        spider,
    ):
        dest = {urlsplit(k): v for k, v in item.hyperlinks.items()}
        dest = {k: v for k, v in dest.items() if k.netloc}
        self.stats.inc_value('rss/hyperlink_count', len(dest))

        sites = {f'{u.scheme}://{u.netloc}' for u in dest} - self._discovered
        self._discovered |= sites
        self.logger.debug(f'depth={depth}; +{len(sites)}')

        for url in sites:
            self.logger.debug(f'{url} (depth={depth})')
            yield spider.probe_feed(
                url, source=response.request,
                meta={
                    'inc_depth': 1,
                    'depth': depth,
                    'reason': 'newly_discovered',
                    'source_item': item,
                })

        self.stats.set_value('cluster/1_discovered_nodes', len(self._discovered))
        depth_limit = spider.config.getint('DEPTH_LIMIT')
        if depth_limit and depth < depth_limit or not depth_limit:
            self.stats.inc_value('cluster/2_scheduled_nodes', len(sites))
        self.update_ratio()

    def update_finished(self, request: Request):
        feed_url = request.meta.get('feed_url')
        if not feed_url:
            return
        self._finished.add(urlsplit(feed_url).netloc)
        self.stats.set_value('cluster/3_finished_nodes', len(self._finished))
        self.update_ratio()

    def update_ratio(self):
        finished = self.stats.get_value('cluster/3_finished_nodes', 0)
        scheduled = self.stats.get_value('cluster/2_scheduled_nodes', 1)
        if not scheduled:
            return
        ratio = finished / scheduled
        self.stats.set_value('cluster/4_explored', f'{ratio * 100:.2f}%')


class FeedClusterSpider(FeedlyRSSSpider, OptionsContributor, _doc_order=9):
    """
    Spider to crawl a group of feeds.

    It works by recursively trying to crawl websites found in the contents of a feed,
    until it hits the depth limit, or until no more crawlable website can be found.

    Usage
    -----
    `scrapy crawl cluster -s OPTIONS=... ...`

    This spider supports all options supported by the single feed spider.
    """

    name = 'cluster'

    custom_settings = compose_mappings(FeedlyRSSSpider.custom_settings, {
        'DEPTH_PRIORITY': 1,
        'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue',
        'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue',
    })

    LOGSTATS_ITEMS = [
        *FeedlyRSSSpider.LOGSTATS_ITEMS,
        'rss/hyperlink_count',
        'cluster/1_discovered_nodes',
        'cluster/2_scheduled_nodes',
        'cluster/3_finished_nodes',
        'cluster/4_explored',
        'cluster/5_maxdepth',
    ]

    class SpiderConfig(FeedlyRSSSpider.SpiderConfig):
        FOLLOW_DOMAINS = None
        DEPTH_LIMIT = 1

    def start_requests(self):
        return super().start_requests()

    def filter_feeds(self, feeds, meta):
        if meta['reason'] == 'user_specified':
            for feed in feeds:
                yield self.next_page({'id': feed}, meta=meta, initial=True)
            return

        select = self.config.get('FEED_STATE_SELECT', 'all')
        for feed, dead in feeds.items():
            prio = self.SELECTION_STRATS[select][dead]
            if not prio:
                self.logger.info(_(f'Dropped {"dead" if dead else "living"} feed {feed[5:]}', color='grey'))
            else:
                yield self.next_page({'id': feed}, meta=meta, initial=True, priority=prio)

    @staticmethod
    def _help_options():
        return {
            'FOLLOW_DOMAINS': """
            Only nodes whose domains or parent domains are included here will be expanded upon.

            Value should be a collection of domains. (Other nodes are still recorded,
            but are not used to find new feeds).

            If set to None, spider will not filter nodes based on domains.

            **Example**
                `FOLLOW_DOMAINS = ['tumblr.com', 'wordpress.com']`
            """,
            'DEPTH_LIMIT': """
            How much the spider will expand the cluster. Value should be an integer.

            (This is the same settings as the one used by the built-in ~DepthMiddleware~.)

            Nodes that are more than `depth + 1` degree removed from the starting feed
            will not be expanded upon.

            If set to ~1~, only the starting feed will be crawled.
            If set to ~0~ or ~None~, spider will keep crawling until manually stopped.
            """,
            'FEED_STATE_SELECT': """
            Only crawl feeds that are of a certain `state`.

            A feed can be in one of two states:
            `dead`    - The feed URL is unreachable (e.g. timed out); or a HEAD request
                          returns a status code other than `200 OK`, `206 Partial`, or
                          `405 Method Not Allowed`;
                          or the responded MIME type is anything other than that of a
                          valid RSS feed `(text/xml, application/xml, application/rss+xml,`
                          `application/rdf+xml, application/atom+xml)`.
            `alive`   - All other feeds are considered alive.

            This option accepts the following values:
            ~all~     - Do not filter feeds based on their state
            ~dead~    - Only crawl dead feeds
            ~alive~   - Only crawl living feeds
            ~dead+~   - Crawl all feeds, but dead feeds receive a higher priority
            ~alive+~  - Crawl all feeds, but living feeds receive a higher priority

            Note that values other than `all` cause the spider to send a HEAD request to
            each feed URL about to be crawled, which will add a slight overhead to the running time.
            """,
        }
