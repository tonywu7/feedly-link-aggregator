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
from typing import List
from urllib.parse import urlsplit

from scrapy.http import Request, TextResponse

from ..datastructures import compose_mappings
from ..feedly import FeedlyEntry
from ..requests import FinishedRequest
from ..utils import SpiderOutput
from .base import FeedlyRSSSpider


class FeedClusterSpider(FeedlyRSSSpider):
    name = 'cluster'

    custom_settings = compose_mappings(FeedlyRSSSpider.custom_settings, {
        'SPIDER_MIDDLEWARES': {
            'feedly.spiders.cluster.ExplorationSpiderMiddleware': 900,
        },
        'DEPTH_PRIORITY': 1,
        'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue',
        'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue',
    })

    class SpiderConfig(FeedlyRSSSpider.SpiderConfig):
        FOLLOW_DOMAINS = None
        DEPTH_LIMIT = 1

    def __init__(self, name=None, **kwargs):
        super().__init__(name=name, **kwargs)

        domains = self.config['FOLLOW_DOMAINS']
        if isinstance(domains, str):
            domains = set(domains.split(' '))
        elif isinstance(domains, List):
            domains = set(domains)
        self.config['FOLLOW_DOMAINS'] = domains

        self.logstats_items.extend([
            'rss/hyperlink_count',
            'cluster/1_discovered_nodes',
            'cluster/2_scheduled_nodes',
            'cluster/3_finished_nodes',
            'cluster/4_explored',
            'cluster/5_maxdepth',
        ])

    def start_requests(self):
        return super().start_requests()


class ExplorationSpiderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def __init__(self, crawler):
        self.stats = crawler.stats
        self.logger = logging.getLogger('explore')
        self._discovered = set()
        self._finished = set()

    def process_spider_output(self, response: TextResponse, result: SpiderOutput, spider: FeedClusterSpider):
        depth = response.meta.get('depth', 0)
        self.stats.max_value('cluster/5_maxdepth', depth)
        for data in result:
            if isinstance(data, FinishedRequest):
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
        spider: FeedClusterSpider,
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
