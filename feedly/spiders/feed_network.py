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

import time
from typing import List, Union
from urllib.parse import urlsplit

import igraph
import simplejson as json
from scrapy.http import Request, TextResponse

from .single_feed import FeedlyRSSSpider
from .. import utils
from ..feedly import FeedlyEntry
from ..utils import JSONDict


class SiteNetworkSpider(FeedlyRSSSpider):
    name = 'feed_network'

    custom_settings = utils.compose_mappings(FeedlyRSSSpider.custom_settings, {
        'SPIDER_MIDDLEWARES': {
            'feedly.spiders.single_feed.FeedResourceMiddleware': None,
            'feedly.spiders.single_feed.FeedEntryMiddleware': None,
            'feedly.spiders.feed_network.GraphExpansionMiddleware': 900,
        },
    })

    NODE_TRANSFORMS = {
        'feeds': lambda c: list(set(c)),
        'names': lambda c: list(set(c)),
        'keywords': lambda c: list({k.lower() for k in c}),
    }
    EDGE_TRANSFORMS = {
        'hrefs': lambda c: list({tuple(h) for h in c}),
    }

    class SpiderConfig(FeedlyRSSSpider.SpiderConfig):
        OVERWRITE = True

        ALLOWED_DOMAINS = None
        DEPTH_LIMIT = 1

    def __init__(self, name=None, **kwargs):
        super().__init__(name=name, **kwargs)

        domains = self.config['ALLOWED_DOMAINS']
        if isinstance(domains, str):
            domains = set(domains.split(' '))
        elif isinstance(domains, List):
            domains = set(domains)
        self.config['ALLOWED_DOMAINS'] = domains

        self.logstats_items.extend([
            'rss/hyperlink_count',
            'network/discovered_nodes',
            'network/scheduled_nodes',
            'network/finished_nodes',
            'network/explored',
        ])

    def crawl_search_result(self, _):
        if _ is None:
            return
        response, feed = _
        if not feed or len(feed) > 1:
            return
        feed = feed[0]
        yield from self.next_page({'id': feed}, response=response, initial=True)

    def _digest(self, stream):
        items = {}
        resources = utils.HyperlinkStore()

        vertices = {}
        edges = {}

        next_line = stream.readline()
        while next_line:
            data: JSONDict = json.loads(next_line.rstrip())
            if '_graph' in data:
                src = data['src']
                metadata = data['metadata']
                items[src] = metadata

                vertices[src] = True
                for dest, keywords in data['dests'].items():
                    tag_name = keywords['tag'][0]
                    depth = data['depth']
                    time_crawled = data['time_crawled']
                    vertices[dest] = True
                    edges[(src, dest)] = (tag_name, depth, time_crawled)

                    resources.put(
                        dest, **{k: set(v) for k, v in keywords.items()},
                        feedly_id={metadata['id_hash']},
                        feedly_keyword=set(metadata['keywords']),
                    )

            next_line = stream.readline()

        g = igraph.Graph(directed=True)
        vertex_ids = {k: i for k, i in zip(vertices, range(len(vertices)))}
        edges = {(vertex_ids[t[0]], vertex_ids[t[1]]): v for t, v in edges.items()}
        g.add_vertices(len(vertices))
        g.add_edges(edges)
        g.vs['url'] = list(vertices)
        g.es['type'], g.es['depth'], g.es['timestamp'] = tuple(zip(*edges.values()))

        return g, items, resources

    def digest_feed_export(self, stream):
        self.logger.info('Digesting crawled data, this may take a while...')
        g, items, resources = self._digest(stream)

        path = self.config['OUTPUT'].joinpath('index.graphml')
        with open(path, 'w') as f:
            self.logger.info(f'Saving graph to {path} ...')
            g.write(f, format='graphml')

        path = self.config['OUTPUT'].joinpath('entries.json')
        with open(path, 'w') as f:
            self.logger.info(f'Saving feed content to {path} ...')
            json.dump(items, f, **utils.SIMPLEJSON_KWARGS)

        path = self.config['OUTPUT'].joinpath('resources.json')
        with open(path, 'w') as f:
            self.logger.info(f'Saving feed content to {path} ...')
            json.dump(items, f, **utils.SIMPLEJSON_KWARGS)


class GraphExpansionMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def __init__(self, crawler):
        self.stats = crawler.stats
        self._discovered = set()

    def process_spider_output(self, response: TextResponse, result: List[Union[FeedlyEntry, Request]], spider: SiteNetworkSpider):
        depth = response.meta.get('depth')
        for item in result:
            if not isinstance(item, FeedlyEntry):
                yield item
                continue
            self.stats.inc_value('rss/page_count')
            yield from self.process_item(response, item, spider, depth)

    def process_item(self, response: TextResponse, item: FeedlyEntry, spider: SiteNetworkSpider, depth=None):
        store = utils.HyperlinkStore()
        for k, v in item.markup.items():
            store.parse_html(item.url, v)

        dest = {urlsplit(k): v for k, v in store.items()}
        dest = {k: v for k, v in dest.items() if k.netloc}
        self.stats.inc_value('rss/hyperlink_count', len(dest))

        sites = {f'{u.scheme}://{u.netloc}' for u in dest} - self._discovered
        self._discovered |= sites
        spider.logger.debug(f'depth={depth}; +{len(sites)}')

        for url in sites:
            spider.logger.debug(f'Possible new feed {url} (depth={depth})')

            def set_priority(r: TextResponse):
                if r.status >= 400:
                    return response.request.priority + 100
                else:
                    return response.request.priority - 100

            def default_priority(failure):
                return 0

            def start_crawl(priority):
                return spider.start_feed(
                    url, priority=priority, response=response,
                    meta={
                        'inc_depth': True,
                        'depth': depth,
                        'reason': 'newly_discovered',
                        'source_item': item,
                    },
                )

            def log(exc):
                self.logger.error(exc, exc_info=True)
                self.logger.error(f'in {url}')

            yield from (  # noqa: ECE001
                # fetch(url, method='HEAD', meta={'inc_depth': True, 'depth': depth})
                # .then(set_priority, default_priority)
                # .then(start_crawl)
                start_crawl(0)
                .then(spider.start_search(url))
                .then(spider.crawl_search_result)
                .catch(log)
                # .catch(spider.log_exception)
                .finally_(self.update_finished)
            )

        self.stats.set_value('network/discovered_nodes', len(self._discovered))
        depth_limit = spider.config.getint('DEPTH_LIMIT')
        if depth_limit and depth < depth_limit or not depth_limit:
            self.stats.inc_value('network/scheduled_nodes', len(sites))
        self.update_ratio()

        yield {
            '_graph': 1,
            'src': item.url,
            'dests': store,
            'depth': depth,
            'time_crawled': time.time(),
            'metadata': item,
        }

    def update_finished(self):
        finished = self.stats.get_value('network/finished_nodes', 0)
        finished += 1
        self.stats.set_value('network/finished_nodes', finished)
        self.update_ratio()

    def update_ratio(self):
        ratio = self.stats.get_value('network/finished_nodes', 0) / self.stats.get_value('network/scheduled_nodes', 1)
        self.stats.set_value('network/explored', f'{ratio * 100:.2f}%')
