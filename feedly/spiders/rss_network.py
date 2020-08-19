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

from typing import Dict, List, Union
from urllib.parse import SplitResult, urlsplit

import networkx as nx
import simplejson as json
from networkx.classes.function import set_edge_attributes, set_node_attributes
from networkx.readwrite import node_link_data, node_link_graph
from scrapy.http import Request, TextResponse

from .rss_spider import FeedlyRssSpider
from .. import utils
from ..datastructures import KeywordCollection
from ..feedly import FeedlyEntry


def graph_for_json(self):
    return node_link_data(self)


class SiteNetworkSpider(FeedlyRssSpider):
    name = 'feed_network'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'SPIDER_MIDDLEWARES': {
            'scrapy.spidermiddlewares.depth.DepthMiddleware': None,
            'feedly.spiders.rss_network.GraphExpansionMiddleware': 900,
        },
    }

    NODE_TRANSFORMS = {
        'feeds': lambda c: list(set(c)),
        'names': lambda c: list(set(c)),
    }
    EDGE_TRANSFORMS = {
        'hrefs': lambda c: list({tuple(h) for h in c}),
    }

    def __init__(self, name=None, *, depth, **kwargs):
        super().__init__(name=name, overwrite=True, flush_watermark=kwargs.get('flush_watermark', 4096), **kwargs)
        self.depth = int(depth)
        self._logstats_milestones = {
            'rss/node_count': 100,
            'rss/page_count': 4096,
        }

        self.index: nx.Graph = nx.Graph()
        if self.output.exists():
            with open(self.output, 'r') as f:
                try:
                    gdata = json.load(f)
                    self.index = node_link_graph(gdata)
                except (json.JSONDecodeError, KeyError) as e:
                    raise ValueError(f'Cannot load graph data from existing file {self.output}') from e
        self.index.for_json = graph_for_json.__get__(self.index)

    def new_feed(self, feed, depth):
        if not feed or len(feed) > 1:
            return
        feed = feed[0]
        self.logger.info(f'Scheduling newly discovered feed: {feed}')
        yield from self.next_page({'id': feed}, depth=depth + 1, callback=self.parse)

    @classmethod
    def _index_processor(cls, g: nx.Graph):
        for attr, transform in cls.NODE_TRANSFORMS.items():
            nodes = {v: {**data, attr: transform(data[attr])} for v, data in g.nodes.data() if attr in data}
            set_node_attributes(g, nodes)
        for attr, transform in cls.EDGE_TRANSFORMS.items():
            edges = {(u, v): {**data, attr: transform(data[attr])} for u, v, data in g.edges.data() if attr in data}
            set_edge_attributes(g, edges)
        return g


class GraphExpansionMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        m = cls()
        m.stats = crawler.stats
        return m

    def process_spider_output(self, response: TextResponse, result: List[Union[FeedlyEntry, Request]], spider: SiteNetworkSpider):
        depth = response.meta.get('depth')
        if depth is not None and depth >= spider.depth:
            depth = None
        for item in result:
            if not isinstance(item, FeedlyEntry):
                yield item
                continue
            self.stats.inc_value('rss/page_count')
            yield from self.process_item(item, depth, spider)
            yield item

    def process_item(self, item: FeedlyEntry, depth: Union[None, int], spider: SiteNetworkSpider):
        store = utils.HyperlinkStore()
        for k, v in item.markup.items():
            store.parse_html(item.url, v)

        src = urlsplit(item.url)
        dest = {urlsplit(k): v for k, v in store.items()}
        dest = {k: v for k, v in dest.items() if k.netloc}
        self.stats.inc_value('rss/resource_count', len(dest))
        if depth is not None:
            for url in [f'{u.scheme}://{u.netloc}' for u in dest]:
                yield spider.search_for_feed(url, spider.new_feed, cb_kwargs={'depth': depth})

        if not src.netloc:
            return
        self.update_node(spider.index, src, dest, item)
        self.update_edges(spider.index, src, dest, item)

    def update_node(self, g: nx.Graph, src: SplitResult, dests: Dict[SplitResult, KeywordCollection], item: FeedlyEntry):
        domains = [u.netloc for u in dests]
        domains.append(src.netloc)
        for d in domains:
            if not g.has_node(d):
                self.stats.inc_value('rss/node_count', 1)
                g.add_node(d, feeds=[], names=[])
        feed = item.origin and item.origin['feed']
        name = item.origin and item.origin['title']
        g.nodes[src.netloc]['feeds'].append(feed)
        g.nodes[src.netloc]['names'].append(name)

    def update_edges(self, g: nx.Graph, src: SplitResult, dests: Dict[SplitResult, KeywordCollection], item: FeedlyEntry):
        src_domain = src.netloc
        for dest, dest_info in dests.items():
            dest_domain = dest.netloc
            if not g.has_edge(src_domain, dest_domain):
                edge_info = {
                    'forward': dest_domain,
                    'hrefs': [],
                }
                g.add_edge(src_domain, dest_domain, **edge_info)
            else:
                edge_info = g[src_domain][dest_domain]
            if edge_info['forward'] == dest_domain:
                href = (1, utils.path_only(src), utils.path_only(dest), item.published.timestamp(), dest_info['tag'].pop())
            else:
                href = (0, utils.path_only(dest), utils.path_only(src), item.published.timestamp(), dest_info['tag'].pop())
            edge_info['hrefs'].append(href)
