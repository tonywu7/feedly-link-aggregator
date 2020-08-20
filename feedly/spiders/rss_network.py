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
from scrapy.utils.url import url_is_from_any_domain

from .rss_spider import FeedlyRSSSpider
from .. import utils
from ..datastructures import KeywordCollection
from ..feedly import FeedlyEntry


def graph_for_json(self):
    return node_link_data(self)


class SiteNetworkSpider(FeedlyRSSSpider):
    name = 'feed_network'

    custom_settings = utils.compose_mappings(FeedlyRSSSpider.custom_settings, {
        'SPIDER_MIDDLEWARES': {
            'feedly.spiders.rss_spider.FeedResourceMiddleware': None,
            'feedly.spiders.rss_spider.FeedEntryMiddleware': None,
            'feedly.spiders.rss_network.GraphExpansionMiddleware': 900,
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
    DEFAULT_CONFIG = {
        **FeedlyRSSSpider.DEFAULT_CONFIG,
        'depth': 0,
        'overwrite': True,
    }

    def __init__(self, name=None, **kwargs):
        super().__init__(name=name, **kwargs)
        self._depth = int(self._config['depth'])

        self._statspipeline_config = {
            'logstats': {
                'rss/node_count': 1000,
                'rss/page_count': 4096,
            },
            'autosave': 'rss/page_count',
        }

        domains = self._config.get('domains')
        if isinstance(domains, str):
            domains = set(domains.split(' '))
        elif isinstance(domains, List):
            domains = set(domains)
        self._domains = domains

        self.index: nx.Graph = nx.Graph()
        if self.output.exists():
            with open(self.output, 'r') as f:
                try:
                    gdata = json.load(f)
                    self.index = node_link_graph(gdata)
                except (json.JSONDecodeError, KeyError) as e:
                    raise ValueError(f'Cannot load graph data from existing file {self.output}') from e
        self.index.for_json = graph_for_json.__get__(self.index)

        _check_recommendations(self)

    def single_feed_only(self, feed, depth):
        if not feed or len(feed) > 1:
            return
        feed = feed[0]
        yield from self.next_page({'id': feed}, depth=depth, initial=True, callback=self.parse_feed)

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
        return cls(crawler)

    def __init__(self, crawler):
        self.stats = crawler.stats
        self._discovered = set()

    def process_spider_output(self, response: TextResponse, result: List[Union[FeedlyEntry, Request]], spider: SiteNetworkSpider):
        depth = response.meta.get('depth')
        if depth is not None and depth >= spider._depth:
            depth = None
        for item in result:
            if not isinstance(item, FeedlyEntry):
                yield item
                continue
            self.stats.inc_value('rss/page_count')
            yield from self.process_item(item, spider, depth)
            yield item

    def process_item(self, item: FeedlyEntry, spider: SiteNetworkSpider, depth=None):
        store = utils.HyperlinkStore()
        for k, v in item.markup.items():
            store.parse_html(item.url, v)

        src = urlsplit(item.url)
        dest = {urlsplit(k): v for k, v in store.items()}
        dest = {k: v for k, v in dest.items() if k.netloc}
        self.stats.inc_value('rss/resource_count', len(dest))
        if depth is not None:
            sites = [f'{u.scheme}://{u.netloc}' for u in dest]
            if spider._domains:
                sites = {u for u in sites if url_is_from_any_domain(u, spider._domains)} - self._discovered
                self._discovered |= sites
            for url in sites:
                spider.logger.info(f'Found possible RSS feed {url} (depth={depth + 1})')
                yield from spider.try_feed_urls(url, search_callback=spider.single_feed_only, meta={'depth': depth})

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
                g.add_node(d, feeds=[], names=[], keywords=[])
        feed = item.origin and item.origin['feed']
        name = item.origin and item.origin['title']
        src_node = g.nodes[src.netloc]
        src_node['feeds'].append(feed)
        src_node['names'].append(name)
        src_node['keywords'].extend(item.keywords)

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


def _check_recommendations(spider: SiteNetworkSpider):
    lines = []
    if json.dumps(spider.DEFAULT_CONFIG['templates']) == json.dumps(spider._config.get('templates')):
        if spider._fuzzy:
            lines.extend([
                '',
                'Fuzzy search is enabled and no URL template is provided.',
                '',
                'This means that spider will initiate a search',
                'via Feedly for almost all new nodes it discovers,',
                'which could quickly lead to rate-limiting.',
                "(Feedly's Search API is much more sensitive to high-volume requests.)",
                '',
                'Consider providing templates (via profiles) and disabling fuzzy search.',
                'See files under profiles/ for examples.',
            ])
        else:
            lines.extend([
                '',
                'No URL template is provided and fuzzy search is disabled.',
                '',
                'This means that spider will only check if the homepage of',
                'a website exists on Feedly as a valid RSS feed,',
                'which is almost never the case, and the number of nodes the spider',
                'can crawl will be greatly reduced.',
                '',
                'Consider providing templates (via profiles).',
                'See files under profiles/ for examples.',
            ])
    if not spider._domains:
        lines.extend([
            '',
            'No allowed domain is specified via a profile or the `-a domains` option.',
            '',
            'Spider will attempt to crawl every domain it has encountered,',
            'which may include CDN servers and unrelated sites (such as social media sites).',
            '',
            'Consider restricting which domains the spider can look for RSS feeds.',
        ])
    if lines:
        for line in lines:
            spider.logger.warn(line)
        spider.logger.warn('')
        spider.logger.warn(f'Crawler will start in {len(lines) // 3}s. Press CTRL-C to stop.')
        utils.wait(len(lines) // 3)
