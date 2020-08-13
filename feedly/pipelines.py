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

from scrapy.exceptions import DropItem

from .items import FeedlyEntry, HyperlinkStore
from .spiders.link_aggregator import FeedlyRssSpider
from .utils import JSONDict

log = logging.getLogger('feedly.pipeline')


class FeedlyItemPipeline:
    def process_item(self, item: JSONDict, spider: FeedlyRssSpider):
        try:
            entry = FeedlyEntry.from_upstream(item)
        except Exception as e:
            log.warn(exc_info=e)
            raise DropItem()

        spider.index['items'][entry.id_hash] = entry
        store: HyperlinkStore[str] = spider.index['resources']
        for k in {'content', 'summary'}:
            content = item.get(k)
            if content:
                content = content.get('content')
            if content:
                store.parse_html(entry.source, content)
                entry.markup[k] = content

        visual = item.get('visual')
        if visual:
            u = visual.get('url')
            if u and u != 'none':
                store.put(u, tag={'img'})

        return item


class SaveIndexPipeline:
    def process_item(self, item: JSONDict, spider: FeedlyRssSpider):
        if not spider._flush_limit:
            return item
        if len(spider.index['items']) - spider.stats.get_value('item_milestone') >= spider._flush_limit:
            spider._flush()
            spider.stats.set_value('item_milestone', len(spider.index['items']))
