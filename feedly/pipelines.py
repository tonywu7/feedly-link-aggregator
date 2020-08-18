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

import simplejson as json

from .feedly import FeedlyEntry
from .utils import json_converters

log = logging.getLogger('feedly.pipeline')


class PeriodicSavePipeline:
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        pipeline.stats = crawler.stats
        return pipeline

    def open_spider(self, spider):
        self.index = spider.index
        self.output = spider.output
        self._flush_watermark = spider._flush_watermark

    def close_spider(self, spider):
        self._flush()

    def _flush(self):
        log.info(
            f'Saving progress ... got {self.stats.get_value("rss/item_count")} items, '
            f'{self.stats.get_value("rss/resource_count")} external links',
        )
        with open(self.output.resolve(), 'w') as f:
            json.dump(
                self.index, f,
                ensure_ascii=False, default=json_converters, for_json=True,
                iterable_as_array=True,
            )

    def process_item(self, item: FeedlyEntry, spider):
        if (
            self._flush_watermark
            and self.stats.get_value('rss/item_count') - self.stats.get_value('rss/item_milestone') >= self._flush_watermark
        ):
            self._flush()
            self.stats.set_value('rss/item_milestone', self.stats.get_value('rss/item_count'))
        return item
