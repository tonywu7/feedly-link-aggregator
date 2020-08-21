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

import cProfile
import logging
from logging.config import dictConfig

import simplejson as json

from .logger import make_logging_config
from .utils import json_converters

log = logging.getLogger('feedly.pipeline')


class ConfigLogging:
    @classmethod
    def from_crawler(cls, crawler):
        dictConfig(make_logging_config(
            'feedly',
            formatter_style='standard',
            formatter_colored=True,
            level=crawler.settings.get('LOG_LEVEL') or 20,
        ))
        return cls()

    def process_item(self, item, spider):
        return item


class CProfile:
    def __init__(self):
        self.pr = cProfile.Profile()

    def open_spider(self, spider):
        self.pr.enable()

    def close_spider(self, spider):
        self.pr.disable()
        self.pr.print_stats(sort='tottime')

    def process_item(self, item, spider):
        return item


class StatsPipeline:
    def __init__(self, crawler):
        self.stats = crawler.stats
        self.log = logging.getLogger('feedly.stats')

    @staticmethod
    def process(index):
        return index

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def open_spider(self, spider):
        self.milestones = getattr(spider, 'statspipeline_config', {'logstats': {}, 'autosave': None})

        self.index = spider.index
        self.output = spider.output
        if callable(getattr(spider, '_index_processor', None)):
            self.process = spider._index_processor

    def process_item(self, item, spider):
        report = False
        stats = {}
        for k, v in self.milestones['logstats'].items():
            s = self.stats.get_value(k, 0)
            stats[k] = s
            if s - self.stats.get_value(f'milestones/{k}', 0) >= v:
                self.stats.set_value(f'milestones/{k}', s)
                report = k
        if report:
            self.log.info('Statistics:')
            for k, v in stats.items():
                self.log.info(f'  {k}: {v}')
        autosave = self.milestones['autosave']
        if report == autosave:
            self._flush()
        return item

    def close_spider(self, spider):
        self._flush()

    def _flush(self):
        log.info('Saving progress ...')
        with open(self.output.resolve(), 'w') as f:
            json.dump(
                (self.process(self.index)), f,
                ensure_ascii=False, default=json_converters, for_json=True,
                iterable_as_array=True,
            )
