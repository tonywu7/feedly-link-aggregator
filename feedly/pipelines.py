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
from scrapy.exporters import JsonLinesItemExporter

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

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def open_spider(self, spider):
        self.milestones = getattr(spider, 'statspipeline_config', {})

    def process_item(self, item, spider):
        report = False
        stats = self.stats
        milestones = self.milestones
        items = milestones.items
        values = {k: stats.get_value(k, 0) for k, v in items()}
        diff = {k: v // milestones[k] - stats.get_value(f'milestones/{k}', 0) for k, v in values.items()}
        for k, v in diff.items():
            if v:
                report = k
                stats.inc_value(f'milestones/{k}')
                break
        if report:
            self.log.info('Statistics:')
            for k, v in values.items():
                self.log.info(f'  {k}: {v}')
        return item


class FeedlyEntryExportPipeline:
    def open_spider(self, spider):
        self.output = spider.config['OUTPUT']
        self.file = open(self.output.with_suffix('.json.tmp'), 'a+')
        self.exporter = SimpleJSONLinesExporter(self.file)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.flush()
        self.file.seek(0)
        spider.logger.info(f'Saving feed digest to {self.output} ...')
        with open(self.output, 'w') as f:
            f.write(spider.digest_feed_export(self.file))
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item


class SimpleJSONLinesExporter(JsonLinesItemExporter):
    def __init__(self, file, **kwargs):
        super().__init__(file, **kwargs)
        self.encoder = json.JSONEncoder(
            ensure_ascii=True, default=json_converters,
            for_json=True, iterable_as_array=True)

    def export_item(self, item):
        serialized = self.encoder.encode(item) + '\n'
        self.file.write(serialized)
