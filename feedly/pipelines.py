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
import gzip
import logging
import os
from logging.config import dictConfig
from pathlib import Path

import simplejson as json
from scrapy.exporters import JsonLinesItemExporter

from .logger import make_logging_config
from .utils import json_converters, watch_for_timing


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


class CompressedStreamExportPipeline:
    def open_spider(self, spider):
        self.logger = logging.getLogger('feedly.gzstream')
        self.output_dir: Path = spider.config['OUTPUT']
        path = self.output_dir.joinpath('stream.jsonl.gz')
        if path.exists():
            i = 1
            while self.output_dir.joinpath(f'stream.jsonl.gz.{i}').exists():
                i += 1
            path2 = self.output_dir.joinpath(f'stream.jsonl.gz.{i}')
            self.logger.info(f'Renaming existing {path.name} to {path2.name}')
            os.rename(path, path2)
        self.stream = gzip.open(path, 'at', encoding='utf8')
        self.init_exporter()

    def init_exporter(self):
        self.exporter = SimpleJSONLinesExporter(self.stream)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.stream.close()
        self.stream = gzip.open(self.stream.name, 'rt', encoding='utf8')
        spider.digest_feed_export(self.stream)
        self.stream.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item


class SimpleJSONLinesExporter(JsonLinesItemExporter):
    def __init__(self, file, **kwargs):
        super().__init__(file, **kwargs)
        self.encoder = json.JSONEncoder(
            ensure_ascii=True, default=json_converters,
            for_json=True, iterable_as_array=True,
        )

    def export_item(self, item):
        serialized = self.encoder.encode(item) + '\n'
        with watch_for_timing('Writing to stream', 0.01):
            self.file.write(serialized)
