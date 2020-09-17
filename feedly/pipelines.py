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
from datetime import datetime
from pathlib import Path

import simplejson as json
from scrapy.exporters import JsonLinesItemExporter

from . import _config_logging
from .utils import json_converters, watch_for_timing

NULL_TERMINATE = {'\0': True}


class ConfigLogging:
    @classmethod
    def from_crawler(cls, crawler):
        _config_logging()
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
        self.counter = 0
        self.logger = logging.getLogger('feedly.stream')
        self.output_dir: Path = spider.config['OUTPUT']
        self.compresslevel = spider.config.getint('STREAM_COMPRESSLEVEL', 9)
        date = datetime.now()
        date = f'{date.strftime("%y%m%d")}.{date.strftime("%H%M%S")}'
        path = self.output_dir.joinpath(f'stream.{date}.jsonl.gz')
        self.stream_path = path
        self.stream = gzip.open(path, 'at', encoding='utf8', compresslevel=self.compresslevel)
        self.init_exporter()

    def init_exporter(self):
        self.exporter = SimpleJSONLinesExporter(self.stream)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.stream.close()
        self.stream = gzip.open(self.stream.name, 'rt', encoding='utf8', compresslevel=self.compresslevel)
        if spider.config.getbool('PERSIST_TO_DB_ON_CLOSE', True):
            spider.digest_feed_export(self.stream)
        else:
            self.logger.warn('Scraped data have not been saved to database.')
            self.logger.warn('To save them, run `python -m feedly consume-leftovers`')
            date = f'{self.stream_path.suffixes[0]}.{self.stream_path.suffixes[1]}'
            unsaved = self.stream_path.with_name(f'stream~unsaved.{date}.jsonl.gz')
            self.stream_path.rename(unsaved)
        self.stream.close()

    def process_item(self, item, spider):
        if item is NULL_TERMINATE:
            self.stream.write('\0\n')
            return item
        self.counter += 1
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
        with watch_for_timing('Writing to stream', 0.1):
            self.file.write(serialized)
