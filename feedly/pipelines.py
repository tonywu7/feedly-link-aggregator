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
from collections import deque
from contextlib import suppress
from datetime import datetime
from logging.handlers import QueueHandler
from multiprocessing import Event, Process, Queue
from multiprocessing.queues import Empty, Full
from pathlib import Path

import simplejson as json
from scrapy.exporters import JsonLinesItemExporter

from .feedly import FeedlyEntry
from .sql.metadata import models, tables
from .sql.stream import DatabaseWriter
from .utils import LOG_LISTENER
from .utils import colored as _
from .utils import json_converters, watch_for_timing

NULL_TERMINATE = {'\0': True}


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
        self.logger = logging.getLogger('pipeline.stream')
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


class SQLiteExportPipeline:
    def open_spider(self, spider):
        self.output_dir: Path = spider.config['OUTPUT']
        self.db_path = self.output_dir.joinpath('index.db')
        try:
            self.debug = spider.config.getbool('SQL_DEBUG')
        except ValueError:
            self.debug = spider.config.get('SQL_DEBUG')
        self.init_stream()

    def init_stream(self):
        self.stream = DatabaseWriter(self.db_path, tables, models, debug=self.debug)

    def close_stream(self):
        self.stream.close()

    def process_item(self, data, spider):
        if 'item' not in data:
            return data
        item: FeedlyEntry = data['item']
        stream = self.stream

        src = item.url
        stream.write('url', {'url': src})
        for u, kws in item.hyperlinks.items():
            stream.write('url', {'url': u})
            stream.write('hyperlink', {'source_id': src, 'target_id': u, 'element': list(kws['tag'])[0]})

        feed = item.source['feed']
        stream.write('url', {'url': feed})
        stream.write('feed', {'url_id': feed, 'title': item.source.get('title', '')})

        stream.write('item', {
            'hash': item.id_hash,
            'url': item.url,
            'source': item.source['feed'],
            'author': item.author,
            'title': item.title,
            'published': item.published.isoformat(),
            'updated': item.updated.isoformat() if item.updated else None,
            'crawled': data['time_crawled'],
        })

        for k in item.keywords:
            stream.write('keyword', {'keyword': k})
            stream.write('tagging', {'item_id': item.id_hash, 'keyword_id': k})

        if item.markup:
            for k, v in item.markup.items():
                stream.write(k, {'url_id': src, 'markup': v})

    def close_spider(self, spider):
        self.close_stream()


class DatabaseStorageProcess(Process):
    def __init__(
        self, db_path, item_queue: Queue, log_queue: Queue, err_queue: Queue,
        closing: Event, debug=False, *args, **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.db_path = db_path
        self.debug = debug
        self.item_queue = item_queue
        self.log_queue = log_queue
        self.err_queue = err_queue
        self.closing = closing

    def config_logging(self):
        handler = QueueHandler(self.log_queue)
        root = logging.getLogger()
        for h in root.handlers:
            root.removeHandler(h)
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)
        self.log = logging.getLogger('main')

    def accept_items(self, stream):
        while not self.closing.is_set():
            with suppress(KeyboardInterrupt, SystemExit):
                try:
                    table, item = self.item_queue.get(timeout=.5)
                except Empty:
                    pass
                else:
                    stream.write(table, item)

    def deplete(self, stream):
        leftovers = []
        self.closing.set()
        try:
            while True:
                leftovers.append(self.item_queue.get(timeout=5))
        except Empty:
            self.log.debug('Queue depleted.')
            pass
        for table, item in leftovers:
            stream.write(table, item)

    def run(self):
        self.config_logging()
        self.log.info(_('Starting database process', color='magenta'))
        self.log.info(_(f'Connected to database at {self.db_path}', color='magenta'))
        stream = DatabaseWriter(self.db_path, tables, models, debug=self.debug)

        try:
            self.accept_items(stream)
            self.deplete(stream)

        except BaseException as e:
            self.log.error(e, exc_info=True)
            self.log.error('Database writer process encountered an exception.')
            self.err_queue.put_nowait(e)

        finally:
            self.log.info(_('Finalizing database', color='magenta'))
            stream.close()


class SQLiteExportProcessPipeline(SQLiteExportPipeline):

    class _WriterProxy:
        def __init__(self, queue, closing, log, bufsize=5):
            self.queue = queue
            self.closing = closing
            self.buffer = deque()
            self.bufsize = bufsize
            self.log = log

        def flush(self):
            buffer = self.buffer
            if not buffer:
                return
            try:
                while buffer:
                    item = buffer.popleft()
                    self.queue.put_nowait(item)
            except Full:
                if self.closing.is_set():
                    self.log.warn('Record discarded because writer process has terminated.')
                    buffer.clear()
                    return
                buffer.appendleft(item)

        def write(self, *args):
            self.buffer.append(args)
            if len(self.buffer) >= self.bufsize:
                self.flush()

        def close(self):
            while self.buffer:
                self.flush()

    def open_spider(self, spider):
        LOG_LISTENER.enable()
        self.log = logging.getLogger('pipeline.db')
        return super().open_spider(spider)

    def init_stream(self):
        item_queue = Queue()
        err_queue = Queue()
        log_queue = LOG_LISTENER.start()
        closing = Event()

        self.process = DatabaseStorageProcess(
            self.db_path, item_queue, log_queue, err_queue,
            closing, self.debug, name='StorageProcess',
        )
        self.process.start()
        self.stream = self._WriterProxy(item_queue, closing, self.log)
        self.closing = closing
        self.err_queue = err_queue

    def process_item(self, data, spider):
        if not self.err_queue.empty():
            error = self.err_queue.get()
            with suppress(KeyboardInterrupt, SystemExit):
                raise error
        return super().process_item(data, spider)

    def close_stream(self):
        self.stream.close()
        self.closing.set()
        self.process.join()
