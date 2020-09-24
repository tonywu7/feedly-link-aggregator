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

import gzip
import logging
import signal
import time
from collections import deque
from datetime import datetime
from logging.handlers import QueueHandler
from multiprocessing import get_context
from multiprocessing.queues import Empty, Full
from pathlib import Path

import simplejson as json
from scrapy.exporters import JsonLinesItemExporter

from .sql.metadata import models, tables
from .sql.stream import DatabaseWriter
from .sql.utils import DatabaseVersionError
from .utils import LOG_LISTENER
from .utils import colored as _
from .utils import json_converters, watch_for_len, watch_for_timing

NULL_TERMINATE = {'\0': True}
ctx = get_context('forkserver')


class CompressedStreamExportPipeline:
    def open_spider(self, spider):
        self.counter = 0
        self.logger = logging.getLogger('pipeline.stream')
        self.output_dir: Path = spider.config['OUTPUT']
        self.compresslevel = spider.config.getint('STREAM_COMPRESSLEVEL', 9)
        date = datetime.now()
        date = f'{date.strftime("%y%m%d")}.{date.strftime("%H%M%S")}'
        path = self.output_dir / f'stream.{date}.jsonl.gz'
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
        self.db_path = self.output_dir / 'index.db'
        buffering = spider.config.getint('DATABASE_CACHE_SIZE', 100000)
        try:
            debug = spider.config.getbool('SQL_DEBUG')
        except ValueError:
            debug = spider.config.get('SQL_DEBUG')
        self.init_stream(debug, buffering)

    def init_stream(self, debug, buffering):
        self.stream = DatabaseWriter(self.db_path, tables, models, buffering, debug)

    def close_stream(self):
        self.stream.close()

    def process_item(self, data, spider):
        if 'item' in data:
            return self.process_entry(data)
        if 'source' in data:
            return self.process_feed_source(data)
        return data

    def process_entry(self, data):
        item = data['item']
        stream = self.stream

        src = item.url
        stream.write('url', {'url': src})
        for u, kws in item.hyperlinks.items():
            stream.write('url', {'url': u})
            stream.write('hyperlink', {'source_id': src, 'target_id': u, 'element': list(kws['tag'])[0]})

        feed = item.source
        stream.write('url', {'url': feed})
        stream.write('feed', {'url_id': feed, 'title': '', 'dead': None})

        stream.write('item', {
            'url': src,
            'source': item.source,
            'author': item.author,
            'title': item.title,
            'published': item.published.isoformat(),
            'updated': item.updated.isoformat() if item.updated else None,
            'crawled': data['time_crawled'],
        })

        for k in item.keywords:
            stream.write('keyword', {'keyword': k})
            stream.write('tagging', {'url_id': src, 'keyword_id': k})

        if item.markup:
            for k, v in item.markup.items():
                stream.write(k, {'url_id': src, 'markup': v})

    def process_feed_source(self, data):
        source = data['source']
        feed = source['url']
        self.stream.write('url', {'url': feed})
        self.stream.write('feed', {
            'url_id': feed,
            'title': source['title'],
            'dead': data['dead'],
        })

    def close_spider(self, spider):
        self.close_stream()


class DatabaseStorageProcess(ctx.Process):
    def __init__(
        self, db_path, item_queue: ctx.Queue, log_queue: ctx.Queue, err_queue: ctx.Queue,
        closing: ctx.Event, buffering, debug, *args, **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.db_path = db_path
        self.buffering = buffering
        self.debug = debug
        self.item_queue = item_queue
        self.log_queue = log_queue
        self.err_queue = err_queue
        self.closing = closing
        self.stream: DatabaseWriter
        self._strikes = 0

    def config_logging(self):
        handler = QueueHandler(self.log_queue)
        root = logging.getLogger()
        for h in root.handlers:
            root.removeHandler(h)
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)
        self.log = logging.getLogger('main')

    def accept_items(self):
        stream = self.stream
        while not self.closing.is_set():
            try:
                table, item = self.item_queue.get(timeout=.5)
            except Empty:
                pass
            else:
                stream.write(table, item)

    def deplete(self):
        stream = self.stream
        leftovers = []
        self.closing.set()
        try:
            while True:
                leftovers.append(self.item_queue.get(timeout=2))
        except Empty:
            self.log.debug('Queue depleted.')
            pass
        for table, item in leftovers:
            stream.write(table, item)

    def handle_sigint(self, *args, **kwargs):
        self._strikes += 1
        if self._strikes > 1:
            self.deplete()
        else:
            self.log.warn('Database writer process protected from SIGINT')
            self.log.warn('Send SIGINT again to force unclean shutdown.')
            self.log.warn(_('Sending SIGINT again may cause some records to be lost.', color='yellow'))

    def close(self):
        try:
            self.stream.close()
        except BaseException as e:
            self.err_queue.put_nowait(e)

    def run(self):
        signal.signal(signal.SIGINT, self.handle_sigint)

        self.config_logging()
        self.log.info(_('Starting database process', color='magenta'))
        self.log.info(_(f'Connected to database at {self.db_path}', color='magenta'))

        try:
            self.stream = DatabaseWriter(self.db_path, tables, models, buffering=self.buffering, debug=self.debug)
        except BaseException as e:
            self.closing.set()
            self.err_queue.put_nowait(e)
            return

        try:
            self.accept_items()
        except BaseException as e:
            self.err_queue.put_nowait(e)
        else:
            self.deplete()
        finally:
            self.log.info(_('Finalizing database', color='magenta'))
            self.closing.set()
            self.close()


class SQLiteExportProcessPipeline(SQLiteExportPipeline):

    class _WriterProxy:
        def __init__(self, queue, closing, log, maxsize):
            self.queue = queue
            self.closing = closing
            self.buffer = deque()
            self.maxsize = maxsize
            self.retry = 0
            self.retry_after = 0
            self.log = log

        def flush(self):
            buffer = self.buffer
            if not buffer:
                return
            try:
                while buffer:
                    item = buffer.popleft()
                    self.queue.put_nowait(item)
                self.retry = 0
            except Full:
                self.set_retry()
                if self.closing.is_set():
                    self.log.warn('Record discarded because writer process was terminated.')
                    buffer.clear()
                    return
                with watch_for_len('pending records', buffer, self.maxsize * .75):
                    buffer.appendleft(item)

        def write(self, *item):
            if (self.retry
                and (len(self.buffer) > self.maxsize
                     or self.retry + self.retry_after < int(time.time()))):
                self.buffer.append(item)
                return self.flush()
            try:
                self.queue.put_nowait(item)
            except Full:
                self.buffer.append(item)
                self.set_retry()

        def set_retry(self):
            if not self.retry:
                self.retry_after = 1
            elif self.retry_after < 64:
                self.retry_after = self.retry_after * 2
            self.retry = time.time()

        def close(self):
            while self.buffer:
                self.flush()

    def open_spider(self, spider):
        LOG_LISTENER.enable()

        self.closed = False
        self.log = logging.getLogger('pipeline.db')
        self.exception_handlers = {
            KeyboardInterrupt: lambda _: None,
            DatabaseVersionError: self.handle_version_error,
        }

        return super().open_spider(spider)

    def init_stream(self, debug, buffering):
        closing = ctx.Event()
        throw = ctx.Event()
        item_queue = ctx.Queue()
        err_queue = ctx.Queue()
        log_queue = LOG_LISTENER.start()

        self.process = DatabaseStorageProcess(
            self.db_path, item_queue, log_queue, err_queue,
            closing, buffering, debug, name='StorageProcess',
        )
        self.process.start()
        self.stream = self._WriterProxy(item_queue, closing, self.log, buffering * 2)
        self.closing = closing
        self.throw = throw
        self.err_queue = err_queue
        self.check_error()

    def check_error(self):
        if not self.err_queue.empty():
            error = self.err_queue.get_nowait()
            handler = self.exception_handlers.get(type(error))
            if handler:
                return handler(error)
            raise error

    def handle_version_error(self, exc):
        self.log.critical(exc)
        self.log.error(_('Cannot write to the existing database because it uses another schema version.', color='red'))
        self.log.error(_('Run `python -m feedly upgrade-db` to upgrade it to the current version.', color='cyan'))
        return 'incompatible_database'

    def process_item(self, data, spider):
        if self.closed:
            return data

        should_close = self.check_error()
        if should_close:
            spider.crawler.engine.close_spider(spider, should_close)
            self.close_stream()
            self.closed = True

        return super().process_item(data, spider)

    def close_stream(self):
        self.stream.close()
        self.closing.set()
        self.process.join()
        LOG_LISTENER.disable()
