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

import atexit
import gzip
import logging
import os
import pickle
import shutil
import time
from collections import defaultdict, deque
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import suppress
from datetime import datetime, timedelta
from importlib.util import module_from_spec, spec_from_file_location
from itertools import permutations
from pathlib import Path
from statistics import mean, mode
from threading import Event, Thread

import simplejson as json
from scrapy.crawler import Crawler
from scrapy.exceptions import NotConfigured
from scrapy.extensions.logstats import LogStats
from scrapy.settings import BaseSettings
from scrapy.signals import (engine_stopped, request_dropped,
                            request_reached_downloader, request_scheduled,
                            spider_closed, spider_opened)
from twisted.internet import task

from .datastructures import compose_mappings
from .docs import OptionsContributor
from .requests import reconstruct_request
from .signals import (register_state, request_finished, resume_requests,
                      show_stats)
from .spiders.settings import SettingsAdapter
from .utils import colored as _
from .utils import fmttimedelta, sha1sum


class _LoggingHelper:
    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('VERBOSE', False):
            logging.getLogger('scrapy.middleware').disabled = True
        return cls()


class PresetLoader:
    @classmethod
    def from_crawler(cls, crawler: Crawler):
        import re

        settings: BaseSettings = crawler.settings
        if 'PRESET' in settings or 'preset' in settings:
            raise NotConfigured()
        if not settings.getbool('AUTO_LOAD_PREDEFINED_PRESETS', True):
            raise NotConfigured()

        presets_dir = Path(__file__).parent.with_name('presets')
        if not presets_dir.exists():
            raise NotConfigured()
        auto_load = presets_dir / '_autoload.py'
        if not auto_load.exists():
            raise NotConfigured()

        try:
            sites = {}
            SettingsLoader.from_pyfile(sites, auto_load)
            sites = sites['_SITES']
        except (OSError, ImportError, KeyError):
            raise NotConfigured()

        feed = settings['RSS'] or settings['rss']
        if not feed:
            raise NotConfigured()

        preset = None
        for r, p in sites.items():
            if re.match(r, feed):
                preset = p
                break
        if not preset:
            raise NotConfigured()

        preset = presets_dir / f'{preset}.py'
        if not preset.exists():
            raise NotConfigured()

        settings['PRESET'] = preset
        logging.getLogger('extensions.autoload').info(
            _(f'Auto-loaded preset {preset.relative_to(presets_dir)} '
              'based on the provided feed URL.', color='cyan'),
        )

        return cls()


class SettingsLoader:
    @classmethod
    def from_crawler(cls, crawler: Crawler):
        base_settings: BaseSettings = crawler.settings
        cls.normalize(base_settings)

        settings = BaseSettings(priority='spider')
        cls.from_object(settings, crawler.spidercls.SpiderConfig)
        settings.update({k: v for k, v in base_settings.items() if k in settings},
                        priority='cmdline')

        preset = base_settings.get('PRESET')
        if preset:
            preset_dict = BaseSettings(priority=35)
            cls.from_pyfile(preset_dict, preset)
            settings.update(preset_dict)

        adapted = BaseSettings(priority=50)
        for k, v in settings.items():
            adapt = getattr(SettingsAdapter, k.lower(), None)
            if adapt:
                adapted.update(adapt(v))
            else:
                adapted[k] = v

        base_settings.update(adapted.copy_to_dict(), priority=50)
        base_settings['SPIDER_CONFIG'] = adapted
        return cls()

    @classmethod
    def normalize(cls, settings):
        to_upper = {}
        for k, v in settings.items():
            if not k.isupper():
                to_upper[k] = v
        for k, v in to_upper.items():
            settings[k.upper()] = v
            del settings[k]

    @classmethod
    def from_json(cls, settings, path):
        with open(path) as f:
            cls.merge(settings, json.load(f))

    @classmethod
    def from_pyfile(cls, settings, path):
        spec = spec_from_file_location('feedme.user_preset', path)
        mod = module_from_spec(spec)
        spec.loader.exec_module(mod)
        cls.from_object(settings, mod)

    @classmethod
    def from_object(cls, settings, obj):
        keys = dir(obj)
        cls.merge(settings, {k: getattr(obj, k) for k in keys if k.isupper()})

    @classmethod
    def merge(cls, settings, other):
        d = compose_mappings(settings, other)
        settings.clear()
        settings.update(d)


class GlobalPersistence:
    @classmethod
    def from_crawler(cls, crawler):
        instance = cls(crawler)
        crawler.signals.connect(instance.register, register_state)
        crawler.signals.connect(instance.resume_crawl, resume_requests)
        crawler.signals.connect(instance.freeze_request, request_scheduled)
        crawler.signals.connect(instance.request_done, request_dropped)
        crawler.signals.connect(instance.request_done, request_finished)
        crawler.signals.connect(instance.close, spider_closed)
        crawler.signals.connect(instance.close, engine_stopped)
        return instance

    def __init__(self, crawler):
        self.signals = crawler.signals
        self.logger = logging.getLogger('worker.persistence')

        settings = crawler.settings
        self.settings = settings

        output = settings['OUTPUT']
        os.makedirs(output, exist_ok=True)

        self._jobdir = settings['JOBDIR']
        self.path_state = output / 'program.state'
        self.path_opts = output / 'options.json'
        self.path_archive = output / 'scheduled' / 'freezer'

        self.closing = Event()
        self.thread = Thread(None, target=self.worker, name='RequestPersistenceThread',
                             args=(self.closing,), daemon=True)
        self.future = None

        self.freezer = RequestFreezer(self.path_archive)
        self.thread.start()

        self.state = self.load_state()

        atexit.register(self.archive)
        atexit.register(self.close)
        atexit.register(self.rmjobdir)
        self.dump_opts()

    def worker(self, closing: Event):
        while not closing.wait(20):
            try:
                self.archive()
            except Exception as e:
                self.logger.error(e, exc_info=True)
        self.archive()

    def archive(self):
        self.freezer.flush()
        self.dump_state()

    def load_state(self):
        with suppress(FileNotFoundError, EOFError, gzip.BadGzipFile):
            with gzip.open(self.path_state, 'rb') as f:
                return pickle.load(f)
        return {}

    def dump_state(self):
        with gzip.open(self.path_state, 'wb+') as f, suppress(RuntimeError):
            pickled = pickle.dumps(self.state)
            f.write(pickled)

    def dump_opts(self):
        if 'CMDLINE_ARGS' not in self.settings:
            return
        with open(self.path_opts, 'w+') as f:
            json.dump(self.settings['CMDLINE_ARGS'], f)

    def register(self, obj, namespace, attrs):
        for attr in attrs:
            key = f'{namespace}.{attr}'
            if key in self.state:
                setattr(obj, attr, self.state[key])
                continue
            self.state[key] = getattr(obj, attr)

    def freeze_request(self, request, spider=None):
        request.meta['_time_scheduled'] = time.time()
        self.freezer.add(request)

    def request_done(self, request, spider=None):
        self.freezer.remove(request)

    def resume_crawl(self, spider):
        if self.path_archive.exists():
            self.logger.info('Restoring persisted requests...')
        spider.freezer = self.freezer

    def close(self, spider=None, reason=None):
        if self.closing.is_set():
            return
        self.closing.set()
        self.thread.join(2)
        self.archive()
        num_requests = len(self.freezer)
        if num_requests:
            self.logger.info(_(f'# of requests persisted to filesystem: {num_requests}', color='cyan'))

    def rmjobdir(self):
        with suppress(Exception):
            shutil.rmtree(self._jobdir)


class RequestFreezer:
    def __init__(self, path):
        self.wd = Path(path)
        self.path = self.wd / 'frozen'
        os.makedirs(self.path, exist_ok=True)
        self.buffer = deque()

    def add(self, request):
        key = request.meta.get('pkey')
        if not key:
            return
        for_pickle = {
            'url': request.url,
            'method': request.method,
            'callback': request.callback.__name__,
            'meta': {**request.meta},
            'priority': request.priority,
        }
        self.buffer.append(('add', key, (request.__class__, for_pickle)))

    def remove(self, request):
        key = request.meta.get('pkey')
        if not key:
            return
        self.buffer.append(('remove', key, None))

    def flush(self):
        shelves = {}
        buffer = self.buffer
        self.buffer = deque()
        for action, key, item in buffer:
            hash_ = sha1sum(pickle.dumps(key))
            label = hash_[:2]
            shelf = shelves.get(label)
            if not shelf:
                shelf = shelves[label] = self.open_shelf(label)
            if action == 'add':
                shelf[hash_] = item
            if action == 'remove':
                shelf.pop(hash_, None)
        self.persist(shelves)
        del shelves
        del buffer

    def open_shelf(self, shelf, path=None):
        path = path or self.path / shelf
        try:
            with gzip.open(path) as f:
                return pickle.load(f)
        except Exception:
            return {}

    def persist(self, shelves, path=None):
        path = path or self.path
        for shelf, items in shelves.items():
            shelf = path / shelf
            with gzip.open(shelf, 'wb') as f:
                pickle.dump(items, f)

    def copy(self, src, dst):
        def cp(shelf):
            srcd = self.open_shelf(shelf, src / shelf)
            if not srcd:
                return
            dstd = self.open_shelf(shelf, dst / shelf)
            dstd.update(srcd)
            self.persist({shelf: dstd}, dst)

        with ThreadPoolExecutor(max_workers=32) as executor:
            executor.map(cp, [i + j for i, j in self.names()])

    def defrost(self, spider):
        info = self.load_info()
        defroster_path = self.wd / 'defrosting'
        if defroster_path.exists():
            self.copy(self.path, defroster_path)
            shutil.rmtree(self.path)
        else:
            shutil.move(self.path, defroster_path)
        os.makedirs(self.path)
        self.dump_info(info)

        defroster = RequestDefroster(defroster_path)
        for cls, kwargs in defroster:
            yield reconstruct_request(cls, spider, **kwargs)
        shutil.rmtree(defroster_path, ignore_errors=True)

    def clear(self):
        shutil.rmtree(self.path)
        self.path.mkdir()

    def load_info(self):
        info = {}
        with suppress(EOFError, FileNotFoundError,
                      json.JSONDecodeError, gzip.BadGzipFile):
            with open(self.path / 'info.json') as f:
                return json.load(f)
        return info

    def dump_info(self, info):
        with open(self.path / 'info.json', 'w+') as f:
            json.dump(info, f)

    def names(self):
        return permutations('0123456789abcdef', 2)

    def __len__(self):
        length = 0
        for i, j in self.names():
            shelf = i + j
            length += len(self.open_shelf(shelf))
        return length

    def iter_keys(self):
        for i, j in self.names():
            shelf = i + j
            shelf = self.open_shelf(shelf)
            yield from shelf


class RequestDefroster(RequestFreezer):
    def __init__(self, path):
        self.path = Path(path)

    def __iter__(self):
        for i, j in self.names():
            name = i + j
            shelf = self.open_shelf(name)
            yield from shelf.values()
            with suppress(FileNotFoundError):
                os.unlink(self.path / name)


class ContribMiddleware(OptionsContributor):
    @classmethod
    def from_crawler(cls, crawler):
        contrib_cls = crawler.settings.get('CONTRIB_SPIDER_MIDDLEWARES', {})
        if not contrib_cls:
            raise NotConfigured()

        contrib_cls = sorted(contrib_cls.items(), key=lambda t: t[1])
        order = 400
        normalized = {}
        for k, v in contrib_cls:
            normalized[k] = order
            if order < 499:
                order += 1

        spider_mdws = crawler.settings['SPIDER_MIDDLEWARES']
        spider_mdws.update(normalized)
        return cls()

    @staticmethod
    def _help_options():
        return {
            'CONTRIB_SPIDER_MIDDLEWARES': """
            Enable additional spider middlewares.

            Specified in the same way as Scrapy's SPIDER_MIDDLEWARE setting.
            Middlewares specified here will have a priority > 400 and < 500.

            For example, to enable the provided KeywordPrioritizer, do:

                `CONTRIB_SPIDER_MIDDLEWARES = {`
                    `'feedme.contrib.filters.KeywordPrioritizer': 500`
                `}`
            """,
        }


class RequestMetrics:
    topics = [
        'scheduled/priority/low', 'scheduled/priority/mean',
        'scheduled/priority/mode', 'scheduled/priority/high',
        'scheduled/age/average', 'scheduled/age/oldest', 'scheduled/age/newest',
        'inprogress/priority/low', 'inprogress/priority/mean',
        'inprogress/priority/mode', 'inprogress/priority/high',
        'inprogress/age/average', 'inprogress/age/oldest', 'inprogress/age/newest',
    ]

    @classmethod
    def from_crawler(cls, crawler):
        instance = cls(crawler.settings, crawler.stats)
        crawler.signals.connect(instance.spider_opened, spider_opened)
        crawler.signals.connect(instance.update_scheduled, request_scheduled)
        crawler.signals.connect(instance.update_inprogress, request_reached_downloader)
        crawler.signals.send_catch_log(show_stats, names=cls.topics, namespace='Request stats')
        return instance

    def __init__(self, settings, stats):
        maxlen = settings.getint('METRICS_ROLLING_WINDOW', 64)
        interval = settings.getfloat('METRICS_CALC_INTERVAL')
        if not interval:
            raise NotConfigured()
        self.interval = interval
        self.stats = stats
        self.scheduled_prio = deque(maxlen=maxlen)
        self.scheduled_time = deque(maxlen=maxlen)
        self.inprogress_prio = deque(maxlen=maxlen)
        self.inprogress_time = deque(maxlen=maxlen)

    def spider_opened(self, spider):
        self.task = task.LoopingCall(self.update, spider)
        self.task.start(self.interval)

    def update_scheduled(self, request, spider=None):
        self.scheduled_prio.append(request.priority)
        request.meta.setdefault('_time_scheduled', time.time())
        self.scheduled_time.append(request.meta['_time_scheduled'])

    def update_inprogress(self, request, spider=None):
        self.inprogress_prio.append(request.priority)
        request.meta.setdefault('_time_scheduled', time.time())
        self.inprogress_time.append(request.meta['_time_scheduled'])

    def calc_diff(self, q):
        if not q:
            return 0, 0, 0, 0
        return min(q), max(q), mean(q), mode(q)

    def calc_time(self, q):
        now = datetime.now()
        if not q:
            return timedelta(), timedelta(), timedelta()
        ages = [now.timestamp() - v for v in q]
        oldest = timedelta(seconds=max(ages))
        newest = timedelta(seconds=min(ages))
        avg_age = timedelta(seconds=mean(ages))
        return oldest, newest, avg_age

    def update(self, spider=None):
        min_sprio, max_sprio, avg_sprio, mode_sprio = self.calc_diff(self.scheduled_prio)
        min_iprio, max_iprio, avg_iprio, mode_iprio = self.calc_diff(self.inprogress_prio)
        oldest_s, newest_s, avg_age_s = self.calc_time(self.scheduled_time)
        oldest_i, newest_i, avg_age_i = self.calc_time(self.inprogress_time)
        stats = zip(self.topics, [
            min_sprio, avg_sprio, mode_sprio, max_sprio,
            avg_age_s, oldest_s, newest_s,
            min_iprio, avg_iprio, mode_iprio, max_iprio,
            avg_age_i, oldest_i, newest_i,
        ])
        for k, v in stats:
            self.stats.set_value(k, v)


class LogStatsExtended(LogStats):
    @classmethod
    def from_crawler(cls, crawler):
        instance = super().from_crawler(crawler)
        crawler.signals.connect(instance.add_stats, show_stats)
        instance.crawler = crawler
        if crawler.settings.getbool('LOG_VIOLATIONS', False):
            logging.getLogger('profiler').setLevel(logging.CRITICAL)
        return instance

    def __init__(self, stats, interval=60.0):
        super().__init__(stats, interval=interval)
        self.logger = logging.getLogger('logstats')
        self.items = defaultdict(list)
        self.history = {}
        self.width = 0
        self.window = 5
        self.add_stats(['response_received_count',
                        'requests_in_queue',
                        'requests_in_progress'],
                       namespace='Request stats')

    def spider_opened(self, spider):
        self.task = task.LoopingCall(self.log, spider)
        self.task.start(self.interval, now=False)

    def add_stats(self, names, namespace='Scraping stats'):
        ns = self.items[namespace]
        ns.extend(names)
        ns.sort()
        width = max(len(s) for s in ns) + 1
        self.width = max(width, self.width)
        self.history.update({k: deque(maxlen=self.window) for k in names})

    def _log(self, names, values, rates):
        converters = {
            float: lambda f: round(f, 2),
            timedelta: fmttimedelta,
            datetime: datetime.isoformat,
        }

        for k in names:
            n = f'{k}:'.ljust(self.width)
            v = values[k]
            v = converters.get(type(v), lambda v: v)(v)
            if k in rates:
                self.logger.info(f'  {n} {v} ({rates[k]:+.1f}/min)')
            else:
                self.logger.info(f'  {n} {v}')

    def log(self, spider):
        self.stats.set_value('requests_in_queue', len(self.crawler.engine.slot.scheduler))
        self.stats.set_value('requests_in_progress', len(self.crawler.engine.slot.inprogress))

        values = self.stats.get_stats()
        values = {k: values.get(k, 0) for ns in self.items.values() for k in ns}
        rates = {}

        for k, v in values.items():
            if not isinstance(v, (int, float)):
                continue
            history = self.history[k]
            history.append(v)
            if len(history) > 1:
                rates[k] = (history[-1] - history[0]) / (len(history) - 1)

        self.logger.info('')
        for k, n in self.items.items():
            self.logger.info(f'{k}:')
            self._log(n, values, rates)
            self.logger.info('')
