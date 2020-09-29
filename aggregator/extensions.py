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
import cProfile
import gzip
import logging
import pickle
from collections import deque
from contextlib import suppress
from importlib.util import module_from_spec, spec_from_file_location

import simplejson as json
from scrapy.crawler import Crawler
from scrapy.exceptions import NotConfigured
from scrapy.extensions.logstats import LogStats
from scrapy.settings import BaseSettings
from scrapy.signals import spider_closed, spider_opened

from .datastructures import compose_mappings
from .docs import OptionsContributor
from .signals import register_state
from .spiders.settings import SettingsAdapter


class SettingsLoader:
    @classmethod
    def from_crawler(cls, crawler: Crawler):
        base_settings: BaseSettings = crawler.settings
        cls.normalize(base_settings)

        settings = {}
        cls.from_object(settings, crawler.spidercls.SpiderConfig)
        settings.update({k: v for k, v in base_settings.items() if k in settings})
        preset = base_settings.get('PRESET')
        if preset:
            cls.from_pyfile(settings, preset)

        adapted = BaseSettings(priority='cmdline')
        for k, v in settings.items():
            adapt = getattr(SettingsAdapter, k.lower(), None)
            if adapt:
                adapted.update(adapt(v), 'cmdline')
            else:
                adapted[k] = v

        base_settings.update(adapted, priority='cmdline')
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
        spec = spec_from_file_location('aggregator.user_preset', path)
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
        instance = cls(crawler.settings)
        crawler.signals.connect(instance.register, register_state)
        return instance

    def __init__(self, settings):
        self.output = settings['OUTPUT'] / 'program.state'
        self.data = self.load()
        atexit.register(self.dump)

    def load(self):
        with suppress(FileNotFoundError, EOFError, gzip.BadGzipFile):
            with gzip.open(self.output, 'rb') as f:
                return pickle.load(f)
        return {}

    def dump(self):
        with gzip.open(self.output, 'wb+') as f:
            pickle.dump(self.data, f)

    def register(self, obj, attr):
        if attr in self.data:
            return setattr(obj, attr, self.data[attr])
        self.data = getattr(obj, attr)


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
                    `'aggregator.contrib.filters.KeywordPrioritizer': 500`
                `}`
            """,
        }


class LogStatsExtended(LogStats):
    def __init__(self, stats, interval=60.0):
        super().__init__(stats, interval=interval)
        self.logger = logging.getLogger('logstats')
        self.items: list
        self.history = {}
        self.window = 5

    def spider_opened(self, spider):
        self.items = getattr(spider, 'LOGSTATS_ITEMS', [])
        self.items.extend(['response_received_count'])
        self.items.sort()
        super().spider_opened(spider)

    def log(self, spider):
        values = self.stats.get_stats()
        values = {k: values.get(k, 0) for k in self.items}
        rates = {}

        for k, v in values.items():
            if not isinstance(v, (int, float)):
                continue
            history = self.history.setdefault(k, deque())
            history.append(v)
            if len(history) > self.window:
                history.popleft()
            if len(history) > 1:
                rates[k] = (history[-1] - history[0]) / (len(history) - 1)

        self.logger.info('Statistics:')
        for k, v in values.items():
            if k in rates:
                self.logger.info(f'  {k}: {v} ({rates[k]:.1f}/min)')
            else:
                self.logger.info(f'  {k}: {v}')


class CProfile:
    @classmethod
    def from_crawler(cls, crawler):
        instance = cls(crawler.settings.get('CPROFILE_OUTPUT'))
        crawler.signals.connect(instance.open_spider, spider_opened)
        crawler.signals.connect(instance.close_spider, spider_closed)
        return instance

    def __init__(self, path=None):
        if not path:
            raise NotConfigured()
        self.pr = cProfile.Profile()
        self.path = path

    def open_spider(self, spider):
        self.pr.enable()

    def close_spider(self, spider):
        self.pr.disable()
        self.pr.dump_stats(self.path)
