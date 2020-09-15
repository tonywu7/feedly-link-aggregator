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
from collections import deque

from scrapy.extensions.logstats import LogStats


class LogStatsExtended(LogStats):
    def __init__(self, stats, interval=60.0):
        super().__init__(stats, interval=interval)
        self.logger = logging.getLogger('feedly.logstats')
        self.items: list
        self.history = {}
        self.window = 5

    def spider_opened(self, spider):
        self.items = getattr(spider, 'logstats_items', [])
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
