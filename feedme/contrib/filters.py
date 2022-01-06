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
import re
from math import inf
from urllib.parse import urlsplit

from scrapy.exceptions import NotConfigured

from ..docs import OptionsContributor
from ..requests import ProbeFeed
from ..signals import register_state, start_from_scratch


class KeywordPrioritizer(OptionsContributor, _doc_order=-5):
    """
    Enable this Spider Middleware to (de)prioritize certain feeds based on keywords.

    When using the cluster spider, changing the priorities of requests will shift
    the overall direction the spider is going, by causing some feeds to be crawled sooner
    than others.
    """

    @classmethod
    def from_crawler(cls, crawler):
        instance = cls(crawler.settings)
        crawler.signals.send_catch_log(
            register_state, obj=instance,
            namespace='kwprioritizer', attrs=['priorities'],
        )
        crawler.signals.connect(instance.clear_state_info, start_from_scratch)
        return instance

    def __init__(self, settings):
        self.log = logging.getLogger('contrib.keywordprioritizer')
        weighted_kws = settings.get('PRIORITIZED_KEYWORDS', {})
        if not weighted_kws:
            raise NotConfigured()

        self.keywords = {p: re.compile(r'(?:%s)' % '|'.join(kws), re.IGNORECASE)
                         for p, kws in weighted_kws.items()}
        self.keywords_fullword = {p: re.compile(r'\b(?:%s)\b' % '|'.join(kws), re.IGNORECASE)
                                  for p, kws in weighted_kws.items()}

        self.priorities = {}
        self.starting_weight = 0

    def clear_state_info(self):
        self.priorities.clear()

    def update_priority(self, item, source, target):
        prios = self.priorities
        starting = self.priorities.setdefault(source, self.starting_weight)
        prio = self.priorities.setdefault(target, self.starting_weight + starting)
        if prio is None:
            return True
        delta = 0

        for p, r in self.keywords.items():
            s = r.search(target)
            if not s:
                continue
            delta += p
            self.log.debug(f'{source} {target} {s.group(0)} {p}')
            if delta == -inf:
                break

        if delta == -inf:
            prios[target] = -inf
            return

        phrases = list(item.keywords)
        phrases.extend([item.markup.get('summary', ''), item.title])
        phrases = ' '.join(phrases)
        for p, r in self.keywords_fullword.items():
            s = r.search(phrases)
            if not s:
                continue
            delta += p
            self.log.debug(f'{source} {target} {s.group(0)} {p}')
            if delta == -inf:
                break

        prios[target] = prio + delta

    def process_spider_output(self, response, result, spider):
        for res in result:
            if not isinstance(res, ProbeFeed):
                yield res
                continue

            item = res.meta.get('source_item')
            feed_url = res.meta.get('feed_url')
            if not item or not feed_url:
                yield res
                continue

            source = urlsplit(item.url).netloc
            target = urlsplit(feed_url).netloc
            self.update_priority(item, source, target)

            prio = self.priorities.get(target, 0)
            if prio == -inf:
                continue
            if not prio:
                yield res
                continue

            yield res.replace(priority=res.priority + prio)

    @staticmethod
    def _help_options():
        return {
            'PRIORITIZED_KEYWORDS': """
            A mapping of weights to a list of keywords.

            Before a new feed is crawled, the crawling request is processed here. This
            middleware will then search the text content from which this new feed is
            discovered, such as keywords and HTML markups, and adjust the priority of
            the request accordingly.

            A ~positive~ weight will increase the priority, causing the feed to be crawled
            sooner. A ~negative~ weight will decrease the priority.

            If you use the special `-inf` (negative infinity) value, the new feed will
            be dropped. (`inf` can be imported from `math` or specified as `float('inf')`).

            **Example**

                `PRIORITIZED_KEYWORDS = {`
                `    10: ['cats', 'kitties'],`
                `    5: ['dogs', 'puppies'],`
                `    -5: ['goldfish'],`
                `    -float('inf'): ['rat'],`
                `}`
            """,
        }
