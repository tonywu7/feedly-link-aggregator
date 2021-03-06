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

import re
from pathlib import Path
from typing import List
from urllib.parse import unquote

from scrapy.utils.url import add_http_if_no_scheme


def single_item(f):
    def wrapped(*args, **kwargs):
        return {f.__name__.upper(): f(*args, **kwargs)}
    return wrapped


class SettingsAdapter:
    @staticmethod
    def output(v):
        p = Path(v)
        return {'OUTPUT': p, 'JOBDIR': p / 'scheduled/jobs'}

    @staticmethod
    @single_item
    def rss(v):
        return add_http_if_no_scheme(unquote(v))

    @staticmethod
    @single_item
    def rss_templates(conf):
        return {re.compile(k): v for k, v in conf.items()}

    @staticmethod
    @single_item
    def follow_domains(domains):
        if isinstance(domains, str):
            domains = set(domains.split(' '))
        elif isinstance(domains, List):
            domains = set(domains)
        return domains
