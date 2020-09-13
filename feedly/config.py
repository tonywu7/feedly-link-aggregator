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

import json
import re
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from scrapy.settings import BaseSettings

from .datastructures import compose_mappings


class Config(BaseSettings):
    def from_json(self, path):
        with open(path) as f:
            self.merge(json.load(f))

    def from_pyfile(self, path):
        spec = spec_from_file_location('feedly.user_preset', path)
        mod = module_from_spec(spec)
        spec.loader.exec_module(mod)
        self.from_object(mod)

    def from_object(self, obj):
        keys = dir(obj)
        self.merge({k: getattr(obj, k) for k in keys if k.isupper()})

    def merge(self, other):
        d = compose_mappings(self, other)
        self.clear()
        self.update(d)

    def get_namespace(self, prefix):
        length = len(prefix)
        d = Config({k[length:].lower(): v for k, v in self.items() if k[:length] == prefix})
        return d


transformers = {
    'OUTPUT': lambda text: Path(text),
    'FEED_TEMPLATES': lambda text: {re.compile(k): v for k, v in json.loads(text)},
}
