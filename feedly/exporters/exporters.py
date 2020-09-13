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

import csv
import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path

import simplejson as json


class MappingExporter(ABC):
    def __init__(self, output, filename, escape=None):
        self.output = output
        self.filename = filename
        self.ext = ''.join(Path(filename).suffixes)
        self.escape = escape or (lambda s: s)
        self.files = {}
        self.logger = logging.getLogger('exporter')

    @abstractmethod
    def format(self, item):
        return item

    def get_file(self, item):
        filename = self.escape(self.filename % item)
        if filename[-1] == '/':
            filename = f'{filename}index{self.ext}'
        path = self.output.joinpath(filename)
        out = self.files.get(path)
        if not out:
            os.makedirs(path.parent, exist_ok=True)
            self.files[path] = out = open(path, 'a+')
            self.logger.info(f'New file {path}')
        return out, path

    def write(self, item):
        out, _ = self.get_file(item)
        out.write(f'{self.format(item)}\n')

    def close(self):
        if not self.files:
            self.logger.warn('Exported nothing!')
        for f in self.files.values():
            f.close()

    def __enter__(self):
        return self

    def __exit__(self, typ, val=None, tb=None):
        self.close()
        if not typ:
            return True
        if val is None:
            if tb is None:
                raise typ
            val = typ()
        if tb is not None:
            val = val.with_traceback(tb)
        raise val


class MappingJSONExporter(MappingExporter):
    def __init__(self, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key
        self.storage = {}

    def format(self, item):
        return super().format(item)

    def write(self, item):
        _, fn = self.get_file(item)
        s = self.storage.setdefault(fn, {})
        s[item[self.key]] = item

    def close(self):
        for k, f in self.files.items():
            json.dump(self.storage[k], f)
        return super().close()


class MappingLineExporter(MappingExporter):
    def __init__(self, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key

    def format(self, item):
        return item[self.key]


class MappingCSVExporter(MappingExporter):
    def __init__(self, fieldnames=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.writers = {}
        self.fieldnames = fieldnames

    def format(self, item):
        return super().format(item)

    def get_file(self, item):
        f, fn = super().get_file(item)
        if not self.fieldnames:
            self.fieldnames = tuple(item.keys())
        writer = self.writers.get(fn)
        if not writer:
            writer = self.writers[fn] = csv.DictWriter(f, self.fieldnames, extrasaction='ignore')
            writer.writeheader()
        return writer

    def write(self, item):
        self.get_file(item).writerow({**item})
