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

from scrapy import Request


class DummyRequest(Request):
    def __init__(self, *, callback=None, url=None, dont_filter=None, **kwargs):
        callback = callback or self.callback
        super().__init__('https://httpbin.org/status/204', callback, dont_filter=True, **kwargs)

    def callback(self, _):
        pass


class ResumeRequest(DummyRequest):
    def __init__(self, *, callback, url=None, meta=None, **kwargs):
        super().__init__(callback=callback, meta={'_persist': 'release'}, **kwargs)


class FinishedRequest(DummyRequest):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.meta['_persist'] = 'remove'


class ProbeRequest(Request):
    def __init__(self, *, url, callback, source=None, **kwargs):
        meta = kwargs.pop('meta', {})
        meta['_persist'] = 'add'
        meta['pkey'] = (meta['search_query'], 'search')
        super().__init__(url=url, callback=callback, meta=meta, **kwargs)
        self.priority = source.priority - 1 if source else self.priority - 1


def reconstruct_request(cls, instance, **kwargs):
    callback = kwargs.pop('callback')
    callback = getattr(instance, callback)
    return cls(callback=callback, **kwargs)
