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

from pathlib import Path

import simplejson as json
from scrapy.commands import ScrapyCommand
from scrapy.exceptions import UsageError

from .crawl import CrawlCommand


class ResumeCrawlCommand(CrawlCommand):
    def syntax(self):
        return '<output>'

    def short_desc(self):
        return 'Continue an existing crawl'

    def add_options(self, parser):
        ScrapyCommand.add_options(self, parser)

    def process_options(self, args, opts):
        if len(args) < 1:
            raise UsageError()

        datadir = Path(args[0])

        if not datadir.exists():
            raise UsageError(f'Directory `{datadir}` does not exist.')
        if not datadir.is_dir():
            raise UsageError(f'{datadir} is not a directory.', print_help=False)

        try:
            with open(datadir / 'options.json') as f:
                options = json.load(f)
        except (OSError, json.JSONDecodeError):
            raise UsageError(f'{datadir} does not contain a valid "options.json" file.\n'
                             'Cannot restore command line arguments used to initiate the program.')

        args.clear()
        args.extend(options['args'])
        for k, v in options['opts'].items():
            setattr(opts, k, v)

        super().process_options(args, opts)
