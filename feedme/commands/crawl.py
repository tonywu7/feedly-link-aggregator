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

from scrapy.commands import ScrapyCommand
from scrapy.commands.crawl import Command

from .utils import _LoggingMixin


class CrawlCommand(Command, _LoggingMixin):
    def add_options(self, parser):
        super().add_options(parser)
        parser.add_option('-v', '--verbose', action='store_true',
                          help='Log more information')
        parser.remove_option('-a')
        parser.remove_option('-t')

    def process_options(self, args, opts):
        ScrapyCommand.process_options(self, args, opts)

        opts.spargs = {}

        if len(args) == 2:
            self.settings['RSS'] = args.pop()

        self._takeover_logging()

        if opts.output:
            self.settings['OUTPUT'] = opts.output[0]
        self.settings.pop('FEEDS')

        self.settings['CMDLINE_ARGS'] = {'args': args, 'opts': vars(opts)}

        if opts.verbose:
            self.settings['VERBOSE'] = True
            self.settings.set('LOG_VIOLATIONS', True, priority='cmdline')
            self.settings.set('STATS_DUMP', True, priority='cmdline')
