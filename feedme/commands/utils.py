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


def dir_validator(path):
    path_ = Path(path)
    if path_.exists() and not path_.is_dir():
        raise ValueError(f'{path_} exists and is not a directory.')
    return path


def exists_validator(path):
    path_ = Path(path)
    if not path_.exists():
        raise ValueError(f'{path_} does not exist.')
    return path


def _restore_sigint():
    import signal

    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)


def _disable_initial_log():
    import logging

    logging.getLogger('scrapy.utils.log').disabled = True


class _LoggingMixin:
    def _takeover_logging(self, force=False):
        from scrapy.utils.log import configure_logging

        from .. import _config_logging

        enabled = (self.settings.getbool('LOG_ENABLED')
                   and self.settings.getbool('CUSTOM_LOGGING_ENABLED', True))
        if not force and not enabled:
            return

        settings = self.settings
        configure_logging(install_root_handler=False)
        _config_logging(settings)

        settings['CUSTOM_LOGGING_ENABLED'] = True
        settings.set('LOG_ENABLED', False, priority=9999)

        if 'LOG_FILE' in settings:
            settings['_LOG_FILE'] = settings['LOG_FILE']
            del settings['LOG_FILE']


class _ClickCommand:
    def click_command(self):
        raise NotImplementedError

    def _get_command(self):
        from .. import cli
        return getattr(cli, self.click_command())

    def add_options(self, parser):
        from click import Option

        _disable_initial_log()
        super().add_options(parser)
        parser.add_option('-h', '--help', action='store_true')

        command = self._get_command()
        for param in command.params:
            if isinstance(param, Option):
                opts = [o for o in param.opts if o[0] == '-']
                parser.add_option(*opts, action='store_true')

    def short_desc(self):
        command = self._get_command()
        help_ = command.help or ''
        return help_.split('\n')[0]

    def long_desc(self):
        command = self._get_command()
        return command.help

    def run(self, args, opts):
        import sys

        from ..cli import cli

        _restore_sigint()
        cli(sys.argv, prog_name='scrapy')
