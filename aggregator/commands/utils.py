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
        import logging

        from click import Option

        logging.getLogger('scrapy.utils.log').disabled = True

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
        import signal

        from ..cli import cli

        signal.signal(signal.SIGINT, signal.default_int_handler)
        cli(sys.argv, prog_name='scrapy')
