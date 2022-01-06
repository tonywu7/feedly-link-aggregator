from scrapy.commands import ScrapyCommand


class Command(ScrapyCommand):
    def short_desc(self):
        return 'List available commands'

    def add_options(self, parser):
        import logging

        logging.getLogger('scrapy.utils.log').disabled = True
        super().add_options(parser)

    def run(self, *args, **kwargs):
        from ..cli import cli
        cli(['--help'], prog_name='scrapy')
