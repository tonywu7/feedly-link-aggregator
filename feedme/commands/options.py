from scrapy.commands import ScrapyCommand

from .utils import _ClickCommand


class Command(_ClickCommand, ScrapyCommand):
    def click_command(self):
        return __name__.split('.')[-1].replace('-', '_')
