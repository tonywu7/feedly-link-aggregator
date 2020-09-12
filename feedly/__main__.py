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

from importlib import import_module
from pathlib import Path

import click
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from . import exporters


@click.group()
def cli():
    pass


@cli.command()
@click.argument('topic', required=True)
@click.option('-i', '--input', 'wd', required=True, type=click.Path(exists=True))
@click.option('-o', '--output', required=True)
@click.option('+f', '--include', nargs=3, multiple=True, default=None)
@click.option('-f', '--exclude', nargs=3, multiple=True, default=None)
@click.argument('exporter-args', nargs=-1, type=click.UNPROCESSED)
def export(topic, wd, include, exclude, output, exporter_args):
    options = dict([a.split('=', 1) for a in exporter_args])
    wd = Path(wd)
    try:
        exporter = import_module(f'.{topic}', exporters.__name__)
        exporter.export
    except (AttributeError, ModuleNotFoundError):
        raise ValueError(f"No exporter found for topic '{topic}'")
    exporter.export(wd, include, exclude, output, **options)


@cli.command()
@click.option('-s', 'spider')
@click.option('-p', 'profile')
def debug_spider(spider, profile):
    settings = get_project_settings()
    settings['AUTOTHROTTLE_ENABLED'] = False
    process = CrawlerProcess(settings)
    process.crawl(spider, profile=profile)
    process.start(stop_after_crawl=True)


if __name__ == '__main__':
    cli()
