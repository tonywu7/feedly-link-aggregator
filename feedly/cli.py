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

import gzip
import os
import re
from functools import wraps
from importlib import import_module
from pathlib import Path
from textwrap import dedent, indent

import click
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from . import exporters
from .sql.stream import consume_stream
from .sql.utils import migrate


def stylize(pattern, **styles):
    def wrapper(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            for s in func(*args, **kwargs):
                yield re.sub(pattern, lambda m: click.style(m.group(1), **styles), s)
        return wrapped
    return wrapper


def markdown_inline(func):
    @stylize(re.compile(r'`(.*?)`'), fg='green')
    @stylize(re.compile(r'_(.*?)_'), fg='blue', underline=True)
    @stylize(re.compile(r'\*\*(.*?)\*\*'), fg='yellow', bold=True)
    def f(*args, **kwargs):
        yield from func(*args, **kwargs)
    return f


def get_help(ctx):
    get_help_gen = markdown_inline(lambda ctx: (yield ctx.get_help()))
    return next(get_help_gen(ctx))


@click.group()
def cli():
    pass


def export_load_exporter(ctx: click.Context, param, value):
    try:
        exporter = import_module(f'.{value}', exporters.__name__)
        exporter.export
    except (AttributeError, ModuleNotFoundError):
        exporter = export
    if ctx.params.get('help'):
        ctx.meta['topic_name'] = value
        ctx.invoke(help_export, ctx, None, exporter)
    elif exporter is None:
        click.secho(str(ValueError(f"No exporter found for topic '{value}'")), fg='red')
        ctx.exit(1)
    return exporter


def help_export(ctx: click.Context, param, exporter):
    if not exporter or ctx.resilient_parsing:
        return
    if exporter is True:
        return True
    if exporter is export:
        click.echo(get_help(ctx))
        ctx.exit()

    @markdown_inline
    def help_subcommand():
        yield from [
            click.style('Data Exporter Help\n\n'.upper(), fg='black', bg='white', bold=True),
            'For help on the syntax of the _export_ command itself, use `export --help`.\n\n',
            click.style(ctx.meta['topic_name'], fg='black', bg='magenta', bold=True),
        ]
        doc = numpydoc2click(exporter.help_text)
        yield click.style(' - ' + next(doc) + '\n', fg='black', bg='magenta', bold=True)
        yield from doc
    click.echo_via_pager(help_subcommand())
    ctx.exit()


@cli.command()
@click.argument('topic', callback=export_load_exporter, default='help', metavar='topic')
@click.option('-h', '--help', callback=help_export, is_flag=True, is_eager=True,
              help="""
              Show this help and exit.\n
              Use `export <topic> --help` to see more info for a particular exporter.
              """)
@click.option('-i', '--input', 'wd', required=True, type=click.Path(exists=True),
              help="""
              Path to the directory containing scraped data.
              """)
@click.option('-o', '--output', 'fmt', type=click.Path(writable=True, dir_okay=False),
              help="""
              Path to which exported data is written. Will always be under an `out/` directory inside the input directory.\n
              Some exporters support output path templates, see their help for more info.
              """)
@click.option('+f', '--include', nargs=3, multiple=True, default=None, metavar='EXPR', help='')
@click.option('-f', '--exclude', nargs=3, multiple=True, default=None, metavar='EXPR',
              help="""
              Filter results based on the expression EXPR, specified with 3 values _attr predicate value_,
              such as `source:url is example.org`.\n
              Expressions themselves should not be quoted.\n
              Each exporter supports different filters, some does not support filtering.
              See their help for more info.
              """)
@click.argument('exporter-args', nargs=-1, type=click.UNPROCESSED, metavar='additional-params')
def export(topic, exporter_args, **kwargs):
    """
    Export items from scraped data.

    `topic` is the kind of information to export. Currently 2 topics are available:

    \b
        _urls_: Export URLs in formats such as plain-text lines or CSV.
        _graph_: Export scraped data as GraphML graphs.

    Some exporters accept additional parameters not listed below, which can be specified as
    a list of _key=value_ pairs after other options.

    For more info on each exporter, specify the topic and the `-h/--help` option,
    such as `export urls --help`.
    """
    options = dict([a.split('=', 1) for a in exporter_args])
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    topic.export(**kwargs, **options)


# @cli.command()
@click.option('-s', 'spider')
@click.option('-p', 'preset')
def debug_spider(spider, preset, **kwargs):
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(spider, preset=preset)
    process.start(stop_after_crawl=True)


@cli.command()
@click.option('-i', '--input', 'wd', required=True, type=click.Path(exists=True),
              help='Path to the directory containing scraped data.')
@click.option('-d', 'delete', is_flag=True,
              help='Delete leftovers after finished.')
@click.option('-s', '--cache-size', 'size', type=click.INT, default=100000)
def consume_leftovers(wd, delete, size):
    """Persist all leftover data (`stream.jsonl.gz`) to the database."""
    wd = Path(wd)
    db_path = wd.joinpath('index.db')
    for f in os.listdir(wd):
        if f[:15] == 'stream.jsonl.gz':
            streamf = wd.joinpath(f)
            print(f'Reading {streamf}')
            with gzip.open(streamf, 'rt') as stream:
                consume_stream(db_path, stream, size)
            if delete:
                os.remove(streamf)


@cli.command()
@click.option('-i', '--input', 'wd', required=True, type=click.Path(exists=True),
              help='Path to the directory containing scraped data.')
def upgrade_db(wd):
    """Upgrade an older database to the latest schema version."""
    db_path = Path(wd).joinpath('index.db')
    migrate(db_path)


def numpydoc2click(doc: str):
    PARA = re.compile(r'((?:.+\n)+)')
    PARA_WITH_HEADER = re.compile(r'(^ *)(.+)\n(?:\s*(?:-+|=+))\n((?:.+\n)+)')
    paragraphs = list(PARA.findall(dedent(doc)))
    yield paragraphs[0]
    for i in range(1, len(paragraphs)):
        p = paragraphs[i]
        match = PARA_WITH_HEADER.match(p)
        if match:
            indentation, header, p = match.group(1), match.group(2), match.group(3)
            if not indentation:
                header = header.upper()
            yield indent(click.style(header, bold=True), indentation)
            yield '\n'
        yield indent(p, '    ')
        yield '\n'
