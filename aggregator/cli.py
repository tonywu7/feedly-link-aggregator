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

import logging
from importlib import import_module
from pathlib import Path
from textwrap import dedent

import click
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from . import _config_logging, exporters
from .docs import markdown_inline, numpydoc2click
from .sql.cli import check, leftovers, merge, migrate

get_help_gen = markdown_inline(lambda ctx: (yield ctx.get_help()))


def get_help(ctx):
    return next(get_help_gen(ctx))


@click.group()
@click.option('--debug', is_flag=True)
@click.pass_context
def cli(ctx, debug=False):
    level = logging.DEBUG if debug else logging.INFO
    _config_logging(level=level)
    ctx.ensure_object(dict)
    ctx.obj['DEBUG'] = debug


def export_load_exporter(ctx: click.Context, param, value):
    not_found = False
    try:
        exporter = import_module(f'.{value}', exporters.__name__)
        assert exporter.export
    except (AttributeError, ModuleNotFoundError, AssertionError):
        not_found = True
        exporter = export
    if ctx.params.get('help') or value == 'help':
        ctx.meta['topic_name'] = value
        ctx.invoke(help_export, ctx, None, exporter)
    elif not_found:
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
            'For help on the syntax of the ~export~ command itself, use `export --help`.\n\n',
            click.style(ctx.meta['topic_name'], fg='black', bg='magenta', bold=True),
        ]
        doc = numpydoc2click(exporter.help_text)
        yield click.style(' - ' + next(doc), fg='black', bg='magenta', bold=True)
        yield from doc
    click.echo_via_pager(help_subcommand())
    ctx.exit()


def ensure_index_db(path):
    path = Path(path)
    if path.is_dir():
        return path / 'index.db'
    return path


@cli.command()
@click.argument('topic', callback=export_load_exporter, default='help', metavar='topic')
@click.option('-h', '--help', callback=help_export, is_flag=True, is_eager=True,
              help="""
              Show this help and exit.\n
              Use `export <topic> --help` to see more info for a particular exporter.
              """)
@click.option('-i', '--input', 'wd', required=True, type=click.Path(exists=True, file_okay=False),
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


@cli.command(hidden=True)
@click.option('-s', 'spider')
@click.option('-p', 'preset')
def run_spider(spider, preset, **kwargs):
    settings = get_project_settings()
    settings['PRESET'] = preset
    process = CrawlerProcess(settings, install_root_handler=False)
    process.crawl(spider)
    process.start(stop_after_crawl=True)


@cli.command()
@click.option('-i', '--input', 'db_path', required=True, type=click.Path(exists=True),
              help='Path to the database.')
@click.option('-d', '--sql-debug', 'debug', type=click.Path(exists=False, dir_okay=False),
              help='Optional file to write executed SQL statements to.')
@click.pass_context
def check_db(ctx, db_path, debug=False, **kwargs):
    """Check a database for potential problems and inconsistencies."""

    ctx.exit(check(ensure_index_db(db_path), debug=debug))


@cli.command()
@click.option('-i', '--input', 'db_path', required=True, type=click.Path(exists=True),
              help='Path to the database.')
@click.option('-d', '--sql-debug', 'debug', type=click.Path(exists=False, dir_okay=False),
              help='Optional file to write executed SQL statements to.')
@click.pass_context
def upgrade_db(ctx, db_path, debug=False, **kwargs):
    """Upgrade an older database to the latest schema version."""

    ctx.exit(migrate(ensure_index_db(db_path), debug=debug))


@cli.command()
@click.option('-i', '--input', 'db_paths', multiple=True, required=True, type=click.Path(exists=True, dir_okay=False),
              help='Path to the database to be merged. Can be specified multiple times.')
@click.option('-o', '--output', 'output', required=True, type=click.Path(exists=False, dir_okay=False),
              help='Optional file to write executed SQL statements to.')
@click.option('-d', '--sql-debug', 'debug', type=click.Path(exists=False, dir_okay=False),
              help='Optional file to write executed SQL statements to.')
@click.pass_context
def merge_db(ctx, *, db_paths, output, debug=False, **kwargs):
    """Merge multiple databases into a new database."""

    ctx.exit(merge(output, *db_paths, debug=debug))


@cli.command()
@click.option('-i', '--input', 'wd', required=True, type=click.Path(exists=True, file_okay=False),
              help='Path to the directory containing scraped data.')
@click.option('-d', '--sql-debug', 'debug', type=click.Path(exists=False, dir_okay=False),
              help='Optional file to write executed SQL statements to.')
@click.pass_context
def cleanup(ctx, wd, debug=False, **kwargs):
    """Find all temporary databases and attempt to merge them into the main database."""

    ctx.exit(leftovers(wd, debug=debug))


@cli.command()
def options():
    """List available spider options."""

    from . import walk_package
    for _ in walk_package():
        pass

    from .docs import OptionsContributor
    click.echo_via_pager(OptionsContributor.format_docs())


@cli.command(hidden=True)
def gen_commands():
    template = dedent("""
    from scrapy.commands import ScrapyCommand

    from .utils import _ClickCommand


    class Command(_ClickCommand, ScrapyCommand):
        def click_command(self):
            return __name__.split('.')[-1].replace('-', '_')
    """).lstrip().rstrip(' ')

    path = Path(__file__).with_name('commands')
    for p in path.iterdir():
        if not p.is_file():
            continue
        content = open(p).read()
        if content == template:
            p.unlink()
    for k, v in cli.commands.items():
        if not v.hidden:
            with open(path / f'{k}.py', 'w+') as f:
                f.write(template)
