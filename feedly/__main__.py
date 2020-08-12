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

import json
from pathlib import Path

import click


@click.group()
def cli():
    pass


@cli.command()
@click.option('-a', '--attr', default='src', help='Comma-separated list of HTML attributes to extract; default is `src`')
@click.argument('crawled', type=click.Path(exists=True))
def collect_urls(crawled, attr):
    """Collect all external links in a crawled project and print them as a newline-separated list.

    CRAWLED is the path to the root directory of an existing crawl.
    """
    home = Path(crawled)
    attr = set(attr.split(','))

    with open(home.joinpath('index.json'), 'r') as f:
        index = json.load(f)

    for item_hash in index['items']:
        with open(home.joinpath(f'items/{item_hash[:2]}/{item_hash[2:4]}/{item_hash}/external.json'), 'r') as f:
            external = json.load(f)
            for k in attr:
                urls = external.get(k, [])
                if urls:
                    print('\n'.join(urls))


if __name__ == '__main__':
    cli()
