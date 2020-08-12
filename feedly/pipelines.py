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
import logging
from hashlib import sha1
from pathlib import Path
from typing import Dict, Set

from scrapy.exceptions import DropItem

from .spiders.feedly_rss import FeedlyRssSpider
from .utils import JSONDict, parse_html, is_http

log = logging.getLogger('feedly.pipeline')


class FeedlyItemPipeline:
    def process_item(self, item: JSONDict, spider: FeedlyRssSpider):
        item_id = item.get('id')
        if not item_id:
            raise DropItem('Malformed item (id not found)')

        item_cks = sha1(item_id.encode()).hexdigest()
        spider.index['items'][item_cks] = 1

        item_home = Path(f'items/{item_cks[:2]}/{item_cks[2:4]}/{item_cks}')
        spider._write_file(item_home.joinpath('index.json'), json.dumps(item, ensure_ascii=False, skipkeys=True))

        log.info(f'Got item {item_id}')
        origin = item.get('originId')
        if origin:
            log.info(f'URL: {origin}')
        item['_hash'] = item_cks
        item['_dir'] = item_home
        return item


class FeedlyExternalResourcePipeline:
    def process_item(self, item: JSONDict, spider: FeedlyRssSpider):
        external: Dict[str, Set[str]] = {k: set() for k in {'href', 'src', 'data-src', 'data-href'}}
        for k in {'content', 'summary'}:
            content = item.get(k)
            if content:
                content = parse_html(content.get('content', ''))
                for attr, links in external.items():
                    links |= {tag.attrib.get(attr) for tag in content.css(f'[{attr}]')}

        visual = item.get('visual')
        if visual:
            external['src'].add(visual.get('url'))

        external = {k: [u for u in v if is_http(u)] for k, v in external.items()}

        home: Path = item['_dir']
        spider._write_file(home.joinpath('external.json'), json.dumps(external, ensure_ascii=False, skipkeys=True))

        return item
