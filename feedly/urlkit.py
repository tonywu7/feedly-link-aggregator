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

from typing import Tuple
from urllib.parse import SplitResult, urlsplit

from .datastructures import labeled_sequence


def is_http(u):
    return isinstance(u, str) and urlsplit(u).scheme in {'http', 'https'}


def is_absolute_http(u):
    if not isinstance(u, str):
        return False
    s = urlsplit(u)
    return s.scheme in {'http', 'https'} or s.scheme == '' and s.netloc


def ensure_protocol(u, protocol='http'):
    s = urlsplit(u)
    return u if s.scheme else f'{protocol}:{u}'


def domain_parents(domain: str) -> Tuple[str]:
    parts = domain.split('.')
    return tuple('.'.join(parts[-i:]) for i in range(len(parts), 1, -1))


def no_scheme(url: SplitResult) -> str:
    return url.geturl()[len(f'{url.scheme}:'):]


def path_only(url: SplitResult) -> str:
    return url.geturl()[len(f'{url.scheme}://{url.netloc}'):]


def select_templates(query, template_tree):
    matches = {r: r.match(query) for r in template_tree}
    matches = [(r, m) for r, m in matches.items() if m]
    if not matches:
        raise ValueError('No template provider')
    pattern, match = matches[0]
    templates = template_tree[pattern]
    if not callable(templates):
        templates = [t[0] for t in templates.items()]
    return match, templates


def build_urls(base, match, templates):
    parsed = urlsplit(base)
    if callable(templates):
        return templates(parsed, match)
    specifiers = {
        **parsed._asdict(),
        'network_path': no_scheme(parsed),
        'path_query': path_only(parsed),
        'original': parsed.geturl(),
        **match.groupdict(),
        **labeled_sequence(match.groups(), start=1, as_str=True),
    }
    return [t % specifiers for t in templates]
