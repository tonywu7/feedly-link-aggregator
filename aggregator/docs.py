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

import re
from functools import wraps
from textwrap import dedent, indent

import click

docs = []


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
    @stylize(re.compile(r'~(.*?)~'), fg='blue', underline=True)
    @stylize(re.compile(r'\*\*(.*?)\*\*'), fg='yellow', bold=True)
    def f(*args, **kwargs):
        yield from func(*args, **kwargs)
    return f


def numpydoc2click(doc: str):
    PARA = re.compile(r'((?:.+\n)+)')
    PARA_WITH_HEADER = re.compile(r'(^ *)(.+)\n(?:\s*(?:-+|=+))\n((?:.+\n)+)')
    paragraphs = list(PARA.findall(dedent(doc)))
    yield paragraphs[0] + '\n'
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


class OptionsContributor:
    _subclassed = set()

    @classmethod
    def __init_subclass__(cls, _doc_order=0):
        for c in cls.mro():
            if c.__qualname__ in cls._subclassed:
                return

        cls._subclassed.add(cls.__qualname__)
        docs.append((cls, cls._help_options(), _doc_order))

    @staticmethod
    @markdown_inline
    def format_docs():
        yield from [
            click.style('Aggregator Customization Manual\n\n'.upper(), fg='black', bg='white', bold=True),
            'This program supports the use of presets, which lets you define \n'
            'options for different scenarios.\n',
            '\n',
            'A preset works like a Scrapy settings file: you simply declare your options as \n'
            'uppercase-only top level variables, such as `FOLLOW_DOMAINS = ["abc.xyz"]`.\n'
            '\n',
            'Then, run Scrapy with the command-line option ~-s PRESET=<path-to-preset.py>~.\n',
            '\n',
            'You may also specify options directly on the command line with the ~-s~ option:\n',
            'such as `-s RSS=http://xkcd.com/atom.xml`, in which case those declared on the\n',
            'command line take precedence over those in a preset.\n',
            '\n',
            'Some example presets are located in the `presets/` directory.\n',
            '\n',
            'The following is the list of supported options, grouped by the components they\n',
            'belong to.\n'
            '\n',
            click.style('============*============\n', fg='white', bold=True),
            '\n',
        ]

        for cls, options, _ in sorted(docs, key=lambda t: t[2], reverse=True):
            yield click.style('-------------\n', fg='black', bold=True)
            yield f'**{repr(cls)}**\n'
            yield '\n'

            docstring = cls.__doc__
            if docstring:
                yield from numpydoc2click(dedent(docstring))

            for opt, doc in options.items():
                yield f'  ~{opt}~\n'
                yield f'{indent(dedent(doc), "      ")}\n'

            yield '\n\n'
