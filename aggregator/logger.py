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
import sys
from typing import Dict, Union

try:
    import termcolor
    _ = termcolor.colored
except ImportError:
    _ = None

try:
    import colorama
    colorama.init()
except ImportError:
    _ = None


def compose_mappings(*mappings):
    base = {}
    base.update(mappings[0])
    for m in mappings[1:]:
        for k, v in m.items():
            if k in base and type(base[k]) is type(v):
                if isinstance(v, dict):
                    base[k] = compose_mappings(base[k], v)
                elif isinstance(v, set):
                    base[k] |= v
                elif isinstance(v, list):
                    base[k].extend(v)
                else:
                    base[k] = v
            else:
                base[k] = v
    return base


class _LogContainer:
    pass


class _ColoredFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style='%', *, color='white'):
        super().__init__(fmt, datefmt, style)
        self.termcolor_args = lambda self, record: ()
        if isinstance(color, str):
            self.termcolor_args = lambda self, record: (color,)
        elif isinstance(color, tuple):
            self.termcolor_args = lambda self, record: color
        elif callable(color):
            self.termcolor_args = color

    def format(self, record):
        color_args = self.termcolor_args(self, record)
        return _(super().format(record), *color_args)


class _CascadingFormatter(logging.Formatter):
    def __init__(
        self, sections: str,
        stylesheet: Dict[str, Union[str, logging.Formatter]],
        style='%', stacktrace=None, datefmt=None,
    ):
        self.stylesheet = {}
        for section, fmt in stylesheet.items():
            formatter = logging.Formatter(fmt) if isinstance(fmt, str) else fmt
            if section != stacktrace:
                formatter.formatException = lambda info: ''
                formatter.formatStack = lambda info: ''
            self.stylesheet[section] = formatter
        super().__init__(sections, datefmt, style)

    def format(self, record):
        parent = _LogContainer()
        for child, fmt in self.stylesheet.items():
            setattr(parent, child, fmt.format(record))
        return super().formatMessage(parent)

    @classmethod
    def from_config(cls, *, sections, stylesheet, **kwargs):
        stylesheet_ = {}
        for k, fmt in stylesheet.items():
            if isinstance(fmt, str):
                stylesheet_[k] = fmt
                continue
            f_kwargs = {}
            f_kwargs.update(fmt)
            factory = f_kwargs.pop('()', logging.Formatter)
            stylesheet_[k] = factory(**f_kwargs)
        return cls(sections, stylesheet_, **kwargs)


LOG_LEVEL_PREFIX_COLORS = {
    'DEBUG': ('magenta', None, ['bold']),
    'INFO': ('white', None, ['bold']),
    'WARNING': ('yellow', None, ['bold']),
    'ERROR': ('red', None, ['bold']),
    'CRITICAL': ('grey', 'on_red', ['bold']),
}
LOG_LEVEL_PREFIX_COLORS_DEBUG = {
    **LOG_LEVEL_PREFIX_COLORS,
    'INFO': ('blue', None, ['bold']),
}


def _color_stacktrace(self, record: logging.LogRecord):
    return ('red',) if record.exc_info else ('white',)


def _conditional_color(field, rules, default=('white',)):
    def fn(self, record):
        return rules.get(getattr(record, field), default)
    return fn


FMT_PREFIX = '%(asctime)s %(levelname)8s'
FMT_LOGGER = '[%(processName)s:%(name)s]'
FMT_SOURCE = '(%(module)s.%(funcName)s:%(lineno)d)'

formatter_styles = {
    'standard': {
        'normal': {
            'format': f'{FMT_PREFIX} {FMT_LOGGER} %(message)s',
        },
        'colored': {
            '()': _CascadingFormatter.from_config,
            'sections': '%(prefix)s %(name)s %(message)s',
            'stylesheet': {
                'prefix': {
                    '()': _ColoredFormatter,
                    'fmt': FMT_PREFIX,
                    'color': _conditional_color('levelname', LOG_LEVEL_PREFIX_COLORS),
                },
                'name': {
                    '()': _ColoredFormatter,
                    'fmt': FMT_LOGGER,
                    'color': 'blue',
                },
                'message': {
                    '()': _ColoredFormatter,
                    'fmt': '%(message)s',
                    'color': _color_stacktrace,
                },
            },
            'stacktrace': 'message',
        },
    },
    'debug': {
        'normal': {
            'format': f'{FMT_PREFIX} {FMT_LOGGER}{FMT_SOURCE}  %(message)s',
        },
        'colored': {
            '()': _CascadingFormatter.from_config,
            'sections': '%(prefix)s %(name)s%(source)s  %(message)s',
            'stylesheet': {
                'prefix': {
                    '()': _ColoredFormatter,
                    'fmt': FMT_PREFIX,
                    'color': _conditional_color('levelname', LOG_LEVEL_PREFIX_COLORS_DEBUG),
                },
                'name': {
                    '()': _ColoredFormatter,
                    'fmt': FMT_LOGGER,
                    'color': 'blue',
                },
                'source': {
                    '()': _ColoredFormatter,
                    'fmt': FMT_SOURCE,
                    'color': 'cyan',
                },
                'message': {
                    '()': _ColoredFormatter,
                    'fmt': '%(message)s',
                    'color': _color_stacktrace,
                },
            },
            'stacktrace': 'message',
        },
    },
}

logging_config_template = {
    'disable_existing_loggers': True,
    'version': 1,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'stream': sys.stderr,
        },
    },
    'loggers': {
        'main': {
            'level': logging.NOTSET,
        },
        'scrapy.core': {
            'level': logging.NOTSET,
        },
        'scrapy.core.engine': {
            'level': logging.NOTSET,
        },
        'scrapy.crawler': {
            'level': logging.WARNING,
        },
        'scrapy.middleware': {
            'level': logging.WARNING,
        },
        'twisted': {
            'level': logging.ERROR,
        },
    },
    'root': {
        'handlers': ['console'],
    },
}


def make_logging_config(
    app_name, *overrides, level=logging.INFO,
    style='standard', colored=True, datefmt=None,
    logfile=None, **kwargs,
):
    color_mode = 'colored' if colored and _ else 'normal'
    if style in formatter_styles:
        formatter = formatter_styles[style][color_mode]
    else:
        formatter = style

    app_logging_config = {
        'formatters': {
            'default_fmt': formatter,
        },
        'handlers': {
            'console': {
                'formatter': 'default_fmt',
                'level': level,
            },
        },
        'loggers': {
            f'{app_name}': {
                'level': logging.NOTSET,
            },
        },
        'root': {
            'level': level,
        },
    }

    file_handler_config = {}
    if logfile:
        file_handler_config = {
            'formatters': {
                'no_color': (formatter_styles[style]['normal']
                             if style in formatter_styles else style),
            },
            'handlers': {
                'file': {
                    'class': 'logging.FileHandler',
                    'filename': logfile,
                    'formatter': 'no_color',
                },
            },
            'root': {
                'handlers': ['file'],
            },
        }

    datefmt_config = {}
    if datefmt:
        datefmt_config = {
            'formatters': {
                'default_fmt': {
                    'datefmt': datefmt,
                },
                'no_color': {
                    'datefmt': datefmt,
                },
            },
        }

    log_config = compose_mappings(
        logging_config_template,
        app_logging_config,
        file_handler_config,
        datefmt_config,
        *overrides,
    )
    return log_config


class _LoggingParticipant:
    def __init__(self, *args, _logger=None, **kwargs):
        if _logger:
            self.log: logging.Logger = _logger
        elif isinstance(getattr(self, '_logger_name', None), str):
            self.log: logging.Logger = logging.getLogger(self._logger_name)
            self.log.disabled = True
        else:
            raise NotImplementedError('_logger_name is not defined')
