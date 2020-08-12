from logging.config import dictConfig

from .logger import make_logging_config
from .settings import LOG_LEVEL

dictConfig(make_logging_config(
    'feedly',
    formatter_style='standard',
    formatter_colored=True,
    level=LOG_LEVEL,
))
