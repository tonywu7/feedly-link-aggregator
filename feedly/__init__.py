from logging.config import dictConfig

from scrapy.utils.project import get_project_settings

from .logger import make_logging_config

dictConfig(make_logging_config(
    'feedly',
    formatter_style='standard',
    formatter_colored=True,
    level=get_project_settings().get('LOG_LEVEL') or 20,
))
