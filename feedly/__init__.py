def _config_logging():
    from logging.config import dictConfig
    from scrapy.utils.project import get_project_settings
    from .logger import make_logging_config
    settings = get_project_settings()
    if settings.getbool('LOG_ENABLED'):
        dictConfig(make_logging_config(
            'feedly',
            formatter_style='standard',
            formatter_colored=True,
            level=settings.getint('LOG_LEVEL') or 20,
            config_override=settings.getdict('LOGGING_OVERRIDE', {}),
        ))


_config_logging()
