def _config_logging(override=None):
    from logging.config import dictConfig

    from .logger import make_logging_config
    level = globals().get('LOG_LEVEL', 20)
    override = override or {}
    dictConfig(make_logging_config(
        'feedly',
        formatter_style='standard',
        formatter_colored=True,
        level=level,
        config_override=override,
    ))


_config_logging()
