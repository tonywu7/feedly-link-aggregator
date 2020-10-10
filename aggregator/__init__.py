def walk_package(path=None, name=__name__):
    import pkgutil
    from importlib.util import module_from_spec
    from pathlib import Path
    if not path:
        path = Path(__file__).parent
    for loader, module_name, is_pkg in pkgutil.walk_packages([str(path)]):
        pkg_name = f'{name}.{module_name}'
        if not is_pkg:
            spec = loader.find_spec(pkg_name)
            mod = module_from_spec(spec)
            try:
                spec.loader.exec_module(mod)
            except (ImportError, ModuleNotFoundError):
                continue
            yield mod
        else:
            yield from walk_package(path / module_name, pkg_name)


def _config_logging(config=None, *args, **kwargs):
    import logging
    import sys
    from logging.config import dictConfig

    from .logger import make_logging_config

    if config and not config.getbool('LOG_ENABLED'):
        return

    if config:
        kwargs = {
            'level': config.get('LOG_LEVEL', logging.INFO),
            'colored': True,
        }
        overrides = []

        if config.get('LOG_FILE'):
            kwargs['logfile'] = config['LOG_FILE']

        if config.get('LOG_DATEFORMAT'):
            kwargs['datefmt'] = config['LOG_DATEFORMAT']

        if config.get('LOG_STDOUT'):
            from scrapy.utils.log import StreamLogger
            sys.stdout = StreamLogger(logging.getLogger('stdout'))

        if config.get('LOG_SHORT_NAMES'):
            from scrapy.utils.log import TopLevelFormatter
            overrides.append({
                'filters': {
                    'tlfmt': {
                        '()': TopLevelFormatter,
                        'loggers': ['scrapy', 'main', 'worker'],
                    }}})

        overrides += config.get('LOGGING_OVERRIDES', [])
        # logging.basicConfig(force=True)
        dictConfig(make_logging_config('feedly', *overrides, **kwargs))
        return

    dictConfig(make_logging_config('feedly', *args, **kwargs))
