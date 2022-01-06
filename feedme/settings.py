# Scrapy settings for feedly project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

__version__ = '0.10.11'

BOT_NAME = 'feedly'

SPIDER_MODULES = ['feedme.spiders']
NEWSPIDER_MODULE = 'feedme.spiders'

LOG_ENABLED = True
LOG_LEVEL = 20

LOGSTATS_INTERVAL = 60.0
METRICS_CALC_INTERVAL = 20.0

LOG_VIOLATIONS = False
STATS_DUMP = False

COMMANDS_MODULE = 'feedme.commands'

# This program uses a custom logging config (see __init__.py)
# To give control of logging back to Scrapy, set this to False
CUSTOM_LOGGING_ENABLED = True

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = f'Mozilla/5.0 (compatible; hyperlinkfeedme/{__version__}; +https://github.com/monotony113/feedly-link-feedme)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html# download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'application/json;q=0.9;text/html,application/xhtml+xml,application/xml;q=0.8,*/*;q=0.7',
}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
SPIDER_MIDDLEWARES = {
    'scrapy.spidermiddlewares.depth.DepthMiddleware': None,
    'feedme.middlewares.RequestDefrosterSpiderMiddleware': 100,
    'feedme.middlewares.DerefItemSpiderMiddleware': 101,
    'feedme.middlewares.OffsiteFeedSpiderMiddleware': 500,
    'feedme.middlewares.ConditionalDepthSpiderMiddleware': 550,
    'feedme.middlewares.FetchSourceSpiderMiddleware': 600,
    'feedme.middlewares.CrawledItemSpiderMiddleware': 800,
    'feedme.spiders.cluster.ExplorationSpiderMiddleware': 900,
}

HTTPERROR_ALLOWED_CODES = [403, 404]

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    'feedme.middlewares.RequestPersistenceDownloaderMiddleware': 150,
    'feedme.middlewares.FeedProbingDownloaderMiddleware': 200,
    'feedme.middlewares.HTTPErrorDownloaderMiddleware': 500,
    'feedme.middlewares.AuthorizationDownloaderMiddleware': 600,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
EXTENSIONS = {
    'scrapy.extensions.logstats.LogStats': None,
    'feedme.extensions._LoggingHelper': 99,
    'feedme.extensions.PresetLoader': 100,
    'feedme.extensions.SettingsLoader': 101,
    'feedme.extensions.LogStatsExtended': 102,
    'feedme.extensions.RequestMetrics': 102,
    'feedme.extensions.ContribMiddleware': 200,
    'feedme.extensions.GlobalPersistence': 999,
}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    # 'feedme.pipelines.CompressedStreamExportPipeline': 900,
    # 'feedme.pipelines.SQLiteExportPipeline': 900,
    'feedme.pipelines.SQLiteExportProcessPipeline': 900,
}

AUTO_LOAD_PREDEFINED_PRESETS = True

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = 0
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.5
# Enable showing throttling stats for every response received:
AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html# httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

DEPTH_LIMIT = 1
