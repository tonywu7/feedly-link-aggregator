# Scrapy settings for feedly project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

__version__ = '0.10.10'

BOT_NAME = 'feedly'

SPIDER_MODULES = ['aggregator.spiders']
NEWSPIDER_MODULE = 'aggregator.spiders'

LOG_ENABLED = True
LOG_LEVEL = 20

LOGSTATS_INTERVAL = 60.0
METRICS_CALC_INTERVAL = 20.0

LOG_VIOLATIONS = False
STATS_DUMP = False

COMMANDS_MODULE = 'aggregator.commands'

# This program uses a custom logging config (see __init__.py)
# To give control of logging back to Scrapy, set this to False
CUSTOM_LOGGING_ENABLED = True

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = f'Mozilla/5.0 (compatible; hyperlinkaggregator/{__version__}; +https://github.com/monotony113/feedly-link-aggregator)'

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
    'aggregator.middlewares.RequestDefrosterSpiderMiddleware': 100,
    'aggregator.middlewares.DerefItemSpiderMiddleware': 101,
    'aggregator.middlewares.OffsiteFeedSpiderMiddleware': 500,
    'aggregator.middlewares.ConditionalDepthSpiderMiddleware': 550,
    'aggregator.middlewares.FetchSourceSpiderMiddleware': 600,
    'aggregator.middlewares.CrawledItemSpiderMiddleware': 800,
    'aggregator.spiders.cluster.ExplorationSpiderMiddleware': 900,
}

HTTPERROR_ALLOWED_CODES = [403, 404]

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    'aggregator.middlewares.RequestPersistenceDownloaderMiddleware': 150,
    'aggregator.middlewares.FeedProbingDownloaderMiddleware': 200,
    'aggregator.middlewares.HTTPErrorDownloaderMiddleware': 500,
    'aggregator.middlewares.AuthorizationDownloaderMiddleware': 600,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
EXTENSIONS = {
    'scrapy.extensions.logstats.LogStats': None,
    'aggregator.extensions._LoggingHelper': 99,
    'aggregator.extensions.PresetLoader': 100,
    'aggregator.extensions.SettingsLoader': 101,
    'aggregator.extensions.LogStatsExtended': 102,
    'aggregator.extensions.RequestMetrics': 102,
    'aggregator.extensions.ContribMiddleware': 200,
    'aggregator.extensions.GlobalPersistence': 999,
}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    # 'aggregator.pipelines.CompressedStreamExportPipeline': 900,
    # 'aggregator.pipelines.SQLiteExportPipeline': 900,
    'aggregator.pipelines.SQLiteExportProcessPipeline': 900,
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
