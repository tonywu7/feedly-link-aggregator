# -*- coding: utf-8 -*-

# Scrapy settings for feedly project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

__version__ = '0.10.2'

BOT_NAME = 'feedly'

SPIDER_MODULES = ['feedly.spiders']
NEWSPIDER_MODULE = 'feedly.spiders'

LOG_ENABLED = True
LOG_LEVEL = 20

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
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
# }

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'feedly.middlewares.FeedlySpiderMiddleware': 543,
# }

HTTPERROR_ALLOWED_CODES = [403, 404]

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    'feedly.middlewares.RequestFilterDownloaderMiddleware': 100,
    'feedly.middlewares.RequestPersistenceDownloaderMiddleware': 150,
    'feedly.middlewares.FeedProbingDownloaderMiddleware': 200,
    'feedly.middlewares.HTTPErrorDownloaderMiddleware': 500,
    'feedly.middlewares.AuthorizationDownloaderMiddleware': 600,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
EXTENSIONS = {
    'scrapy.extensions.telnet.TelnetConsole': None,
    'scrapy.extensions.logstats.LogStats': None,
    'feedly.extensions.LogStatsExtended': 500,
}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'feedly.pipelines.ConfigLogging': 100,
    'feedly.pipelines.CompressedStreamExportPipeline': 900,
    # 'feedly.pipelines.StatsPipeline': 950,
    # 'feedly.pipelines.CProfile': 1000,
}

LOGSTATS_INTERVAL = 60.0

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = 0
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.2
# Enable showing throttling stats for every response received:
AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html# httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
