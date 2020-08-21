from datetime import datetime

OUTPUT = f'./{datetime.now().strftime("%Y%m%d%H%M%S")}.crawl.json'
OVERWRITE = False

FEED = 'https://xkcd.com/atom.xml'
FEED_TEMPLATES = {
    r'.*': {
        '%(original)s': 999,
    },
}

DOWNLOAD_ORDER = 'oldest'
DOWNLOAD_PER_BATCH = 1000

FEEDLY_FUZZY_SEARCH = False
FEEDLY_ACCESS_TOKEN = None

ALLOWED_DOMAINS = None
NETWORK_DEPTH = 1

STREAM_ID_PREFIX = 'feed/'
