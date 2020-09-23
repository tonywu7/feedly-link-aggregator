FOLLOW_DOMAINS = {'livejournal.com'}


FEED_TEMPLATES = {
    r'.*\.livejournal\.com/?.*': {
        'http://%(netloc)s/data/rss': 100,
        'https://%(netloc)s/data/rss': 200,
        'http://%(netloc)s/data/atom': 300,
        'https://%(netloc)s/data/atom': 400,
    },
}
