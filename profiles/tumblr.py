ALLOWED_DOMAINS = {'tumblr.com'}

FEED_TEMPLATES = {
    r'.*\.tumblr\.com/?.*': {
        'http://%(netloc)s/rss': 100,
        'http://%(netloc)s/rss#_=_': 200,
        'https://%(netloc)s/rss': 300,
        'https://%(netloc)s/rss#_=_': 400,
    },
}
