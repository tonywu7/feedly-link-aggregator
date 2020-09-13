from urllib.parse import urlsplit

FOLLOW_DOMAINS = {'tumblr.com'}

FEED_TEMPLATES = {
    r'.*\.tumblr\.com/?.*': {
        'http://%(netloc)s/rss': 100,
        'http://%(netloc)s/rss#_=_': 200,
        'https://%(netloc)s/rss': 300,
        'https://%(netloc)s/rss#_=_': 400,
    },
}


ignored_tumblrs = {
    'www.tumblr.com', 'staff.tumblr.com', 'tumblr.com',
    'engineering.tumblr.com', 'support.tumblr.com',
    'assets.tumblr.com',
}


def filter_tumblr(request, spider):
    feed_url = request.meta.get('feed_url') or request.meta.get('search_query')
    if not feed_url:
        return True
    domain = urlsplit(feed_url).netloc
    if domain in ignored_tumblrs:
        return False
    if domain[-16:] == 'media.tumblr.com':
        return False
    return True


REQUEST_FILTERS = {
    filter_tumblr: 200,
}
