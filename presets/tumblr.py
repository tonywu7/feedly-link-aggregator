DEPTH_LIMIT = 2

FOLLOW_DOMAINS = {'tumblr.com'}
SELECT_FEED_STATE = 'dead+'


def converter(base, match):
    for scheme in ('http', 'https'):
        for ending in ('rss', 'rss#_=_'):
            yield f'{scheme}://{base.netloc}/{ending}'


def deactivated_converter(base, match):
    for scheme in ('http', 'https'):
        for ending in ('rss', 'rss#_=_'):
            yield f'{scheme}://{match.group(1)}.tumblr.com/{ending}'
    yield from converter(base, match)


RSS_TEMPLATES = {
    r'https?://(.*)-deactivated\d*\.tumblr\.com/?.*': deactivated_converter,
    r'.*\.tumblr\.com/?.*': converter,
}

TUMBLR_IGNORE = {
    'www.tumblr.com', 'staff.tumblr.com', 'tumblr.com',
    'engineering.tumblr.com', 'support.tumblr.com',
    'assets.tumblr.com',
}

CONTRIB_SPIDER_MIDDLEWARES = {
    'feedme.contrib.filters.KeywordPrioritizer': 500,
    'feedme.contrib.tumblr.TumblrFilter': 505,
}
