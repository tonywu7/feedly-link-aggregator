FOLLOW_DOMAINS = {'tumblr.com'}
FEED_STATE_SELECT = 'dead+'


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

PRIORITIZED_KEYWORDS = {
    10: ['cats', 'kitties'],
    5: ['dogs', 'puppies'],
    -5: ['goldfish'],
    -float('inf'): ['rat'],
}

CONTRIB_SPIDER_MIDDLEWARES = {
    'aggregator.contrib.filters.KeywordPrioritizer': 500,
    'aggregator.contrib.tumblr.TumblrFilter': 505,
}
