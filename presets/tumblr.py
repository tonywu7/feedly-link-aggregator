from urllib.parse import urlsplit

FOLLOW_DOMAINS = {'tumblr.com'}


def converter(base, match):
    for scheme in ('http', 'https'):
        for ending in ('rss', 'rss#_=_'):
            yield f'{scheme}://{base.netloc}/{ending}'


def deactivated_converter(base, match):
    for scheme in ('http', 'https'):
        for ending in ('rss', 'rss#_=_'):
            yield f'{scheme}://{match.group(1)}.tumblr.com/{ending}'
    yield from converter(base, match)


FEED_TEMPLATES = {
    r'https?://(.*)-deactivated\d*\.tumblr\.com/?.*': deactivated_converter,
    r'.*\.tumblr\.com/?.*': converter,
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
