DEPTH_LIMIT = 2

FOLLOW_DOMAINS = {'wordpress.com'}


def template(base, match):
    for scheme in ('http', 'https'):
        for ending in ('?feed=rss', '?feed=rss2', '?feed=rdf', '?feed=atom'
                       'feed/', 'feed/rss/', 'feed/rss2/', 'feed/rdf/', 'feed/atom/'):
            yield f'{scheme}://{base.netloc}/{ending}'


RSS_TEMPLATES = {
    r'.*\.wordpress\.com/?.*': template,
}
