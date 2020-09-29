from urllib.parse import urlsplit

from scrapy import Request
from scrapy.exceptions import NotConfigured

from ..docs import OptionsContributor


class TumblrFilter(OptionsContributor, _doc_order=-5):
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        self.domains = settings.get('TUMBLR_IGNORE')
        if not self.domains:
            raise NotConfigured()

    def process_spider_output(self, response, result, spider):
        for r in result:
            if not isinstance(r, Request):
                yield r
                continue

            feed_url = r.meta.get('feed_url')
            if not feed_url:
                yield r
                continue

            domain = urlsplit(feed_url).netloc
            if domain in self.domains:
                continue
            if domain[-16:] == 'media.tumblr.com':
                continue
            yield r

    @staticmethod
    def _help_options():
        return {
            'TUMBLR_IGNORE': """
            A list of Tumblr sites to ignore.

            **Example**
                `TUMBLR_IGNORE = {`
                `    'www.tumblr.com', 'staff.tumblr.com', 'tumblr.com',`
                `    'engineering.tumblr.com', 'support.tumblr.com',`
                `    'assets.tumblr.com',`
                `}`
            """,
        }
