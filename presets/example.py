from datetime import datetime
from urllib.parse import urlsplit

from scrapy import Request, Spider

# Path where scraped data will be saved; will be a directory
# If an existing directory with scraped data is specified, newly gathered
# data will be merged with existing one.
OUTPUT = f'./{datetime.now().strftime("%Y%m%d%H%M%S")}'

# The URL to the RSS feed
FEED = 'https://xkcd.com/atom.xml'

# Which part of the feed to download first: either `oldest` or `newest`
DOWNLOAD_ORDER = 'oldest'
# How many entries to download per API request. The minimum is 1 and the maximum is 1000.
DOWNLOAD_PER_BATCH = 1000

# Whether or not to enable the search function
# If enabled, when the feed URL you provided above does not yield any result from Feedly,
# Scrapy will use Feedly's Search API to try to find the correct URL.
#
# It is recommended that you disable search when using the cluster spider, because it could generate
# a large number of search requests, and Feedly's Search API is a lot more sensitive to
# high volume requests than its Streams API, meaning you may quickly run into rate-limiting issues.
ENABLE_SEARCH = False

# Whether or not to download and process the source webpage of a feed item.
# If disabled, spider will only process HTML snippets returned by Feedly, which contain mostly article summaries
# and sometimes images/videos, and will therefore only extract URLs from them.
# If enabled, then in addition to that, spider will also download a copy of the webpage from the source website of the feed
# which could contain many more hyperlinks, although the original webpage may not exist anymore.
SCRAPE_SOURCE_PAGE = False

# How much scraped data the program will keep in the memory before persisting them to the database.
# A lower setting puts less stress on the memory but causes more frequent disk writes.
# Note: frequency of database writes does not affect spider performance
# because it is done in a separate process.
# Setting this to 1 causes every record to be immediately written to the database;
# Setting this to 0 causes the program to keep all scraped data in memory until the spider stops.
DATABASE_CACHE_SIZE = 100000

# If you have a developer access token, you can provide it here.
ACCESS_TOKEN = None

# Cluster spider option.
# Only nodes whose domains or parent domains are included here will be expanded upon
# Value should be a collection of domains.
# (other nodes are still recorded, but are not used to find new feeds).
# If set to None, spider will not filter nodes based on domains.
FOLLOW_DOMAINS = None

# Cluster spider option.
# How much the spider will expand the cluster.
# Value should be an integer.
# (This is the same settings as the one used by the built-in DepthMiddleware.)
# Nodes that are more `depth + 1` degree removed from the starting feed will not be expanded upon.
# If set to 1, only the starting feed will be crawled.
# If set to 0 or None, spider will keep crawling until manually stopped.
DEPTH_LIMIT = 1

# Cluster spider option.
# Only crawl feeds that are of a certain `state`.
#
# A feed can be in one of two states:
# dead    - The feed URL is unreachable (e.g. timed out);
#           or a HEAD request returns a status code other than
#             200 OK, 206 Partial, or 405 Method Not Allowed;
#           or the Content-Type is anything other than that of a valid RSS feed
#             (text/xml, application/xml, application/rss+xml, application/rdf+xml, application/atom+xml).
# alive   - All other feeds are considered alive.
#
# This option accepts the following values:
# all     - Do not filter feeds based on their state
# dead    - Only crawl dead feeds
# alive   - Only crawl living feeds
# dead+   - Crawl all feeds, but dead feeds receive a higher priority
# alive+  - Crawl all feeds, but living feeds receive a higher priority
#
# Note that values other than `all` cause the spider to send a HEAD request to
# each feed URL about to be crawled, which will add extra overhead to the running time.
SELECT_FEED_STATE = 'all'

# Templates to generate different versions of RSS URLs based on the value of the FEED setting.
# Because Feedly sometimes store an RSS feed's source URL with slight variations (e.g. using HTTP instead of HTTPS),
# the URL that you provide above may yield incomplete results (sometimes no result at all).
#
# If you know how the URLs could vary, this option allows you to define URL templates,
# so that Scrapy can try different versions of URLs to increase the chance of finding the correct feed ID on Feedly.
#
# This option should be a mapping (a dict), where
# the key should be a valid regular expression that matches the URLs you wish to apply the corresponding the templates,
#
# and the value should be either another mapping,
# where the key is a %-format string with named placeholders, which will be formatted into the final URL
# and the value is a number that denotes the priority of the template: templates with a lower number are tried first (similar to how
# Scrapy middlewares are ordered),
#
# or it could also be a callable, in which case it is passed the matched URL as a `urlsplit` tuple, and the regex match
# object, and it should return an iterable.
#
# Note that only the templates under the first matching pattern are used. Since dicts are ordered you should place
# more specific patterns at the top of the mapping.
#
# Available placeholders are:
# The components of a urllib.parse.urlsplit named tuple:
#   %(scheme)s          - Network protocol (usually `http` or `https`)
#   %(netloc)s          - Domain name
#   %(path)s            - Path of the URL, with leading / and without the query string
#   %(query)s           - Query string, without the question mark (`key1=value1&key2=value2...`)
# Plus some convenient values:
#   %(original)s        - The original string, unchanged
#   %(network_path)s    - URL minus the protocol part, equivalent to `//%(netloc)s/%(path)s?%(query)s`
#   %(path_query)s      - URL minus protocol and domain name, equivalent to `/%(path)s?%(query)s`
# If you define capture groups in your pattern:
#   %(key)s ...         - Named groups
#   %(1)s, %(2)s        - Numbered groups
FEED_TEMPLATES = {
    r'.*': {  # This regular expression will match any strings
        'http:%(network_path)s': 997,
        'https:%(network_path)s': 998,
        '%(original)s': 999,
    },
}


# An example function that filter requests to only allow HTTPS requests, dropping others
def https_only(request: Request, spider: Spider):
    if urlsplit(request.url).scheme != 'https':
        return False
    return True


# A list of functions to use for filtering requests.
# Each function takes exactly 2 arguments: the Request object and the Spider instance,
# and should return True if the Request should continue, and False if the Request should be dropped.
# If a `Request` object is returned, it will be rescheduled.
# The Request.meta attribute will contain useful details about the request.
# Some common metadata are:
#   reason          - The reason this Request was fired, currently supported values are
#       `user_specified`    - First request to the starting feed
#       `newly_discovered`  - First request to a newly discovered feed (used in the network spider)
#       `continuation`      - Subsequent pages of a currently downloading feed
#       `search`            - A Request to Feedly's Search API
#   feed_url        - The final feed URL that will be sent to Feedly, this could be generated based on the URL templates defined above.
#   source_item     - In the context of the network spider, if a potential new RSS feed is discovered and is about to be crawled,
#                     this attribute will contain the FeedlyEntry object from which the new feed is found.
#                     The object will contain useful information such as keywords and HTML markups.
REQUEST_FILTERS = {
    https_only: 100,
}

STREAM_ID_PREFIX = 'feed/'


class KeywordPrioritizer:
    def __init__(self):
        self.priorities = {}
        self.weighted_keywords = {
            1: ['wanted'],
            -1: ['unwanted'],
        }
        self.weighted_keywords = {k: p for p, kws in self.weighted_keywords.items() for k in kws}
        self.starting_weight = -1

    def update_priority(self, item):
        site = urlsplit(item.url).netloc
        prio = self.priorities.setdefault(site, self.starting_weight)
        delta = 0
        phrases = [k.lower() for k in item.keywords]
        phrases.extend([item.markup.get('summary', ''), item.url, item.title.lower()])
        for kw, p in self.weighted_keywords.items():
            for s in phrases:
                if kw in s:
                    delta += p
        self.priorities[site] = prio + delta

    def __call__(self, request, spider):
        item = request.meta.get('source_item')
        feed_url = request.meta.get('feed_url')
        if not item or not feed_url:
            return True
        self.update_priority(item)
        prio = self.priorities.get(urlsplit(feed_url).netloc)
        if not prio:
            return True
        return request.replace(priority=request.priority + prio)

    def __hash__(self):
        if not self.weighted_keywords:
            raise NotImplementedError
        return hash((tuple(self.weighted_keywords.items())))
