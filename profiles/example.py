from datetime import datetime
from urllib.parse import urlsplit

from scrapy import Request, Spider

# Path where crawled data will be saved
OUTPUT = f'./{datetime.now().strftime("%Y%m%d%H%M%S")}.crawl.json'

# If True, Scrapy will overwrite OUTPUT if it already exists; if False, Scrapy will raise an Exception instead.
OVERWRITE = False

# The URL to the RSS feed (usually specified on the command line)
FEED = 'https://xkcd.com/atom.xml'

# Which part of the feed to download first: either `oldest` or `newest`
DOWNLOAD_ORDER = 'oldest'
# How much Feedly entries to download per API request. The minimum is 20 and the maximum is 1000.
DOWNLOAD_PER_BATCH = 1000

# Whether or not to enable the search function
# If enabled, when the feed URL you provided above does not yield any result from Feedly,
# Scrapy will use Feedly's Search API to try to find the correct URL.
#
# It is recommended that you disable search when using the network spider, because it could generate
# a large number of search requests, and Feedly's Search API is a lot more sensitive to
# high volume requests than its Streams API. You may quickly run into rate-limiting issues.
FUZZY_SEARCH = False

# If you have Feedly's developer access token, you can provide it here.
ACCESS_TOKEN = None

# Network spider related option.
# Value should be a set of domains.
# Only nodes whose domain or parent domain is included here will be expanded upon
# (they still get recorded, but will not be used to find new feeds).
# If set to None, spider will not filter nodes based on domains.
ALLOWED_DOMAINS = None
# Network spider related option.
# This is the same settings as the one used by the built-in DepthMiddleware.
# Value should be an integer.
# Nodes that are more `depth` degree removed from the starting feed will not be expanded upon.
# If set to 0, only the starting feed will be crawled.
# If set to None, spider will keep crawling further and further until manually stopped.
DEPTH_LIMIT = 1

# Templates to generate different versions of RSS URLs based on the value of the FEED setting.
# Because Feedly sometimes store an RSS feed's source URL with slight variations (e.g. using HTTP instead of HTTPS),
# the URL that you provide above may not yield any result.
#
# If you know how the URLs may vary, this option allows you to define URL templates,
# so that Scrapy can try different versions of URLs to increase the chance of finding the correct feed ID on Feedly.
#
# This option should be a mapping (a dict), where
# the key should be a valid regular expression that matches the URLs you wish to apply the corresponding the templates,
#
# and the value should be another mapping,
# where the key is a C-style format string with named placeholders, which will turn into the final URL with the % operator
# ('%(key)s' % {'key': 'value'})
# and the value is a number that denotes the priority of the template: templates with a lower number are tried first (similar to how
# Scrapy middlewares are ordered)
#
# The available placeholders are:
# The components of a urllib.parse.urlsplit named tuple:
#   %(scheme)s          - Network protocol (usually `http` or `https`)
#   %(netloc)s          - Domain name
#   %(path)s            - Path of the URL, with leading / and without the query string
#   %(query)s           - Query string, without the question mark (`key1=value1&key2=value2...`)
# Plus some convenient values:
#   %(original)s        - The original string, unchanged
#   %(network_path)s    - URL minus the protocol part, equivalent to `//%(netloc)s/%(path)s?%(query)s`
#   %(path_query)s      - URL minus protocol and domain name, equivalent to `/%(path)s?%(query)s`
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
#   feed_query      - A string, usually a URL, that describes a feed. This could be either the user-supplied URL, or in the
#                     context of the network spider, domain names extracted from existing feeds that will be used to expand the network.
#   source_item     - In the context of the network spider, if a potential new RSS feed is discovered and is about to be crawled,
#                     this attribute will contain the FeedlyEntry object from which the new feed is found.
#                     The object will contain useful information such as keywords and HTML markups.
REQUEST_FILTERS = {
    https_only: 100,
}

STREAM_ID_PREFIX = 'feed/'


def keyword_prioritizer(request, spider):
    item = request.meta.get('source_item')
    if not item:
        return True
    weight = 0
    kws = {k.lower() for k in item.keywords}
    for w, keys in weighted_keywords.items():
        for k in keys:
            if k in kws or k in item.url:
                weight += w
    if not weight:
        return True
    return request.replace(priority=request.priority + weight)


weighted_keywords = {
    -1: [],
    1: [],
}
