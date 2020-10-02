# MIT License
#
# Copyright (c) 2020 Tony Wu <tony[dot]wu(at)nyu[dot]edu>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from __future__ import annotations

from ..docs import OptionsContributor
from .base import FeedlyRSSSpider


class FeedSpider(FeedlyRSSSpider, OptionsContributor, _doc_order=10):
    """
    Spider to crawl a single feed.

    Usage
    -----
    `scrapy crawl feed -s OPTIONS=... ...`
    """

    name = 'feed'

    def start_requests(self):
        return super().start_requests()

    @staticmethod
    def _help_options():
        return {
            'OUTPUT': """
            Path where scraped data will be saved; will be a directory
            If an existing directory with scraped data is specified, newly gathered
            data will be merged with existing one.
            """,
            'RSS': """
            URL to the RSS feed you would like to scrape.
            Must contain the protocol part of the URL, e.g. `http://`.
            """,
            'DOWNLOAD_ORDER': """
            The part of the feed to download first: either `oldest` or `newest`
            """,
            'DOWNLOAD_PER_BATCH': """
            Number of entries to download per API request. The minimum is 1 and the maximum is 1000.
            """,
            'RSS_TEMPLATES': """
            Templates to generate different versions of RSS URLs based on the value of the RSS setting.

            Because Feedly sometimes store an RSS feed's source URL with slight variations (e.g. using
            HTTP instead of HTTPS), the URL that you provide above may yield incomplete results
            (sometimes no result at all).

            If you know how the URLs could vary, this option allows you to define URL templates,
            so that Scrapy can try different versions of URLs to increase the chance of finding the
            correct feed ID on Feedly.

            This option should be a mapping (a dict), where the key should be a valid regular
            expression that matches the URLs you wish to apply the corresponding the templates, and the
            value should be either another mapping, where the key is a %-format string with named placeholders,
            which will be formatted into the final URL and the value is a number that denotes the priority of the
            template: templates with a lower number are tried first (similar to how Scrapy middlewares are ordered).

            Or it could also be a callable, in which case it is passed the matched URL as a `urlsplit` tuple,
            and the regex match object, and it should return an iterable.

            Note that only the templates under the first matching pattern are used. Since dicts are ordered
            you should place more specific patterns at the top of the mapping.

            Available placeholders are:
            **The components of a urllib.parse.urlsplit named tuple:**
                ~%(scheme)s~          - Network protocol (usually `http` or `https`)
                ~%(netloc)s~          - Domain name
                ~%(path)s~            - Path of the URL, with leading / and without the query string
                ~%(query)s~           - Query string, without the question mark (`key1=value1&key2=value2...`)
            **Plus some convenient values:**
                ~%(original)s~        - The original string, unchanged
                ~%(network_path)s~    - URL minus the protocol part, equivalent to `//%(netloc)s/%(path)s?%(query)s`
                ~%(path_query)s~      - URL minus protocol and domain name, equivalent to `/%(path)s?%(query)s`
            **If you define capture groups in your pattern:**
                ~%(key)s ...~         - Named groups
                ~%(1)s, %(2)s~        - Numbered groups

            **Example**:

                `RSS_TEMPLATES = {`
                `    r'.*': {  # This regular expression will match any strings`
                `        'http:%(network_path)s': 997,`
                `        'https:%(network_path)s': 998,`
                `        '%(original)s': 999,`
                `    },`
                `}`
            """,
            'ENABLE_SEARCH': """
            Whether or not to enable the search function
            If enabled, when the feed URL you provided above does not yield any result from Feedly,
            Scrapy will use Feedly's Search API to try to find the correct URL.

            It is recommended that you disable search when using the cluster spider, because it could generate
            a large number of search requests, and Feedly's Search API is a lot more sensitive to
            high volume requests than its Streams API, meaning you may quickly run into rate-limiting issues.
            """,
            'ACCESS_TOKEN': """
            If you have a developer access token, you can provide it here.
            """,
        }
