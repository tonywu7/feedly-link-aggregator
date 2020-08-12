# feedly-stream-reader

A Scrapy project for consuming feedly's [Streams API](https://developer.feedly.com/v3/streams/).

### Usage

```bash
> git clone https://github.com/monotony113/feedly-stream-reader.git
> cd feedly-stream-reader
```

You'll need a Python 3 environment.

Install dependencies (namely Scrapy and optionally `click` for CLI usage):

```bash
> pip install -r requirements.txt
```

Then start crawling:

```bash
> scrapy crawl feedly_rss -a feed=[url] -a output=[dir]
```

where `[url]` is the URL to your RSS feed, and `[dir]` is the path to the directory where crawled data will be saved.
For example, 

```bash
> scrapy crawl feedly_rss -a feed="https://xkcd.com/atom.xml" -a output=instance/xkcd
```

The feed URL must be the actual RSS feed location that returns RSS/Atom XML data (i.e. a path to the homepage won't work).
(Don't forget to properly quote your URL!)

After it's finished, run the following to list all external links found in webpage data provided by feedly:

```bash
> python3 -m feedly collect-urls [dir]
```

where `[dir]` is the same directory where crawled data are saved.

You may then pipe the URLs to your choice of downloader.

(Note: you should specify a different directory for each RSS feed you want to download, otherwise your previous crawls will be overwritten.)

### Notes

- `feedly.com` has a `robots.txt` policy that disallows bots. Therefore, this crawler is set to disobey `robots.txt` (even though
what it is doing isn't crawling so much as it is consuming data from a publicly available API).
- See the full feedly cloud API at [developer.feedly.com](https://developer.feedly.com).
- Written using Python 3.8, although it shouldn't have problems running on Python 3.7+
- The availability of the crawled data depend on feedly. If no one has ever subscribed to the RSS feed you are
trying to crawl on feedly, then your crawl may not yield any result.
- Similarly, the data you can crawl from feedly is only as complete as how much feedly has crawled your RSS feed.

### Motivation

I wrote this project originally because I found out that feedly caches a significant amount of data from dead Tumblr blogs :)

Basically:

1. Tumblr did not actually delete most of the media files in the Great Tumblr Purge, but rather merely removed the posts
containing them, meaning those media files are still available on the internet, albeit obscured behind their CDN URLs
(those `**.media.tumblr.com` links).
2. feedly differs from ordinary RSS readers in that it caches data from RSS feeds so that people who subscribe to the same 
RSS feed receive data from feedly first instead of directly from the RSS provider when they are using feedly.
3. Among the data that feedly caches are HTML snippets of each page in the RSS feed, which include our Tumblr media links
–– and _feedly doesn't seem to delete them even when the original posts are no longer available._

And so, effectively, feedly has been acting as a huge Tumblr cache for as long as it has implemented such
a content-delivery strategy and people have been using it to subscribe to Tumblr blogs ;)

This project is however usable for any RSS blogs that feedly has ever crawled (e.g. [`https://xkcd.com/atom.xml`](https://xkcd.com/atom.xml)),
or even other feedly content (see their Streams API for details).
