# feedly-link-aggregator

A Scrapy project for collecting hyperlinks from RSS feeds using Feedly's [Streams API](https://developer.feedly.com/v3/streams/).

**Note⚠: This project provides a way to quickly aggregate resources such as images in an RSS feed for purposes such as archival work. If you are only looking to browse a feed and/or download a few things, it's more appropriate (and friendly) to use [Feedly](https://feedly.com) directly.**

### Usage

```bash
> git clone https://github.com/monotony113/feedly-link-aggregator.git
> cd feedly-link-aggregator
```

You'll need at least Python 3.7 (project written using Python 3.8.1).

Install dependencies:

```bash
> python3 -m pip install -r requirements.txt
```

Then start crawling:

```bash
> scrapy crawl feed_content -a feed='<url>' -a output='<json>'
```

where `<url>` is the URL to your RSS feed, and `<json>` is the path to the JSON file where crawled data will be saved.
For example, 

```bash
> scrapy crawl feed_content -a feed="https://xkcd.com/atom.xml" -a output=instance/xkcd.json
```

After it's finished, run the following to list all external links found in webpage data provided by Feedly:

```bash
> python -m feedly collect-urls '<json>'
```

where `<json>` is the same JSON file where crawled data are saved.

You may then pipe the URLs to your choice of downloader.

(Note: you should specify a different directory for each RSS feed you want to download, otherwise your previous crawls will be overwritten.)

### Finding out RSS feed URL from Feedly

If you no longer have access to the original RSS feed, but you are subscribed to the feed on Feedly, then you can retrieve the URL like this:

1. Visit the feed's page list, your window's location should look something like `https://feedly.com/i/subscription/feed%2Fhttps%3A%2F%2Fxkcd.com%2Fatom.xml`
2. Copy everything after the `subscription/feed%2F` part, you will get something like `https%3A%2F%2Fxkcd.com%2Fatom.xml`

### Notes

- `feedly.com` has a `robots.txt` policy that disallows bots. Therefore, this crawler is set to disobey `robots.txt` (even though
what it is doing isn't crawling so much as it is consuming data from a publicly available API).
- The availability of the crawled data depends on Feedly. If no one has ever subscribed to the RSS feed you are
trying to crawl on Feedly, then your crawl may not yield any result.
- Similarly, the data you can crawl from Feedly are only as complete as how much Feedly has crawled your RSS feed.
- Explore the Feedly Cloud API at [developer.feedly.com](https://developer.feedly.com).

### Changelog

- **v0.3, 2020/08/18**
    - _Fuzzy search:_ it's no longer necessary to specify the full URL to the RSS feed data. Spider now uses Feedly's Search API to
    determine the correct URL. This means that you can simply specify e.g. the website's domain name, and Feedly will resolve it for you.
    In case there are multiple matches, they will be printed so that you can choose one and try again.
- **2020/08/17**
    - _URL filtering:_ you can now specify what URLs to include/exclude when running the `collect-urls` command. For example: `--include tag=a --exclude domain=secure.bank.com` will print out all URLs found on HTML `<a>` tags, except for those whose domain or parent domain contains "secure.bank.com".
    - _Feedly keywords:_ Feedly keyword data are now included in the crawl data, which you can use for filtering when running `collect-url`, 
    using the `feedly_keyword=` filter. Additionally, there is a new `collect-keywords` command that lists all keywords found in a crawl.

### Motivation

I started this project because I found out that Feedly caches a significant amount of data from dead Tumblr blogs :)

Basically:

1. As you may have already known, Tumblr did not actually delete most of the media files in the Great Tumblr Purge, 
but rather merely removed the posts containing them, meaning those media files are still available on the internet, 
albeit obscured behind their CDN URLs (the `**.media.tumblr.com` links).
2. Feedly differs from ordinary RSS readers in that it caches data from RSS feeds so that people who subscribe to the same 
RSS feed receive data from Feedly first instead of directly from the RSS provider when they are using Feedly.
3. Among the data that Feedly caches are HTML snippets of each page in the RSS feed, which include our Tumblr media links
–– and _Feedly doesn't seem to delete them even when the original posts are no longer available._

And so, effectively, Feedly has been acting as a huge Tumblr cache for as long as it has implemented such
a content-delivery strategy and people have been using it to subscribe to Tumblr blogs ;)

This project is however usable for any RSS blogs that Feedly has ever crawled (e.g. [`https://xkcd.com/atom.xml`](https://xkcd.com/atom.xml)),
or even other Feedly APIs (see their Streams API for details).
