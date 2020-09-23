## Changelog

- **v0.10.5**
    - _On-the-fly persistence:_ Instead of writing scraped data to a temporary file, then digest that file once crawling is finished
    the program now write to databases while scraping, using a separate process.
    - ![#e5c07b](https://placehold.it/12/e5c07b/000000?text=+) This version introduces database schema changes. Database from
    v0.10.1 onwards can be upgraded to this version.
- **v0.10.3**
    - _Optimization:_ Persisting data to database now requires less memory (with a slight time trade-off).
    - ![#e5c07b](https://placehold.it/12/e5c07b/000000?text=+) This version introduces database schema changes. Database from
    v0.10.1 onwards can be upgraded to this version.
- **v0.10.2**
    - _Cluster spider algorithm:_ Cluster spider now do breadth-first crawls, meaning it will crawl feeds closer to the starting feed
    to completion before crawling feeds that are further away.
    - _Persistence:_ Now uses pickle to persist request to achieve more accurate resumption.
- **v0.10.1**
    - **![#f06073](https://placehold.it/12/f06073/000000?text=+) This version introduces API-breaking changes.**
    - _Command change:_ The commands for both crawling and exporting has changed. See the above sections for details.
    - _Output:_
        - All spiders now require the output path be an available directory.
        - All spiders now persist scraped data using SQLite databases.
        - It is possible to run any of the spiders multiple times on the same output directory, scraped data are automatically
        merged and deduplicated.
    - _Presets:_ You can now use presets to maintain different sets of crawling options. Since presets are Python files, you can
    also specify complex settings, such as custom URL filtering functions, that cannot be specified on the command line.
    - _URL templates:_ The search function introduced in v0.3 is now off by default, because it is discovered that Feedly's Search API
    is a lot more sensitive to high-volume requests. Instead of relying on search, you can specify URL templates that allow the spiders
    to attempt different variations of feed URLs.
    - _New cluster spider:_ A new spider that, instead of crawling a single feed, also attempts to crawl any website mentioned in the feed's
    content that might themselves be RSS feeds, resulting in a network of sites being crawled. (It's like search engine spiders but for RSS feeds.)
    - _Export sorting and format:_ The revamped export module lets you select and sort URLs into different files. You may now export in
    both plain-text lines and CSV format.
    - _Graph export:_ You may now export link data as GraphML graphs, useful for visualization and network analysis. _Requires `python-igraph`._
    _Install with `pip install -r requirements-graph.txt`_
- **v0.3**
    - _Fuzzy search:_ it's no longer necessary to specify the full URL to the RSS feed data. Spider now uses Feedly's Search API to
    determine the correct URL. This means that you can simply specify e.g. the website's domain name, and Feedly will resolve it for you.
    In case there are multiple matches, they will be printed so that you can choose one and try again.
- **v0.1**
    - _URL filtering:_ you can now specify what URLs to include/exclude when running the `collect-urls` command. For example:
    `--include tag=a --exclude domain=secure.bank.com` will print out all URLs found on HTML `<a>` tags, except for those whose
    domains or parent domains contain "secure.bank.com".
    - _Feedly keywords:_ Feedly keyword data are now included in the crawl data, which you can use for filtering when running `collect-url`, 
    using the `feedly_keyword=` filter. Additionally, there is a new `collect-keywords` command that lists all keywords found in a crawl.