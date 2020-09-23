BEGIN;

ALTER TABLE
    url RENAME TO tmp;

CREATE TABLE url (
    id INTEGER NOT NULL,
    url VARCHAR NOT NULL,
    CONSTRAINT pk_url PRIMARY KEY (id),
    CONSTRAINT uq_url_url UNIQUE (url)
);

INSERT INTO
    url (id, url)
SELECT
    *
FROM
    tmp;

DROP TABLE tmp;

ALTER TABLE
    keyword RENAME TO tmp;

CREATE TABLE keyword (
    id INTEGER NOT NULL,
    keyword VARCHAR NOT NULL,
    CONSTRAINT pk_keyword PRIMARY KEY (id),
    CONSTRAINT uq_keyword_keyword UNIQUE (keyword)
);

INSERT INTO
    keyword (id, keyword)
SELECT
    *
FROM
    tmp;

DROP TABLE tmp;

ALTER TABLE
    item RENAME TO tmp;

CREATE TABLE item (
    id INTEGER NOT NULL,
    hash VARCHAR(40) NOT NULL,
    url INTEGER NOT NULL,
    source INTEGER NOT NULL,
    title VARCHAR,
    author VARCHAR,
    published DATETIME,
    updated DATETIME,
    crawled FLOAT,
    CONSTRAINT pk_item PRIMARY KEY (id),
    CONSTRAINT uq_item_hash UNIQUE (hash),
    CONSTRAINT fk_item_url_url FOREIGN KEY(url) REFERENCES url (id),
    CONSTRAINT fk_item_source_url FOREIGN KEY(source) REFERENCES url (id)
);

INSERT INTO
    item (
        id,
        hash,
        url,
        source,
        title,
        author,
        published,
        updated,
        crawled
    )
SELECT
    *
FROM
    tmp;

DROP TABLE tmp;

ALTER TABLE
    hyperlink RENAME TO tmp;

CREATE TABLE hyperlink (
    id INTEGER NOT NULL,
    source_id INTEGER NOT NULL,
    target_id INTEGER NOT NULL,
    element VARCHAR NOT NULL,
    CONSTRAINT pk_hyperlink PRIMARY KEY (id),
    CONSTRAINT fk_hyperlink_source_id_url FOREIGN KEY(source_id) REFERENCES url (id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_hyperlink_target_id_url FOREIGN KEY(target_id) REFERENCES url (id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

INSERT INTO
    hyperlink (source_id, target_id, element)
SELECT
    *
FROM
    tmp;

DROP TABLE tmp;

ALTER TABLE
    feed RENAME TO tmp;

CREATE TABLE feed (
    id INTEGER NOT NULL,
    url_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    CONSTRAINT pk_feed PRIMARY KEY (id),
    CONSTRAINT fk_feed_url_id_url FOREIGN KEY(url_id) REFERENCES url (id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

INSERT INTO
    feed (url_id, title)
SELECT
    *
FROM
    tmp;

DROP TABLE tmp;

ALTER TABLE
    tagging RENAME TO tmp;

CREATE TABLE tagging (
    id INTEGER NOT NULL,
    item_id INTEGER NOT NULL,
    keyword_id INTEGER NOT NULL,
    CONSTRAINT pk_tagging PRIMARY KEY (id),
    CONSTRAINT fk_tagging_item_id_item FOREIGN KEY(item_id) REFERENCES item (id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_tagging_keyword_id_keyword FOREIGN KEY(keyword_id) REFERENCES keyword (id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

INSERT INTO
    tagging (item_id, keyword_id)
SELECT
    *
FROM
    tmp;

DROP TABLE tmp;

ALTER TABLE
    summary RENAME TO tmp;

CREATE TABLE summary (
    id INTEGER NOT NULL,
    url_id INTEGER NOT NULL,
    markup TEXT NOT NULL,
    CONSTRAINT pk_summary PRIMARY KEY (id),
    CONSTRAINT fk_summary_url_id_url FOREIGN KEY(url_id) REFERENCES url (id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

INSERT INTO
    summary (url_id, markup)
SELECT
    *
FROM
    tmp;

DROP TABLE tmp;

ALTER TABLE
    webpage RENAME TO tmp;

CREATE TABLE webpage (
    id INTEGER NOT NULL,
    url_id INTEGER NOT NULL,
    markup TEXT NOT NULL,
    CONSTRAINT pk_webpage PRIMARY KEY (id),
    CONSTRAINT fk_webpage_url_id_url FOREIGN KEY(url_id) REFERENCES url (id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

INSERT INTO
    webpage (url_id, markup)
SELECT
    *
FROM
    tmp;

DROP TABLE tmp;

DROP INDEX IF EXISTS ix_url_id_url;

DROP INDEX IF EXISTS ix_item_id_hash;

CREATE UNIQUE INDEX ix_url_url ON url (url);

CREATE UNIQUE INDEX ix_keyword_keyword ON keyword (keyword);

CREATE UNIQUE INDEX ix_item_hash ON item (hash);

CREATE UNIQUE INDEX ix_hyperlink_source_id_target_id_element ON hyperlink (source_id, target_id, element);

CREATE UNIQUE INDEX ix_feed_url_id ON feed (url_id);

CREATE UNIQUE INDEX ix_tagging_item_id_keyword_id ON tagging (item_id, keyword_id);

CREATE UNIQUE INDEX ix_summary_url_id ON summary (url_id);

CREATE UNIQUE INDEX ix_webpage_url_id ON webpage (url_id);

UPDATE
    __version__
SET
    version = '0.10.5';

END;