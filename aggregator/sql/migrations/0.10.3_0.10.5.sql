BEGIN EXCLUSIVE;

DROP INDEX IF EXISTS ix_url_id_url;

DROP INDEX IF EXISTS ix_item_id_hash;

ALTER TABLE
    url RENAME TO tmp;

CREATE TABLE url (
    id INTEGER NOT NULL,
    url VARCHAR NOT NULL,
    CONSTRAINT pk_url PRIMARY KEY (id)
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
    CONSTRAINT pk_keyword PRIMARY KEY (id)
);

INSERT INTO
    keyword (id, keyword)
SELECT
    *
FROM
    tmp;

DROP TABLE tmp;

CREATE TABLE tmp_tagging (url_id INTEGER, keyword_id INTEGER);

INSERT INTO
    tmp_tagging (url_id, keyword_id)
SELECT
    item.url AS url_id,
    tagging.keyword_id AS keyword_id
FROM
    tagging
    JOIN item ON tagging.item_id == item.id;

DELETE FROM
    tmp_tagging
WHERE
    rowid NOT IN (
        SELECT
            min(rowid)
        FROM
            tmp_tagging
        GROUP BY
            url_id,
            keyword_id
    );

DROP TABLE tagging;

CREATE TABLE tagging (
    id INTEGER NOT NULL,
    url_id INTEGER NOT NULL,
    keyword_id INTEGER NOT NULL,
    CONSTRAINT pk_tagging PRIMARY KEY (id),
    CONSTRAINT fk_tagging_url_id_url FOREIGN KEY(url_id) REFERENCES url (id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_tagging_keyword_id_keyword FOREIGN KEY(keyword_id) REFERENCES keyword (id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

INSERT INTO
    tagging (url_id, keyword_id)
SELECT
    *
FROM
    tmp_tagging;

DROP TABLE tmp_tagging;

ALTER TABLE
    item RENAME TO tmp;

DELETE FROM
    tmp
WHERE
    rowid NOT IN (
        SELECT
            min(rowid)
        FROM
            tmp
        GROUP BY
            url
    );

CREATE TABLE item (
    id INTEGER NOT NULL,
    url INTEGER NOT NULL,
    source INTEGER NOT NULL,
    title VARCHAR,
    author VARCHAR,
    published DATETIME,
    updated DATETIME,
    crawled FLOAT,
    CONSTRAINT pk_item PRIMARY KEY (id),
    CONSTRAINT fk_item_url_url FOREIGN KEY(url) REFERENCES url (id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_item_source_url FOREIGN KEY(source) REFERENCES url (id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

INSERT INTO
    item (
        id,
        url,
        source,
        title,
        author,
        published,
        updated,
        crawled
    )
SELECT
    id,
    url,
    source,
    title,
    author,
    published,
    updated,
    crawled
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
    dead BOOLEAN,
    CONSTRAINT pk_feed PRIMARY KEY (id),
    CONSTRAINT fk_feed_url_id_url FOREIGN KEY(url_id) REFERENCES url (id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT ck_feed_dead CHECK (dead IN (0, 1))
);

INSERT INTO
    feed (url_id, title)
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

CREATE UNIQUE INDEX IF NOT EXISTS ix_url_url ON url (url);

CREATE UNIQUE INDEX IF NOT EXISTS ix_keyword_keyword ON keyword (keyword);

CREATE UNIQUE INDEX IF NOT EXISTS ix_item_url ON item (url);

CREATE UNIQUE INDEX IF NOT EXISTS ix_hyperlink_source_id_target_id_element ON hyperlink (source_id, target_id, element);

CREATE UNIQUE INDEX IF NOT EXISTS ix_feed_url_id ON feed (url_id);

CREATE UNIQUE INDEX IF NOT EXISTS ix_tagging_url_id_keyword_id ON tagging (url_id, keyword_id);

CREATE UNIQUE INDEX IF NOT EXISTS ix_summary_url_id ON summary (url_id);

CREATE UNIQUE INDEX IF NOT EXISTS ix_webpage_url_id ON webpage (url_id);

UPDATE
    __version__
SET
    version = '0.10.5';

COMMIT;