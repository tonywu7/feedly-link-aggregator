PRAGMA foreign_keys = OFF;

BEGIN EXCLUSIVE;

ALTER TABLE
    item RENAME TO tmp;

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
    CONSTRAINT fk_item_url_url FOREIGN KEY(url) REFERENCES url (id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_item_source_url FOREIGN KEY(source) REFERENCES url (id) ON DELETE RESTRICT ON UPDATE CASCADE
);

INSERT INTO
    item
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
    CONSTRAINT fk_hyperlink_source_id_url FOREIGN KEY(source_id) REFERENCES url (id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_hyperlink_target_id_url FOREIGN KEY(target_id) REFERENCES url (id) ON DELETE RESTRICT ON UPDATE CASCADE
);

INSERT INTO
    hyperlink
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
    CONSTRAINT fk_feed_url_id_url FOREIGN KEY(url_id) REFERENCES url (id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT ck_feed_dead CHECK (dead IN (0, 1))
);

INSERT INTO
    feed
SELECT
    *
FROM
    tmp;

DROP TABLE tmp;

ALTER TABLE
    tagging RENAME TO tmp;

CREATE TABLE tagging (
    id INTEGER NOT NULL,
    url_id INTEGER NOT NULL,
    keyword_id INTEGER NOT NULL,
    CONSTRAINT pk_tagging PRIMARY KEY (id),
    CONSTRAINT fk_tagging_url_id_url FOREIGN KEY(url_id) REFERENCES url (id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_tagging_keyword_id_keyword FOREIGN KEY(keyword_id) REFERENCES keyword (id) ON DELETE RESTRICT ON UPDATE CASCADE
);

INSERT INTO
    tagging
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
    CONSTRAINT fk_summary_url_id_url FOREIGN KEY(url_id) REFERENCES url (id) ON DELETE RESTRICT ON UPDATE CASCADE
);

INSERT INTO
    summary
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
    CONSTRAINT fk_webpage_url_id_url FOREIGN KEY(url_id) REFERENCES url (id) ON DELETE RESTRICT ON UPDATE CASCADE
);

INSERT INTO
    webpage
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
    version = '0.10.6';

COMMIT;

PRAGMA foreign_keys = ON;
