BEGIN;

ALTER TABLE
    markup RENAME TO tmp;

CREATE TABLE IF NOT EXISTS summary (
    url_id INTEGER NOT NULL,
    markup TEXT NOT NULL,
    CONSTRAINT pk_summary PRIMARY KEY (url_id),
    CONSTRAINT fk_summary_url_id_url FOREIGN KEY(url_id) REFERENCES url (id)
);

CREATE TABLE IF NOT EXISTS webpage (
    url_id INTEGER NOT NULL,
    markup TEXT NOT NULL,
    CONSTRAINT pk_webpage PRIMARY KEY (url_id),
    CONSTRAINT fk_webpage_url_id_url FOREIGN KEY(url_id) REFERENCES url (id)
);

INSERT INTO
    summary (url_id, markup)
SELECT
    url.id AS url_id,
    tmp.markup AS markup
FROM
    tmp
    JOIN item ON tmp.item_id == item.id
    JOIN url ON item.url == url.id
WHERE
    tmp.type == 'summary'
GROUP BY
    url_id;

INSERT
    OR REPLACE INTO summary (url_id, markup)
SELECT
    url.id AS url_id,
    tmp.markup AS markup
FROM
    tmp
    JOIN item ON tmp.item_id == item.id
    JOIN url ON item.url == url.id
WHERE
    tmp.type == 'content'
GROUP BY
    url_id;

DROP TABLE tmp;

UPDATE
    __version__
SET
    version = '0.10.3';

END;