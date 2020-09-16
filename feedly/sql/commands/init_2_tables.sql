
CREATE TABLE IF NOT EXISTS __version__ (
	version VARCHAR NOT NULL, 
	CONSTRAINT pk___version__ PRIMARY KEY (version)
)

;

CREATE TABLE IF NOT EXISTS url (
	id INTEGER NOT NULL, 
	url VARCHAR NOT NULL, 
	CONSTRAINT pk_url PRIMARY KEY (id), 
	CONSTRAINT uq_url_url UNIQUE (url)
)

;

CREATE TABLE IF NOT EXISTS keyword (
	id INTEGER NOT NULL, 
	keyword VARCHAR NOT NULL, 
	CONSTRAINT pk_keyword PRIMARY KEY (id), 
	CONSTRAINT uq_keyword_keyword UNIQUE (keyword)
)

;

CREATE TABLE IF NOT EXISTS item (
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
)

;

CREATE TABLE IF NOT EXISTS hyperlink (
	source_id INTEGER NOT NULL, 
	target_id INTEGER NOT NULL, 
	element VARCHAR NOT NULL, 
	CONSTRAINT pk_hyperlink PRIMARY KEY (source_id, target_id), 
	CONSTRAINT fk_hyperlink_source_id_url FOREIGN KEY(source_id) REFERENCES url (id), 
	CONSTRAINT fk_hyperlink_target_id_url FOREIGN KEY(target_id) REFERENCES url (id)
)

;

CREATE TABLE IF NOT EXISTS feed (
	url_id INTEGER NOT NULL, 
	title TEXT NOT NULL, 
	CONSTRAINT pk_feed PRIMARY KEY (url_id), 
	CONSTRAINT fk_feed_url_id_url FOREIGN KEY(url_id) REFERENCES url (id)
)

;

CREATE TABLE IF NOT EXISTS tagging (
	item_id INTEGER NOT NULL, 
	keyword_id INTEGER NOT NULL, 
	CONSTRAINT pk_tagging PRIMARY KEY (item_id, keyword_id), 
	CONSTRAINT fk_tagging_item_id_item FOREIGN KEY(item_id) REFERENCES item (id), 
	CONSTRAINT fk_tagging_keyword_id_keyword FOREIGN KEY(keyword_id) REFERENCES keyword (id)
)

;

CREATE TABLE IF NOT EXISTS summary (
	url_id INTEGER NOT NULL, 
	markup TEXT NOT NULL, 
	CONSTRAINT pk_summary PRIMARY KEY (url_id), 
	CONSTRAINT fk_summary_url_id_url FOREIGN KEY(url_id) REFERENCES url (id)
)

;

CREATE TABLE IF NOT EXISTS webpage (
	url_id INTEGER NOT NULL, 
	markup TEXT NOT NULL, 
	CONSTRAINT pk_webpage PRIMARY KEY (url_id), 
	CONSTRAINT fk_webpage_url_id_url FOREIGN KEY(url_id) REFERENCES url (id)
)

;
