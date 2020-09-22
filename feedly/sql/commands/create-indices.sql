CREATE UNIQUE INDEX IF NOT EXISTS ix_url_url ON url (url);
CREATE UNIQUE INDEX IF NOT EXISTS ix_keyword_keyword ON keyword (keyword);
CREATE UNIQUE INDEX IF NOT EXISTS ix_item_hash ON item (hash);
CREATE UNIQUE INDEX IF NOT EXISTS ix_hyperlink_source_id_target_id_element ON hyperlink (source_id, target_id, element);
CREATE UNIQUE INDEX IF NOT EXISTS ix_feed_url_id ON feed (url_id);
CREATE UNIQUE INDEX IF NOT EXISTS ix_tagging_item_id_keyword_id ON tagging (item_id, keyword_id);
CREATE UNIQUE INDEX IF NOT EXISTS ix_summary_url_id ON summary (url_id);
CREATE UNIQUE INDEX IF NOT EXISTS ix_webpage_url_id ON webpage (url_id);
