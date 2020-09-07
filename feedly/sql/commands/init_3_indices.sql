CREATE INDEX IF NOT EXISTS ix_url_id_url ON url (id, url);
CREATE INDEX IF NOT EXISTS ix_item_id_hash ON item (id, hash);
CREATE UNIQUE INDEX IF NOT EXISTS ix_markup_item_id_type ON markup (item_id, type);
