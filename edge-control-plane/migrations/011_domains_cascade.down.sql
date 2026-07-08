-- +migrate Down
-- Reverse of 011_domains_cascade.up.sql. Drops the cascading FK
-- constraint; the orphan-on-delete behaviour returns.

ALTER TABLE domains DROP CONSTRAINT fk_domains_app;