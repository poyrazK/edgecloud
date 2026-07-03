-- +migrate Down
-- 015_webhooks.down.sql
DROP TABLE IF EXISTS webhook_deliveries CASCADE;
DROP TABLE IF EXISTS webhooks CASCADE;
