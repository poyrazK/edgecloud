-- +migrate Down
-- 013_quotas_used_requests.down.sql
ALTER TABLE quotas DROP COLUMN used_request_count;
ALTER TABLE quotas DROP COLUMN max_requests_per_month;