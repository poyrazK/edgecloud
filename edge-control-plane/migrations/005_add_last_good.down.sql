-- +migrate Down
-- Reverse 005_add_last_good.up.sql.
ALTER TABLE active_deployments
    DROP COLUMN last_good_deployment_id;
