-- +migrate Down
ALTER TABLE active_deployments DROP COLUMN IF EXISTS desired_replicas;
ALTER TABLE deployments DROP COLUMN IF EXISTS desired_replicas;
