-- +migrate Up
-- Issue #316: per-deployment replica count for intra-region HA.
-- Workers now operate in fan-out mode (no queue group), so every
-- worker receives every TaskMessage. The desired_replicas column
-- is a monitoring threshold: the reconcile loop warns when fewer
-- than desired_replicas workers heartbeat the app.
--
-- A zero or NULL value means "no threshold" (default behavior):
-- the reconcile loop won't warn about under-replication for this app.

ALTER TABLE active_deployments
    ADD COLUMN IF NOT EXISTS desired_replicas INT NOT NULL DEFAULT 0;

ALTER TABLE deployments
    ADD COLUMN IF NOT EXISTS desired_replicas INT NOT NULL DEFAULT 0;
