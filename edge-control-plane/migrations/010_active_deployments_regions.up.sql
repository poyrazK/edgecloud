-- Issue #127: per-region publish state for cross-region artifact
-- replication. Records which regions an active_deployments row has
-- already been successfully published to, so retries are idempotent
-- and the 502 response can tell the caller exactly which regions
-- got the message and which failed.
--
-- The columns live on active_deployments (not deployments) because
-- publish state is bound to the activation event, not to the
-- artifact. A deployment that was activated once and rolled back
-- keeps its regions_published array on the (now-stale) active row;
-- the next activation writes a fresh row via the upsert in
-- ActiveDeploymentRepository.Set. The DO UPDATE branch of that
-- upsert also rewrites the four new columns, so a re-activation
-- starts with an empty publish history — matches the operator's
-- mental model ("I just activated, so no regions have been notified
-- yet for THIS activation").
--
-- regions_failed is distinct from regions_published because a NATS
-- publish may partially succeed (e.g. us-east OK, eu-west 5xx). The
-- service layer always re-publishes to regions_failed even if a
-- stale entry exists in regions_published (issue #127 Risk 3), so a
-- single transient NATS hiccup cannot permanently wedge a region.

ALTER TABLE active_deployments
    ADD COLUMN regions_published        TEXT[]      NOT NULL DEFAULT '{}',
    ADD COLUMN regions_failed           TEXT[]      NOT NULL DEFAULT '{}',
    ADD COLUMN last_publish_at          TIMESTAMPTZ,
    ADD COLUMN last_publish_attempt_id  UUID;
