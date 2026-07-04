-- +migrate Up
-- PR #133 review finding #4: add FOREIGN KEY … ON DELETE CASCADE from
-- domains(tenant_id, app_name) to apps(tenant_id, name). The apps
-- table already carries UNIQUE (tenant_id, name) at 004_apps.up.sql:7,
-- which satisfies the FK's required unique index.
--
-- Why: previously, deleting an app (via AppRepository.AtomicDelete)
-- left orphan `domains` rows whose FQDN continued to authorize TLS
-- cert issuance through Caddy's on_demand.ask callback. The cascading
-- FK ensures app deletion propagates to domains rows for the same
-- (tenant, app).
--
-- Pre-flight for non-pristine databases: if existing domains rows
-- reference an (tenant_id, app_name) that no longer exists in apps,
-- this ALTER will fail. To clean up before re-running:
--
--   DELETE FROM domains d
--   WHERE NOT EXISTS (
--       SELECT 1 FROM apps a
--       WHERE a.tenant_id = d.tenant_id AND a.name = d.app_name
--   );

-- Idempotent FK: ADD CONSTRAINT has no IF NOT EXISTS in PG, so we
-- DROP IF EXISTS + ADD. The DROP is a no-op the first time and
-- clears the constraint on subsequent re-applies (the test suite's
-- idempotency check wipes gorp_migrations and re-runs every Up).
ALTER TABLE domains DROP CONSTRAINT IF EXISTS fk_domains_app;
ALTER TABLE domains
    ADD CONSTRAINT fk_domains_app
    FOREIGN KEY (tenant_id, app_name)
    REFERENCES apps(tenant_id, name)
    ON DELETE CASCADE;