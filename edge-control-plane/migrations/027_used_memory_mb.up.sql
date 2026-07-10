-- +migrate Up
-- 027_used_memory_mb.up.sql
--
-- Issue #44 (part 2): enforce MaxMemoryMB as a deploy-time quota cap.
--
-- Adds the per-tenant aggregate memory counter paired with
-- quota.MaxMemoryMB. The deploy-time gate rejects a new deploy when
-- used_memory_mb + per_app_memory > MaxMemoryMB. The counter is
-- mutated transactionally by the activate / rollback / promote paths
-- in internal/service/deployment.go, mirroring how used_outbound_bytes
-- and used_request_count move on every heartbeat.
--
-- default 0 preserves all existing rows. NOT NULL keeps the
-- WHERE-clause math (used_memory_mb + $N <= max_memory_mb) simple —
-- no NULL-coalesce needed in the gate. No backfill is bundled here:
-- on a hot upgrade, tenants with pre-existing active deployments will
-- under-report until those apps are next activate / rollback'd. The
-- deploy-time gate may over-accept on the first deploy after upgrade,
-- and the next deploy catches the over-cap state.

ALTER TABLE quotas ADD COLUMN used_memory_mb BIGINT NOT NULL DEFAULT 0;

-- Defense-in-depth: used_memory_mb only legitimately moves in one
-- direction. The activate path increments by +perApp, the rollback
-- path increments by ±perApp via the active-row swap, and no other
-- write path touches it. A negative value can only result from a
-- bug — a stray operator UPDATE, a double-decrement, or a future
-- refactor that drops the per-tenant scope. The CHECK surfaces such
-- bugs as constraint violations at write time instead of silent
-- drift. Idempotent re-apply follows the 005_api_key_hash_algorithm
-- pattern: DROP IF EXISTS + ADD.
ALTER TABLE quotas DROP CONSTRAINT IF EXISTS quotas_used_memory_mb_nonneg;
ALTER TABLE quotas ADD CONSTRAINT quotas_used_memory_mb_nonneg
    CHECK (used_memory_mb >= 0);
