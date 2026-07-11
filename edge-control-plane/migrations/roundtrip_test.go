//go:build integration
// +build integration

// Package migrations_test exercises every migration in this directory
// against a real Postgres via testcontainers, proving two contracts:
//
//  1. Forward apply: every *.sql file in this directory parses under
//     rubenv/sql-migrate and applies cleanly to a fresh database.
//     Catches malformed markers, CREATE INDEX CONCURRENTLY in a
//     default-wrapped transaction (migrate.go:540-548), and any
//     regressions in the SQL itself that the sqlmock-based repository
//     tests silently allow.
//
//  2. Round-trip reversibility: rolling all the way back and
//     reapplying succeeds. Catches asymmetries between an *.up.sql
//     body and its corresponding *.down.sql body — e.g. a migration
//     that adds a column without dropping it on rollback, leaving
//     subsequent reapply in an inconsistent state.
//
// This file is build-tag-gated so the default `go test ./...` CI run
// does NOT spin Docker. Run locally with:
//
//	cd edge-control-plane
//	go test -tags=integration -v -count=1 ./migrations/...
//
// CI runs it under `go-test-integration` (services: postgres:16-alpine).
// See .github/workflows/ci.yml.
//
// Note on split files: the migrations in this directory are stored as
// `*.up.sql` + `*.down.sql` pairs (with `-- +migrate Up` / `-- +migrate
// Down` markers inline, courtesy of PR #259). rubenv's FileMigrationSource
// reads every .sql file, so each pair produces TWO Migration records:
// one with the Up populated and Down nil, one with Down populated and
// Up nil. The apply/rollback counts are therefore 2N where N is the
// number of logical migrations. That's fine — the test just asserts
// consistency of the two passes (apply N pairs → rollback N pairs →
// re-apply N pairs → gorp_migrations has the same count).

package migrations_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/rubenv/sql-migrate"
	"github.com/stretchr/testify/require"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/testutil"
)

// splitFileCount is the number of *.sql files in this directory.
// Each logical migration has one .up.sql and one .down.sql, so the
// apply + rollback paths will track this many records in gorp_migrations.
// Update when adding a new migration pair.
//
// On current branch after merge of PR #466 (#42), PR #420 (quota
// grace columns → 025_quotas_grace_columns), issue #440 commit 6
// (026_active_deployments_activation_attempt_started_at), PR #534
// (027_used_memory_mb + 028_quota_memory_constraint etc.), PR #485
// (029_quotas_resident_seconds + 030_billing_usage_events), PR #439
// (031_active_deployment_idempotency_keys), issue #574 retention
// GCs (additional (created_at) indexes landed on existing migrations),
// and issue #305 (032_tenant_rate_limits — per-tenant data-plane rate
// limit storage):
// 42 .up.sql + 42 .down.sql = 84 split files. Some numeric prefixes
// collide (005_*, 009_*, 010_*, 017_*, 018_*, 025_*, 026_*, 027_*,
// 028_*, 029_*, 030_*, 031_*), so this is the on-disk file count,
// not a strict 2× the migration number.
// After rebasing PR #661 (issue #305, 032_tenant_rate_limits) onto
// main, the post-#430 figure is 47 .up.sql + 47 .down.sql + the
// #305 pair = 48 pairs (issue #305 adds 032_tenant_rate_limits).
// Some numeric prefixes collide (005_*, 009_*, 010_*, 017_*, 018_*,
// 025_*, 026_*, 027_*, 028_*, 029_*, 030_*, 031_*, 033_*), so this
// is the on-disk file count, not a strict 2× the migration number.
const splitFileCount = 96 // 48 .up.sql + 48 .down.sql after issue #305 + #430 (workers.public_key + length-cap)

// wantTables is the post-015 expected set of public-schema tables.
// Update when adding a migration that creates a new table. The
// roundtrip test asserts each is present after Up and absent after
// rolling back to v0.
var wantTables = []string{
	"tenants",
	"quotas",
	"api_keys",
	"deployments",
	"active_deployments",
	"app_env",
	"workers",
	"worker_status",
	"apps",
	"logs",
	"app_traffic_splits",
	"domains",
	"autoscale_events",
	"audit_logs",
	"webhooks",
	"webhook_deliveries",
	"billing_subscriptions",              // 022 (issue #419)
	"billing_events",                     // 023 (issue #419)
	"outbox",                             // 025 (issue #42)
	"idempotency_keys",                   // 026 (issue #52)
	"billing_usage_events",               // 030 (issue #485)
	"active_deployment_idempotency_keys", // 031 (issue #439)
}

// wantColumns enumerates the public-schema columns each table must
// have after the up migrations apply. Acts as a schema-vs-migration
// contract: if someone renames a column in a migration, drops one
// without updating this map, or splits a column across migrations and
// forgets one of the pieces, the UpAppliesAllAndCreatesTables subtest
// fails with the exact column that's missing.
//
// Update when:
//   - A migration adds a column to a tracked table → add the column
//     here, and ideally reference the migration number in the comment
//     so reviewers can trace the contract back to the schema change.
//   - A migration adds a new table → add a new entry.
//   - A migration renames or drops a column → rename/remove here in
//     the same PR so the test reflects the new shape.
//
// Note: this asserts column *existence*, not type, nullability, or
// constraint enforcement. For those, see the sibling maps wantTypes
// and wantNotNull in this file. Indexes are tracked separately in
// wantIndexes. The original migration files remain the source of
// truth for those properties — this test guards against accidental
// renames/drops, not against subtle type drift.
var wantColumns = map[string][]string{
	"tenants": {
		"id",
		"name",
		"plan",
		"allowlisted_destinations",
		"created_at",
		"overage_allowed_until", // 025_quotas_grace_columns (issue #420)
	},
	"quotas": {
		"tenant_id",
		"max_deployments",                // 001
		"max_apps",                       // 001
		"max_workers",                    // 001
		"max_memory_mb",                  // 001
		"max_outbound_mb",                // 001
		"used_outbound_bytes",            // 009_quotas_used_outbound
		"quota_period_start",             // 009_quotas_used_outbound
		"max_requests_per_month",         // 013
		"used_request_count",             // 013
		"quota_lock_grace_until",         // 025_quotas_grace_columns (issue #420)
		"used_memory_mb",                 // 027_used_memory_mb (issue #44 part 2)
		"max_resident_seconds_per_month", // 029_quotas_resident_seconds (issue #485)
		"used_resident_seconds",          // 029_quotas_resident_seconds (issue #485)
		"max_compute_ms_per_month",       // 031_quotas_compute_ms (issue #555)
		"used_compute_ms",                // 031_quotas_compute_ms (issue #555)
		"tenant_rate_limit_rps",          // 032_tenant_rate_limits (issue #305)
		"tenant_rate_limit_burst",        // 032_tenant_rate_limits (issue #305)
		"tenant_concurrent_limit",        // 032_tenant_rate_limits (issue #305)
		"tenant_bandwidth_bps",           // 032_tenant_rate_limits (issue #305)
		"tenant_rate_limit_set_at",       // 032_tenant_rate_limits (issue #305)
	},
	"api_keys": {
		"id",
		"tenant_id",
		"name",
		"key_hash",
		"role",
		"created_at",
		"last_used",
		"expires_at",
		"hash_algorithm", // 005_api_key_hash_algorithm
		"lookup_hash",    // 006_api_key_lookup_hash + 007 NOT NULL
	},
	"deployments": {
		"id",
		"tenant_id",
		"app_name",
		"status",
		"hash",
		"created_at",
		"regions",               // 008_deployments_regions
		"auto_rollback_enabled", // 009_add_auto_rollback
		"signature",             // 017_add_signature
		"signing_key_id",        // 017_add_signature
		"build_attestation",     // 020_add_build_attestation
		"preview_id",            // 021_add_preview_columns (issue #308)
		"preview_pr_number",     // 021_add_preview_columns (issue #308)
		"preview_expires_at",    // 021_add_preview_columns (issue #308)
	},
	"active_deployments": {
		"tenant_id",
		"app_name",
		"deployment_id",
		"last_good_deployment_id",       // 005_add_last_good
		"auto_rollback_enabled",         // 009_add_auto_rollback
		"stable_since",                  // 009_add_auto_rollback
		"regions_published",             // 010_active_deployments_regions
		"regions_failed",                // 010_active_deployments_regions
		"regions_cached",                // 017_active_deployments_regions_cached
		"regions_cache_failed",          // 018_active_deployments_regions_cache_failed
		"region_cache_retry_count",      // 028_active_deployments_region_cache_retry_count (issue #501 retry cap)
		"last_publish_at",               // 010_active_deployments_regions
		"last_publish_attempt_id",       // 010_active_deployments_regions
		"activation_attempt_started_at", // 026_active_deployments_activation_attempt_started_at
	},
	"app_env": {
		"tenant_id",
		"app_name",
		"env_key",
		"env_value",
	},
	"workers": {
		"id",
		"region",
		"ip",
		"memory_mb",
		"last_seen",
		"created_at",
		"tenant_id", // 003_workers_tenant_id
	},
	"worker_status": {
		"worker_id",
		"apps",
		"last_report",
	},
	"apps": {
		"id",
		"tenant_id",
		"name",
		"description",
		"created_at",
	},
	"logs": {
		"id",
		"tenant_id",
		"deployment_id",
		"app_name",
		"worker_id",
		"region",
		"level",
		"message",
		"labels",
		"ts",
	},
	"app_traffic_splits": {
		"tenant_id",
		"app_name",
		"deployment_id",
		"weight",
		"created_at",
	},
	"domains": {
		"id",
		"tenant_id",
		"app_name",
		"fqdn",
		"status",
		"last_error",
		"created_at",
		"verified_at",
	},
	"autoscale_events": {
		"id",
		"created_at",
		"region",
		"action",
		"from_count",
		"to_count",
		"reason",
		"provider_kind",
		"succeeded",
		"error_message",
	},
	"audit_logs": {
		"id",
		"tenant_id",
		"api_key_id",
		"role",
		"action",
		"resource",
		"resource_id",
		"details",
		"outcome",
		"error_msg",
		"request_ip",
		"created_at",
	},
	"webhooks": {
		"id",
		"tenant_id",
		"url",
		"secret",
		"events",
		"description",
		"enabled",
		"created_at",
	},
	"webhook_deliveries": {
		"id",
		"webhook_id",
		"event_type",
		"status",
		"status_code",
		"request_body",
		"response_body",
		"error_msg",
		"attempt",
		"max_attempts",
		"created_at",
		"completed_at",
	},
	"billing_subscriptions": { // 022 (issue #419)
		"tenant_id",
		"provider",
		"provider_customer_id",
		"provider_subscription_id",
		"plan",
		"status",
		"current_period_end",
		"cancel_at_period_end",
		"created_at",
		"updated_at",
	},
	"billing_events": { // 023 (issue #419)
		"event_id",
		"provider",
		"event_type",
		"tenant_id",
		"received_at",
		"processed_at",
		"payload_hash",
	},
	"billing_usage_events": { // 030 (issue #485)
		"id",
		"tenant_id",
		"kind",
		"quantity",
		"idempotency_key",
		"recorded_at",
		"processed_at",
		"provider",
	},
	"outbox": { // 025 (issue #42)
		"id",
		"tenant_id",
		"app_name",
		"kind",
		"payload",
		"regions",
		"attempt_count",
		"next_attempt_at",
		"status",
		"last_error",
		"dedupe_key",
		"created_at",
		"published_at",
		"claimed_until",
	},
}

// IndexExpectation describes one CREATE INDEX statement that must
// exist in the public schema after the up migrations apply. The
// helper queries pg_indexes by index name (unique within a schema),
// then verifies it lives on the expected Table. Column ordering and
// included columns are NOT asserted — out of scope for this layer.
type IndexExpectation struct {
	Table string
	Name  string
}

// wantTypes enumerates the PostgreSQL type of each public-schema
// column. Values are the underlying type names as returned by
// information_schema.columns.udt_name:
//
//	text        → TEXT
//	int4        → INT / INTEGER
//	int8        → BIGINT (and BIGSERIAL — the SERIAL-ness lives in
//	             column_default, not the type itself)
//	bool        → BOOLEAN
//	timestamptz → TIMESTAMPTZ
//	jsonb       → JSONB
//	_text       → TEXT[]   (PG array convention: leading underscore)
//	varchar     → VARCHAR(N) — the size is dropped by udt_name
//	uuid        → UUID
//
// One row per (table, column). Inline comments reference the
// migration number where the column was added so reviewers can trace
// the contract back to the schema change.
var wantTypes = map[string]map[string]string{
	"tenants": {
		"id":                       "text",
		"name":                     "text",
		"plan":                     "text",
		"allowlisted_destinations": "_text", // 001 — TEXT[]
		"created_at":               "timestamptz",
		"overage_allowed_until":    "timestamptz", // 025_quotas_grace_columns (issue #420, nullable)
	},
	"quotas": {
		"tenant_id":                      "text",
		"max_deployments":                "int4",        // 001
		"max_apps":                       "int4",        // 001
		"max_workers":                    "int4",        // 001
		"max_memory_mb":                  "int4",        // 001
		"max_outbound_mb":                "int4",        // 001
		"used_outbound_bytes":            "int8",        // 009_quotas_used_outbound
		"quota_period_start":             "timestamptz", // 009_quotas_used_outbound
		"max_requests_per_month":         "int4",        // 013
		"used_request_count":             "int8",        // 013
		"quota_lock_grace_until":         "timestamptz", // 025_quotas_grace_columns (issue #420, nullable)
		"used_memory_mb":                 "int8",        // 027_used_memory_mb (issue #44 part 2)
		"max_resident_seconds_per_month": "int4",        // 029_quotas_resident_seconds (issue #485)
		"used_resident_seconds":          "int8",        // 029_quotas_resident_seconds (issue #485)
		"max_compute_ms_per_month":       "int4",        // 031_quotas_compute_ms (issue #555)
		"used_compute_ms":                "int8",        // 031_quotas_compute_ms (issue #555)
	},
	"api_keys": {
		"id":             "text",
		"tenant_id":      "text",
		"name":           "text",
		"key_hash":       "text",
		"role":           "text",
		"created_at":     "timestamptz",
		"last_used":      "timestamptz",
		"expires_at":     "timestamptz",
		"hash_algorithm": "text", // 005_api_key_hash_algorithm
		"lookup_hash":    "text", // 006_api_key_lookup_hash
	},
	"deployments": {
		"id":                    "text",
		"tenant_id":             "text",
		"app_name":              "text",
		"status":                "text",
		"hash":                  "text",
		"created_at":            "timestamptz",
		"regions":               "_text", // 008_deployments_regions — TEXT[]
		"auto_rollback_enabled": "bool",  // 009_add_auto_rollback
		"signature":             "text",  // 017_add_signature (nullable)
		"signing_key_id":        "text",  // 017_add_signature (nullable)
		"build_attestation":     "jsonb", // 020_add_build_attestation (nullable)
	},
	"active_deployments": {
		"tenant_id":                     "text",
		"app_name":                      "text",
		"deployment_id":                 "text",
		"last_good_deployment_id":       "text",        // 005_add_last_good
		"auto_rollback_enabled":         "bool",        // 009_add_auto_rollback
		"stable_since":                  "timestamptz", // 009_add_auto_rollback (nullable)
		"regions_published":             "_text",       // 010_active_deployments_regions
		"regions_failed":                "_text",       // 010_active_deployments_regions
		"regions_cached":                "_text",       // 017_active_deployments_regions_cached
		"regions_cache_failed":          "_text",       // 018_active_deployments_regions_cache_failed
		"region_cache_retry_count":      "jsonb",       // 028_active_deployments_region_cache_retry_count (issue #501 retry cap)
		"last_publish_at":               "timestamptz", // 010_active_deployments_regions (nullable)
		"last_publish_attempt_id":       "uuid",        // 010_active_deployments_regions (nullable)
		"activation_attempt_started_at": "timestamptz", // 026_active_deployments_activation_attempt_started_at (nullable)
	},
	"app_env": {
		"tenant_id": "text",
		"app_name":  "text",
		"env_key":   "text",
		"env_value": "text",
	},
	"workers": {
		"id":         "text",
		"region":     "text",
		"ip":         "text", // nullable
		"memory_mb":  "int4",
		"last_seen":  "timestamptz",
		"created_at": "timestamptz",
		"tenant_id":  "text", // 003_workers_tenant_id
	},
	"worker_status": {
		"worker_id":   "text",
		"apps":        "jsonb",
		"last_report": "timestamptz",
	},
	"apps": {
		"id":          "text",
		"tenant_id":   "text",
		"name":        "text",
		"description": "text", // nullable
		"created_at":  "timestamptz",
	},
	"logs": {
		"id":            "int8", // BIGSERIAL — serial-ness in default, type is int8
		"tenant_id":     "varchar",
		"deployment_id": "varchar",
		"app_name":      "varchar",
		"worker_id":     "varchar",
		"region":        "varchar",
		"level":         "varchar",
		"message":       "text",
		"labels":        "jsonb",
		"ts":            "timestamptz",
	},
	"app_traffic_splits": {
		"tenant_id":     "text",
		"app_name":      "text",
		"deployment_id": "text",
		"weight":        "int4", // INTEGER
		"created_at":    "timestamptz",
	},
	"domains": {
		"id":          "text",
		"tenant_id":   "text",
		"app_name":    "text",
		"fqdn":        "text",
		"status":      "text",
		"last_error":  "text", // nullable
		"created_at":  "timestamptz",
		"verified_at": "timestamptz", // nullable
	},
	"autoscale_events": {
		"id":            "int8", // BIGSERIAL
		"created_at":    "timestamptz",
		"region":        "text",
		"action":        "text",
		"from_count":    "int4", // INTEGER
		"to_count":      "int4", // INTEGER
		"reason":        "text",
		"provider_kind": "text",
		"succeeded":     "bool",
		"error_message": "text", // nullable
	},
	"audit_logs": {
		"id":          "int8", // BIGSERIAL
		"tenant_id":   "varchar",
		"api_key_id":  "varchar",
		"role":        "varchar",
		"action":      "varchar",
		"resource":    "varchar",
		"resource_id": "text",
		"details":     "text",
		"outcome":     "varchar",
		"error_msg":   "text",
		"request_ip":  "varchar", // VARCHAR(45)
		"created_at":  "timestamptz",
	},
	"webhooks": {
		"id":          "varchar", // VARCHAR(64)
		"tenant_id":   "varchar", // VARCHAR(64)
		"url":         "text",
		"secret":      "text",
		"events":      "_text", // 015 — TEXT[]
		"description": "text",
		"enabled":     "bool",
		"created_at":  "timestamptz",
	},
	"webhook_deliveries": {
		"id":            "int8",    // BIGSERIAL
		"webhook_id":    "varchar", // VARCHAR(64)
		"event_type":    "varchar", // VARCHAR(32)
		"status":        "varchar", // VARCHAR(16)
		"status_code":   "int4",    // nullable
		"request_body":  "text",
		"response_body": "text",
		"error_msg":     "text",
		"attempt":       "int4",
		"max_attempts":  "int4",
		"created_at":    "timestamptz",
		"completed_at":  "timestamptz", // nullable
	},
	"billing_subscriptions": { // 022 (issue #419)
		"tenant_id":                "varchar",     // VARCHAR(64)
		"provider":                 "varchar",     // VARCHAR(32)
		"provider_customer_id":     "varchar",     // VARCHAR(128)
		"provider_subscription_id": "varchar",     // VARCHAR(128), nullable
		"plan":                     "varchar",     // VARCHAR(32)
		"status":                   "varchar",     // VARCHAR(32)
		"current_period_end":       "timestamptz", // nullable
		"cancel_at_period_end":     "bool",
		"created_at":               "timestamptz",
		"updated_at":               "timestamptz",
	},
	"billing_events": { // 023 (issue #419)
		"event_id":     "varchar", // VARCHAR(128)
		"provider":     "varchar", // VARCHAR(32)
		"event_type":   "varchar", // VARCHAR(64)
		"tenant_id":    "varchar", // VARCHAR(64), nullable
		"received_at":  "timestamptz",
		"processed_at": "timestamptz", // nullable
		"payload_hash": "varchar",     // VARCHAR(128)
	},
	"billing_usage_events": { // 030 (issue #485)
		"id":              "int8",    // BIGSERIAL
		"tenant_id":       "varchar", // VARCHAR(64)
		"kind":            "varchar", // VARCHAR(16)
		"quantity":        "int8",    // BIGINT
		"idempotency_key": "varchar", // VARCHAR(128)
		"recorded_at":     "timestamptz",
		"processed_at":    "timestamptz", // nullable
		"provider":        "varchar",     // VARCHAR(32)
	},
	"outbox": { // 025 (issue #42)
		"id":              "int8",
		"tenant_id":       "text",
		"app_name":        "text",
		"kind":            "text",
		"payload":         "jsonb",
		"regions":         "_text",
		"attempt_count":   "int4",
		"next_attempt_at": "timestamptz",
		"status":          "text",
		"dedupe_key":      "text",
		"created_at":      "timestamptz",
		// last_error, published_at, claimed_until are nullable (see wantNotNull).
	},
	"idempotency_keys": { // 026 (issue #52)
		"tenant_id":      "text",        // TEXT (PK)
		"key":            "text",        // TEXT (PK)
		"deployment_id":  "text",        // TEXT (FK target)
		"request_sha256": "bytea",       // BYTEA — 32-byte SHA-256
		"created_at":     "timestamptz", // TIMESTAMPTZ, default NOW()
	},
}

// wantNotNull enumerates the columns that must have is_nullable='NO'.
// Stored as map[table][]string — any column in wantColumns but NOT in
// this map is implicitly asserted to be nullable. PK columns are
// always NOT NULL by Postgres contract, so they're listed here to make
// the contract explicit (and to produce a clear failure if a PK is
// silently dropped via a typo like removing PRIMARY KEY).
//
// Update when a migration flips a column from NULL → NOT NULL
// (add to this map) or NOT NULL → NULL (remove from this map and
// reference the migration number in the comment).
var wantNotNull = map[string][]string{
	"tenants": {
		"id",
		"name",
		"plan",
		"created_at",
		// allowlisted_destinations is nullable (DEFAULT '{}' but no NOT NULL).
	},
	"quotas": {
		"tenant_id",
		"max_deployments",                // 001
		"max_apps",                       // 001
		"max_workers",                    // 001
		"max_memory_mb",                  // 001
		"max_outbound_mb",                // 001
		"used_outbound_bytes",            // 009_quotas_used_outbound
		"quota_period_start",             // 009_quotas_used_outbound
		"max_requests_per_month",         // 013
		"used_request_count",             // 013
		"used_memory_mb",                 // 027_used_memory_mb (issue #44 part 2)
		"max_resident_seconds_per_month", // 029_quotas_resident_seconds (issue #485)
		"used_resident_seconds",          // 029_quotas_resident_seconds (issue #485)
		"max_compute_ms_per_month",       // 031_quotas_compute_ms (issue #555)
		"used_compute_ms",                // 031_quotas_compute_ms (issue #555)
	},
	"api_keys": {
		"id",
		"tenant_id",
		"name",
		"key_hash",
		"role",
		"created_at",
		"hash_algorithm", // 005 — SET NOT NULL after backfill precondition
		"lookup_hash",    // 007 — SET NOT NULL after null-row precondition
		// last_used, expires_at are nullable.
	},
	"deployments": {
		"id",
		"tenant_id",
		"app_name",
		"status",
		"hash",
		"created_at",
		"regions",               // 008
		"auto_rollback_enabled", // 009
	},
	"active_deployments": {
		"tenant_id",
		"app_name",
		"deployment_id",
		"auto_rollback_enabled", // 009
		"regions_published",     // 010
		"regions_failed",        // 010
		// last_good_deployment_id is nullable (per 005 — pre-existing rows have NULL).
		// stable_since is nullable (declared NULL).
		// last_publish_at is nullable.
		// last_publish_attempt_id is nullable.
	},
	"app_env": {
		"tenant_id",
		"app_name",
		"env_key",
		"env_value",
	},
	"workers": {
		"id",
		"region",
		"memory_mb",
		"last_seen",
		"created_at",
		"tenant_id", // 003
		// ip is nullable.
	},
	"worker_status": {
		"worker_id",
		"apps",
		"last_report",
	},
	"apps": {
		"id",
		"tenant_id",
		"name",
		"created_at",
		// description is nullable.
	},
	"logs": {
		"id",
		"tenant_id",
		"deployment_id",
		"app_name",
		"worker_id",
		"region",
		"level",
		"message",
		"labels",
		"ts",
	},
	"app_traffic_splits": {
		"tenant_id",
		"app_name",
		"deployment_id",
		"weight",
		"created_at",
	},
	"domains": {
		"id",
		"tenant_id",
		"app_name",
		"fqdn",
		"status",
		"created_at",
		// last_error, verified_at are nullable.
	},
	"autoscale_events": {
		"id",
		"created_at",
		"region",
		"action",
		"from_count",
		"to_count",
		"reason",
		"provider_kind",
		"succeeded",
		// error_message is nullable (set when succeeded=false).
	},
	"audit_logs": {
		"id",
		"tenant_id",
		"api_key_id",
		"role",
		"action",
		"resource",
		"resource_id",
		"details",
		"outcome",
		"error_msg",
		"request_ip",
		"created_at",
	},
	"webhooks": {
		"id",
		"tenant_id",
		"url",
		"secret",
		"events",
		"description",
		"enabled",
		"created_at",
	},
	"webhook_deliveries": {
		"id",
		"webhook_id",
		"event_type",
		"status",
		"request_body",
		"response_body",
		"error_msg",
		"attempt",
		"max_attempts",
		"created_at",
		// status_code, completed_at are nullable.
	},
	"billing_subscriptions": { // 022 (issue #419); 024 (issue #419 review) relaxed provider_customer_id
		"tenant_id",
		"provider",
		"plan",
		"status",
		"cancel_at_period_end",
		"created_at",
		"updated_at",
		// provider_customer_id, provider_subscription_id, current_period_end are nullable.
	},
	"billing_events": { // 023 (issue #419)
		"event_id",
		"provider",
		"event_type",
		"received_at",
		"payload_hash",
		// tenant_id, processed_at are nullable.
	},
	"billing_usage_events": { // 030 (issue #485)
		"id",
		"tenant_id",
		"kind",
		"quantity",
		"idempotency_key",
		"recorded_at",
		"provider",
		// processed_at is nullable.
	},
	"outbox": { // 025 (issue #42)
		"id",
		"tenant_id",
		"app_name",
		"kind",
		"payload",
		"regions",
		"attempt_count",
		"next_attempt_at",
		"status",
		"dedupe_key",
		"created_at",
		// last_error, published_at, claimed_until are nullable.
	},
	"idempotency_keys": { // 026 (issue #52)
		"tenant_id",
		"key",
		"deployment_id",
		"request_sha256",
		// created_at has a non-NULL default — see wantDefaults.
	},
}

// wantIndexes enumerates every CREATE INDEX in the migrations. The
// helper queries pg_indexes by name (unique within the public schema)
// and asserts each lives on the expected table. Column ordering and
// included columns are NOT asserted — out of scope for this layer.
//
// Update when a migration creates or renames an index. Inline comments
// reference the migration number where the index was created.
var wantIndexes = []IndexExpectation{
	{Table: "deployments", Name: "idx_deployments_tenant_app"},                                  // 002_add_indexes
	{Table: "deployments", Name: "idx_deployments_tenant"},                                      // 002_add_indexes
	{Table: "workers", Name: "idx_workers_region"},                                              // 002_add_indexes
	{Table: "api_keys", Name: "idx_api_keys_tenant"},                                            // 002_add_indexes
	{Table: "active_deployments", Name: "idx_active_deployments_tenant"},                        // 002_add_indexes
	{Table: "app_env", Name: "idx_app_env_tenant_app"},                                          // 002_add_indexes
	{Table: "workers", Name: "idx_workers_tenant_id"},                                           // 003_workers_tenant_id
	{Table: "apps", Name: "idx_apps_tenant_id"},                                                 // 004_apps
	{Table: "logs", Name: "idx_logs_tenant_app_ts"},                                             // 005_logs
	{Table: "logs", Name: "idx_logs_ts"},                                                        // 005_logs
	{Table: "api_keys", Name: "idx_api_keys_lookup_hash"},                                       // 006_api_key_lookup_hash
	{Table: "app_traffic_splits", Name: "idx_ats_tenant_app"},                                   // 009_traffic_splits
	{Table: "domains", Name: "idx_domains_tenant_app"},                                          // 010_domains
	{Table: "domains", Name: "idx_domains_fqdn"},                                                // 010_domains
	{Table: "autoscale_events", Name: "idx_autoscale_events_region_time"},                       // 012_autoscale_events
	{Table: "audit_logs", Name: "idx_audit_logs_tenant_created"},                                // 014_audit_logs
	{Table: "audit_logs", Name: "idx_audit_logs_resource"},                                      // 014_audit_logs
	{Table: "webhooks", Name: "idx_webhooks_tenant"},                                            // 015_webhooks
	{Table: "webhook_deliveries", Name: "idx_webhook_deliveries_webhook"},                       // 015_webhooks
	{Table: "deployments", Name: "idx_deployments_preview_expires_at"},                          // 021_add_preview_columns (issue #308)
	{Table: "billing_subscriptions", Name: "idx_billing_subscriptions_provider_customer"},       // 022_billing_subscriptions (issue #419)
	{Table: "billing_events", Name: "idx_billing_events_tenant_received"},                       // 023_billing_events (issue #419)
	{Table: "outbox", Name: "outbox_due_idx"},                                                   // 025_outbox (issue #42)
	{Table: "outbox", Name: "outbox_tenant_app_idx"},                                            // 025_outbox (issue #42)
	{Table: "outbox", Name: "outbox_failed_idx"},                                                // 025_outbox (issue #42)
	{Table: "idempotency_keys", Name: "idx_idempotency_keys_deployment_id"},                     // 026_idempotency_keys (issue #52)
	{Table: "active_deployments", Name: "idx_active_deployments_regions_cache_failed_nonempty"}, // 027_active_deployments_regions_cache_failed_index (issue #501)
	{Table: "tenants", Name: "idx_tenants_overage_allowed_until"},                               // 025_quotas_grace_columns (issue #420, partial)
	{Table: "quotas", Name: "idx_quotas_grace_until"},                                           // 025_quotas_grace_columns (issue #420, partial)
	{Table: "billing_usage_events", Name: "idx_billing_usage_events_unprocessed"},               // 030_billing_usage_events (issue #485, partial)
	{Table: "audit_logs", Name: "idx_audit_logs_created_at"},                                    // 031_gc_retention_indexes (issue #574)
	{Table: "webhook_deliveries", Name: "idx_webhook_deliveries_created_at"},                    // 031_gc_retention_indexes (issue #574)
	{Table: "autoscale_events", Name: "idx_autoscale_events_created_at"},                        // 031_gc_retention_indexes (issue #574)
}

// ForeignKeyExpectation describes one FOREIGN KEY constraint that
// must exist on a given table after the up migrations apply. Matches
// the text rendered by pg_get_constraintdef() against pg_constraint
// (the canonical source for constraint metadata — more reliable than
// information_schema which renders inconsistently across PG versions).
type ForeignKeyExpectation struct {
	Constraint string // constraint name as stored in pg_constraint.conname
	Definition string // expected pg_get_constraintdef() output, e.g.
	//                  "FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE"
}

// wantForeignKeys enumerates every FOREIGN KEY the migrations create.
// Single source of truth for the schema-vs-migration FK contract:
// dropping or renaming a column referenced by an FK shows up here as
// a test failure rather than as a silent runtime cascade miss.
//
// Update when:
//   - A migration adds an FK → add an entry with the migration number
//     in the comment so reviewers can trace the contract.
//   - A migration drops an FK → remove the entry.
//   - A migration changes an FK's ON DELETE action → update the
//     Definition string.
//
// Definition values match pg_get_constraintdef() output verbatim —
// copy them verbatim from a live postgres (see discover_test.go for
// the one-shot query).
var wantForeignKeys = map[string][]ForeignKeyExpectation{
	"active_deployments": {
		{"active_deployments_deployment_id_fkey", "FOREIGN KEY (deployment_id) REFERENCES deployments(id)"},
		{"active_deployments_last_good_deployment_id_fkey", "FOREIGN KEY (last_good_deployment_id) REFERENCES deployments(id) ON DELETE SET NULL"},
	},
	"api_keys": {
		{"api_keys_tenant_id_fkey", "FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE"},
	},
	"billing_usage_events": { // 030 (issue #485)
		{"billing_usage_events_tenant_id_fkey", "FOREIGN KEY (tenant_id) REFERENCES tenants(id)"},
	},
	"app_traffic_splits": {
		{"app_traffic_splits_deployment_id_fkey", "FOREIGN KEY (deployment_id) REFERENCES deployments(id)"},
	},
	"apps": {
		{"apps_tenant_id_fkey", "FOREIGN KEY (tenant_id) REFERENCES tenants(id)"},
	},
	"billing_subscriptions": { // 022 (issue #419)
		{"billing_subscriptions_tenant_id_fkey", "FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE"},
	},
	"deployments": {
		{"deployments_tenant_id_fkey", "FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE"},
	},
	"domains": {
		{"fk_domains_app", "FOREIGN KEY (tenant_id, app_name) REFERENCES apps(tenant_id, name) ON DELETE CASCADE"},
	},
	"idempotency_keys": { // 026 (issue #52)
		{"idempotency_keys_deployment_id_fkey", "FOREIGN KEY (deployment_id) REFERENCES deployments(id) ON DELETE CASCADE"},
	},
	"quotas": {
		{"quotas_tenant_id_fkey", "FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE"},
	},
	"webhook_deliveries": {
		{"webhook_deliveries_webhook_id_fkey", "FOREIGN KEY (webhook_id) REFERENCES webhooks(id) ON DELETE CASCADE"},
	},
	"worker_status": {
		{"worker_status_worker_id_fkey", "FOREIGN KEY (worker_id) REFERENCES workers(id) ON DELETE CASCADE"},
	},
	"workers": {
		{"workers_tenant_id_fkey", "FOREIGN KEY (tenant_id) REFERENCES tenants(id)"},
	},
}

// wantChecks enumerates every CHECK constraint the migrations create.
// Each value is the pg_get_constraintdef() output verbatim. Adding a
// new CHECK constraint to a migration without updating this map will
// fail the test (silent schema drift caught at PR time).
//
// Update when:
//   - A migration adds a CHECK → add the entry with the migration
//     number in the comment.
//   - A migration relaxes or tightens a CHECK → update the Definition
//     string to match the new pg_get_constraintdef() output.
//
// Note: pg 16 renders CHECK clauses with implicit casts (e.g.,
// 'sha256'::text) and full parenthesization. The Definition strings
// here are pinned to PG 16; if the team upgrades to PG 17/18 and the
// rendering changes, the test will need updates.
var wantChecks = map[string]string{
	"api_keys.api_keys_hash_algorithm_check":                   "CHECK ((hash_algorithm = ANY (ARRAY['sha256'::text, 'argon2id'::text])))",                                                  // 005_api_key_hash_algorithm
	"app_traffic_splits.app_traffic_splits_weight_check":       "CHECK (((weight >= 0) AND (weight <= 100)))",                                                                               // 009_traffic_splits
	"autoscale_events.autoscale_events_action_check":           "CHECK ((action = ANY (ARRAY['scale_up'::text, 'scale_down'::text, 'noop'::text])))",                                        // 012_autoscale_events
	"quotas.quotas_used_memory_mb_nonneg":                      "CHECK ((used_memory_mb >= 0))",                                                                                             // 027_used_memory_mb (issue #44 part 2)
	"billing_usage_events.billing_usage_events_kind_check":     "CHECK ((kind = ANY (ARRAY['resident_seconds'::text, 'request_count'::text, 'outbound_bytes'::text, 'compute_ms'::text])))", // 030_billing_usage_events (issue #485, extended for compute_ms in #555)
	"billing_usage_events.billing_usage_events_quantity_check": "CHECK ((quantity >= 0))",                                                                                                   // 030_billing_usage_events (issue #485)
	"workers.workers_public_key_length_check":                  "CHECK (((public_key IS NULL) OR (length(public_key) <= 256)))",                                                             // 033_workers_public_key_length (issue #430 review follow-up)
}

// wantDefaults enumerates every public-schema column that has a
// non-NULL column_default. Each value is the rendered text from
// information_schema.columns.column_default verbatim. Catches
// accidental DEFAULT changes that would otherwise silently change
// application behavior (e.g., changing `DEFAULT 'free'` to
// `DEFAULT 'trial'` on tenants.plan would change every new tenant's
// plan tier without a code change).
//
// Columns with column_default IS NULL are simply not in this map —
// they're implicitly asserted to have no default. SERIAL defaults
// (nextval(...)) and NOW() defaults are stable across migrations so
// they're included for completeness; if a future change removes a
// SERIAL the test will catch it.
//
// Update when a migration adds/changes/removes a DEFAULT expression
// on any tracked column. Reference the migration number in the comment.
var wantDefaults = map[string]map[string]string{
	"active_deployments": {
		"auto_rollback_enabled": "false",        // 009
		"regions_failed":        "'{}'::text[]", // 010
		"regions_published":     "'{}'::text[]", // 010
	},
	"api_keys": {
		"role": "'developer'::text", // 001
	},
	"audit_logs": {
		"api_key_id":  "''::character varying", // 014
		"details":     "''::text",              // 014
		"error_msg":   "''::text",              // 014
		"request_ip":  "''::character varying", // 014
		"resource_id": "''::text",              // 014
		"role":        "''::character varying", // 014
		"tenant_id":   "''::character varying", // 014
	},
	"billing_subscriptions": { // 022 (issue #419)
		"cancel_at_period_end": "false", // 022
		"created_at":           "now()", // 022
		"updated_at":           "now()", // 022
	},
	"billing_events": { // 023 (issue #419)
		"received_at": "now()", // 023
	},
	"billing_usage_events": { // 030 (issue #485)
		"recorded_at": "now()", // 030
	},
	"deployments": {
		"auto_rollback_enabled": "false",            // 009
		"regions":               "'{}'::text[]",     // 008
		"status":                "'deployed'::text", // 001
	},
	"domains": {
		"status": "'pending'::text", // 010
	},
	"idempotency_keys": { // 026 (issue #52)
		"created_at": "now()", // 026
	},
	"logs": {
		"labels": "'{}'::jsonb", // 005
	},
	"quotas": {
		"max_apps":                       "5",                                                           // 001
		"max_deployments":                "10",                                                          // 001
		"max_memory_mb":                  "256",                                                         // 001
		"max_outbound_mb":                "1000",                                                        // 001
		"max_requests_per_month":         "100000",                                                      // 013
		"max_resident_seconds_per_month": "0",                                                           // 029_quotas_resident_seconds (issue #485); backfilled per plan
		"max_compute_ms_per_month":       "0",                                                           // 031_quotas_compute_ms (issue #555); backfilled per plan
		"max_workers":                    "3",                                                           // 001
		"quota_period_start":             "date_trunc('month'::text, (now() AT TIME ZONE 'UTC'::text))", // 009
		"used_outbound_bytes":            "0",                                                           // 009
		"used_request_count":             "0",                                                           // 013
		"used_memory_mb":                 "0",                                                           // 027_used_memory_mb (issue #44 part 2)
		"used_resident_seconds":          "0",                                                           // 029_quotas_resident_seconds (issue #485)
	},
	"tenants": {
		"allowlisted_destinations": "'{}'::text[]", // 001
		"plan":                     "'free'::text", // 001
	},
	"webhook_deliveries": {
		"attempt":       "1",        // 015
		"error_msg":     "''::text", // 015
		"max_attempts":  "1",        // 015
		"request_body":  "''::text", // 015
		"response_body": "''::text", // 015
	},
	"webhooks": {
		"description": "''::text",     // 015
		"enabled":     "true",         // 015
		"events":      "'{}'::text[]", // 015
	},
	"worker_status": {
		"apps": "'{}'::jsonb", // 001
	},
	"workers": {
		"memory_mb": "4096", // 001
	},
}

// TestRoundtrip is the headline acceptance test for the migration
// directory. Subtests share a single Postgres container + *sqlx.DB so
// the rollback and reapply steps build on the up pass. Failure in any
// subtest aborts siblings (default t.Run behaviour).
func TestRoundtrip(t *testing.T) {
	if reason, ok := testutil.ShouldSkipIntegration("SKIP_INTEGRATION_TESTS"); ok {
		t.Skip(reason)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pgC := newTestPostgres(t, ctx)
	t.Cleanup(func() {
		cctx, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = pgC.Terminate(cctx)
	})

	db := newDBFromContainer(t, ctx, pgC)
	t.Cleanup(func() { _ = db.Close() })

	src := migrationsDir(t)

	t.Run("UpAppliesAllAndCreatesTables", func(t *testing.T) {
		n, err := migrate.Exec(db.DB, "postgres", &migrate.FileMigrationSource{Dir: src}, migrate.Up)
		require.NoError(t, err)
		require.Equal(t, splitFileCount, n)

		// rubenv tracks applied migrations in gorp_migrations (default
		// TableName; verified at migrate.go:50-55). Cross-check via
		// the tracking table instead of trusting the return value alone
		// — protects against future library changes to count semantics.
		var tracked int
		require.NoError(t, db.Get(&tracked, "SELECT COUNT(*) FROM gorp_migrations"))
		require.Equal(t, splitFileCount, tracked)

		for _, want := range wantTables {
			assertTableExists(t, db, want)
		}

		// Column-level contract: every expected column on every tracked
		// table must exist. Catches accidental renames/drops in a
		// migration that would otherwise leave the schema silently
		// drifting from what the Go repositories expect.
		for table, cols := range wantColumns {
			assertColumnsExist(t, db, table, cols)
		}

		// Type contract: every column must have the expected PostgreSQL
		// type. Catches accidental type drift (TEXT → BIGINT, etc.)
		// that would only fail at runtime when a repository scans into
		// a Go type that doesn't match.
		assertColumnTypes(t, db, wantTypes)

		// Nullability contract: every listed column must be NOT NULL.
		// Columns in wantColumns but not in wantNotNull are implicitly
		// asserted to be nullable. Catches accidental NOT NULL drops
		// that would let the application insert NULLs into a column
		// the business logic treats as required.
		assertNotNull(t, db, wantNotNull)

		// Index contract: every expected index must exist in the public
		// schema on the expected table. Catches DROP INDEX typos and
		// migrations that rename an index without updating dependent
		// code paths. Column ordering inside each index is NOT checked.
		assertIndexesExist(t, db, wantIndexes)

		// FK contract: every expected foreign key must exist with its
		// expected definition. Catches accidental FK drops, column
		// renames that break the referenced side, and ON DELETE action
		// changes that would silently disable cascading deletes.
		assertForeignKeys(t, db, wantForeignKeys)

		// CHECK contract: every expected check constraint must exist
		// with its expected clause. Catches loosened or tightened CHECK
		// expressions that would silently allow (or reject) values.
		assertCheckConstraints(t, db, wantChecks)

		// Default contract: every column with a non-NULL default must
		// have the expected column_default. Catches DEFAULT changes
		// that would silently alter application behavior (e.g.,
		// changing a quota cap default).
		assertDefaults(t, db, wantDefaults)
	})

	t.Run("DownReversesAllToVersionZero", func(t *testing.T) {
		// migrate.Exec(Down) walks every applied migration in reverse
		// and applies each Down section. ExecVersion(0, Down) would
		// fail because rubenv's planner looks up the target version
		// via VersionInt() (migrate.go:686) and no migration has
		// version-int 0 — the prefix regex starts at 1.
		n, err := migrate.Exec(db.DB, "postgres", &migrate.FileMigrationSource{Dir: src}, migrate.Down)
		require.NoError(t, err)
		require.Equal(t, splitFileCount, n)

		var tracked int
		require.NoError(t, db.Get(&tracked, "SELECT COUNT(*) FROM gorp_migrations"))
		require.Equal(t, 0, tracked)

		// Every public-schema table we created in the up pass should
		// now be gone. Using the same wantTables set catches migrations
		// whose Down section silently leaks a table.
		for _, want := range wantTables {
			assertTableAbsent(t, db, want)
		}
	})

	t.Run("UpReappliesCleanlyFromEmpty", func(t *testing.T) {
		n, err := migrate.Exec(db.DB, "postgres", &migrate.FileMigrationSource{Dir: src}, migrate.Up)
		require.NoError(t, err)
		require.Equal(t, splitFileCount, n)
	})

	t.Run("MigrationsAreLexicographicallyOrdered", func(t *testing.T) {
		// rubenv applies migrations in byId order, which is a
		// lexicographic sort on the file name. The numeric prefix
		// (NNN_) must be zero-padded (or otherwise lexically sortable)
		// so e.g. 002_*.sql sorts before 010_*.sql. This catches a
		// common foot-gun where someone adds a migration named
		// `2_*.sql` instead of `002_*.sql` and the apply order
		// silently breaks.
		assertMigrationsLexicallyOrdered(t, src)
	})

	t.Run("MigrationsAreIdempotent", func(t *testing.T) {
		// Idempotency contract: every migration's Up section must be
		// re-runnable on an already-migrated DB. Wipe gorp_migrations
		// to force rubenv to re-evaluate every *.sql file as "pending",
		// then re-run Exec(Up). If any CREATE TABLE / CREATE INDEX /
		// ALTER TABLE ADD COLUMN lacks IF NOT EXISTS (or its equivalent
		// guard), Postgres errors with "relation already exists" /
		// "column already exists" and this test fails.
		//
		// Pre-condition for this test passing: every migration uses
		// IF NOT EXISTS on every CREATE / ADD COLUMN. The IF NOT EXISTS
		// clauses were added as part of this PR — see the migrations
		// diff. Removing one in a future PR will surface here as a
		// loud failure.
		_, err := db.DB.Exec("DELETE FROM gorp_migrations")
		require.NoError(t, err)

		n, err := migrate.Exec(db.DB, "postgres", &migrate.FileMigrationSource{Dir: src}, migrate.Up)
		require.NoError(t, err, "a migration's Up section is not idempotent — re-running after wiping gorp_migrations failed. Add IF NOT EXISTS / IF EXISTS to the offending statement.")
		require.Equal(t, splitFileCount, n, "every migration should be re-applied after wiping gorp_migrations")
	})

	t.Run("BackfillsApplyCorrectValuesPerPlan", func(t *testing.T) {
		// Data-dependent backfill contract: migration 013's CASE/WHEN
		// must produce the right per-plan caps. We seed tenants with
		// each plan tier, then apply migration 013 to verify the
		// resulting max_requests_per_month in quotas.
		//
		// Uses a fresh DB container (separate from the parent's) so
		// we can stop at a specific migration version and seed data
		// before the backfill runs.
		subPgC := newTestPostgres(t, ctx)
		subCtx, subCancel := context.WithTimeout(ctx, 2*time.Minute)
		t.Cleanup(func() {
			cctx, c := context.WithTimeout(context.Background(), 30*time.Second)
			defer c()
			_ = subPgC.Terminate(cctx)
			subCancel()
		})

		subDB := newDBFromContainer(t, subCtx, subPgC)
		t.Cleanup(func() { _ = subDB.Close() })

		// Apply all 40 migration file records (split .up.sql + .down.sql
		// means each logical migration produces two records). This
		// sets up the full schema. Then we'll wipe gorp_migrations and
		// selectively re-apply 013 in isolation after seeding.
		_, err := migrate.Exec(subDB.DB, "postgres", &migrate.FileMigrationSource{Dir: src}, migrate.Up)
		require.NoError(t, err)

		// Wipe gorp_migrations to force rubenv to re-evaluate every
		// *.sql file as pending on the next apply.
		_, err = subDB.DB.Exec("DELETE FROM gorp_migrations")
		require.NoError(t, err)

		// Reset the schema to pre-013 state by dropping the column
		// that 013 adds (and its quota rows). This lets us test the
		// backfill in isolation. CASCADE drops the dependent rows.
		_, err = subDB.DB.Exec("ALTER TABLE quotas DROP COLUMN IF EXISTS max_requests_per_month, DROP COLUMN IF EXISTS used_request_count")
		require.NoError(t, err)

		// Seed one tenant + matching quota row per plan tier. Use
		// prefixed IDs so the test's rows are easy to identify if
		// they leak. The UPDATE backfill in 013 joins quotas to tenants
		// and only touches existing quota rows, so each tenant needs a
		// pre-existing quota row for the backfill to produce a value
		// the test can read back.
		for _, plan := range []string{"free", "pro", "business", "enterprise", "unknown"} {
			_, err := subDB.DB.Exec(
				"INSERT INTO tenants (id, name, plan) VALUES ($1, $2, $3)",
				"t_test_"+plan, "Test "+plan, plan)
			require.NoErrorf(t, err, "seeding tenant for plan %q", plan)
			_, err = subDB.DB.Exec(
				"INSERT INTO quotas (tenant_id) VALUES ($1)",
				"t_test_"+plan)
			require.NoErrorf(t, err, "seeding quota row for plan %q", plan)
		}

		// Trigger the UPDATE backfill by running migration 013 only.
		// We do this by manually executing the migration body rather
		// than via ExecVersion, because the split-file format means
		// rubenv's version-int logic doesn't cleanly target a single
		// logical migration.
		_, err = subDB.DB.Exec(`
			ALTER TABLE quotas
				ADD COLUMN IF NOT EXISTS max_requests_per_month INT   NOT NULL DEFAULT 100000,
				ADD COLUMN IF NOT EXISTS used_request_count     BIGINT NOT NULL DEFAULT 0;
			UPDATE quotas q
			   SET max_requests_per_month = CASE t.plan
			       WHEN 'free'       THEN 100000
			       WHEN 'pro'        THEN 5000000
			       WHEN 'business'   THEN 50000000
			       WHEN 'enterprise' THEN -1
			       ELSE 100000
			   END
			  FROM tenants t
			 WHERE q.tenant_id = t.id;
		`)
		require.NoError(t, err)

		// Verify each plan got the right cap. Mirrors the CASE arms
		// in 013_quotas_used_requests.up.sql — if a future change
		// drops or swaps a WHEN, this test fails with a clear diff.
		type expectation struct {
			plan    string
			wantCap int
		}
		expected := []expectation{
			{"free", 100000},       // explicit
			{"pro", 5000000},       // explicit
			{"business", 50000000}, // explicit
			{"enterprise", -1},     // explicit (unlimited)
			{"unknown", 100000},    // ELSE falls back to free-tier
		}
		for _, e := range expected {
			var got int
			require.NoError(t, subDB.Get(&got,
				"SELECT max_requests_per_month FROM quotas WHERE tenant_id=$1",
				"t_test_"+e.plan))
			require.Equalf(t, e.wantCap, got,
				"plan %q: quotas.max_requests_per_month = %d, want %d (013 backfill drifted?)",
				e.plan, got, e.wantCap)
		}
	})

	t.Run("BackfillsResidentSecondsCapPerPlan_029", func(t *testing.T) {
		// Data-dependent backfill contract for issue #485 / migration
		// 029. Mirrors the 013 backfill subtest above: seed tenants
		// with each plan tier, run the 029 backfill in isolation, and
		// verify max_resident_seconds_per_month matches the per-plan
		// values declared in internal/domain/plans.go.
		//
		// Uses a fresh DB container so we can stop at a specific
		// migration version and seed data before the backfill runs.
		subPgC := newTestPostgres(t, ctx)
		subCtx, subCancel := context.WithTimeout(ctx, 2*time.Minute)
		t.Cleanup(func() {
			cctx, c := context.WithTimeout(context.Background(), 30*time.Second)
			defer c()
			_ = subPgC.Terminate(cctx)
			subCancel()
		})

		subDB := newDBFromContainer(t, subCtx, subPgC)
		t.Cleanup(func() { _ = subDB.Close() })

		// Apply all migrations to set up the full schema.
		_, err := migrate.Exec(subDB.DB, "postgres", &migrate.FileMigrationSource{Dir: src}, migrate.Up)
		require.NoError(t, err)

		// Reset to pre-029 state by dropping the columns 029 adds.
		// CASCADE drops dependent rows so the seeding step below
		// controls the row set.
		_, err = subDB.DB.Exec(`
			ALTER TABLE quotas
				DROP COLUMN IF EXISTS max_resident_seconds_per_month,
				DROP COLUMN IF EXISTS used_resident_seconds;
		`)
		require.NoError(t, err)

		// Seed one tenant + matching quota row per plan tier. Each
		// tenant needs a pre-existing quota row for the UPDATE backfill
		// (which joins quotas to tenants) to produce a value the test
		// can read back.
		for _, plan := range []string{"free", "pro", "business", "enterprise", "unknown"} {
			_, err := subDB.DB.Exec(
				"INSERT INTO tenants (id, name, plan) VALUES ($1, $2, $3)",
				"t_resid_"+plan, "Test "+plan, plan)
			require.NoErrorf(t, err, "seeding tenant for plan %q", plan)
			_, err = subDB.DB.Exec(
				"INSERT INTO quotas (tenant_id) VALUES ($1)",
				"t_resid_"+plan)
			require.NoErrorf(t, err, "seeding quota row for plan %q", plan)
		}

		// Run the 029 backfill in isolation — same shape as the
		// migration body, with the per-tier caps matching plans.go.
		_, err = subDB.DB.Exec(`
			ALTER TABLE quotas
				ADD COLUMN IF NOT EXISTS max_resident_seconds_per_month INT   NOT NULL DEFAULT 0,
				ADD COLUMN IF NOT EXISTS used_resident_seconds           BIGINT NOT NULL DEFAULT 0;
			UPDATE quotas q
			   SET max_resident_seconds_per_month = CASE t.plan
			       WHEN 'free'       THEN  2592000
			       WHEN 'pro'        THEN  7776000
			       WHEN 'business'   THEN 31104000
			       WHEN 'enterprise' THEN       -1
			       ELSE 2592000
			   END
			  FROM tenants t
			 WHERE q.tenant_id = t.id;
		`)
		require.NoError(t, err)

		// Verify each plan got the right cap. Values must match
		// internal/domain/plans.go:planTiers exactly — if a future
		// change drops or swaps a WHEN, this test fails with a clear diff.
		type expectation struct {
			plan    string
			wantCap int
		}
		expected := []expectation{
			{"free", 2592000},      // explicit
			{"pro", 7776000},       // explicit
			{"business", 31104000}, // explicit
			{"enterprise", -1},     // explicit (unlimited)
			{"unknown", 2592000},   // ELSE falls back to free-tier
		}
		for _, e := range expected {
			var got int
			require.NoError(t, subDB.Get(&got,
				"SELECT max_resident_seconds_per_month FROM quotas WHERE tenant_id=$1",
				"t_resid_"+e.plan))
			require.Equalf(t, e.wantCap, got,
				"plan %q: quotas.max_resident_seconds_per_month = %d, want %d (029 backfill drifted?)",
				e.plan, got, e.wantCap)
		}

		// And used_resident_seconds is 0 across the board (default).
		var usedCount int
		require.NoError(t, subDB.Get(&usedCount,
			"SELECT COUNT(*) FROM quotas WHERE used_resident_seconds != 0"))
		require.Equalf(t, 0, usedCount,
			"every quota row should have used_resident_seconds=0 after backfill")
	})

	t.Run("BackfillsComputeMsCapPerPlan_031", func(t *testing.T) {
		// Data-dependent backfill contract for issue #555 / migration
		// 031. Mirrors the 029 backfill subtest above: seed tenants
		// with each plan tier, run the 031 backfill in isolation, and
		// verify max_compute_ms_per_month matches the per-plan values
		// declared in internal/domain/plans.go. The fourth metered
		// dimension's cap is the resident-seconds cap scaled by
		// 1_000 (free=2_592_000_000, pro=7_776_000_000,
		// business=31_104_000_000, enterprise=-1).
		//
		// Uses a fresh DB container so we can stop at a specific
		// migration version and seed data before the backfill runs.
		subPgC := newTestPostgres(t, ctx)
		subCtx, subCancel := context.WithTimeout(ctx, 2*time.Minute)
		t.Cleanup(func() {
			cctx, c := context.WithTimeout(context.Background(), 30*time.Second)
			defer c()
			_ = subPgC.Terminate(cctx)
			subCancel()
		})

		subDB := newDBFromContainer(t, subCtx, subPgC)
		t.Cleanup(func() { _ = subDB.Close() })

		// Apply all migrations to set up the full schema.
		_, err := migrate.Exec(subDB.DB, "postgres", &migrate.FileMigrationSource{Dir: src}, migrate.Up)
		require.NoError(t, err)

		// Reset to pre-031 state by dropping the columns 031 adds.
		_, err = subDB.DB.Exec(`
			ALTER TABLE quotas
				DROP COLUMN IF EXISTS max_compute_ms_per_month,
				DROP COLUMN IF EXISTS used_compute_ms;
		`)
		require.NoError(t, err)

		// Seed one tenant + matching quota row per plan tier.
		for _, plan := range []string{"free", "pro", "business", "enterprise", "unknown"} {
			_, err := subDB.DB.Exec(
				"INSERT INTO tenants (id, name, plan) VALUES ($1, $2, $3)",
				"t_compms_"+plan, "Test "+plan, plan)
			require.NoErrorf(t, err, "seeding tenant for plan %q", plan)
			_, err = subDB.DB.Exec(
				"INSERT INTO quotas (tenant_id) VALUES ($1)",
				"t_compms_"+plan)
			require.NoErrorf(t, err, "seeding quota row for plan %q", plan)
		}

		// Run the 031 backfill in isolation — same shape as the
		// migration body, with the per-tier caps matching plans.go.
		_, err = subDB.DB.Exec(`
			ALTER TABLE quotas
				ADD COLUMN IF NOT EXISTS max_compute_ms_per_month INT   NOT NULL DEFAULT 0,
				ADD COLUMN IF NOT EXISTS used_compute_ms           BIGINT NOT NULL DEFAULT 0;
			UPDATE quotas q
			   SET max_compute_ms_per_month = CASE t.plan
			       WHEN 'free'       THEN  2592000000
			       WHEN 'pro'        THEN  7776000000
			       WHEN 'business'   THEN 31104000000
			       WHEN 'enterprise' THEN          -1
			       ELSE 2592000000
			   END
			  FROM tenants t
			 WHERE q.tenant_id = t.id;
		`)
		require.NoError(t, err)

		// Verify each plan got the right cap. Values must match
		// internal/domain/plans.go:planTiers exactly — if a future
		// change drops or swaps a WHEN, this test fails with a clear diff.
		type expectation struct {
			plan    string
			wantCap int
		}
		expected := []expectation{
			{"free", 2592000000},      // 30 days × 86_400_000 ms/day
			{"pro", 7776000000},       // 90 days × 86_400_000 ms/day
			{"business", 31104000000}, // 360 days × 86_400_000 ms/day
			{"enterprise", -1},        // unlimited
			{"unknown", 2592000000},   // ELSE falls back to free-tier
		}
		for _, e := range expected {
			var got int
			require.NoError(t, subDB.Get(&got,
				"SELECT max_compute_ms_per_month FROM quotas WHERE tenant_id=$1",
				"t_compms_"+e.plan))
			require.Equalf(t, e.wantCap, got,
				"plan %q: quotas.max_compute_ms_per_month = %d, want %d (031 backfill drifted?)",
				e.plan, got, e.wantCap)
		}

		// And used_compute_ms is 0 across the board (default).
		var usedCount int
		require.NoError(t, subDB.Get(&usedCount,
			"SELECT COUNT(*) FROM quotas WHERE used_compute_ms != 0"))
		require.Equalf(t, 0, usedCount,
			"every quota row should have used_compute_ms=0 after backfill")
	})

	t.Run("BillingUsageRepository_Roundtrip", func(t *testing.T) {
		// Exercise the metering ledger (issue #485) end-to-end against
		// the migrated DB. This is the roundtrip contract for the
		// MeteringDrainer + heartbeat-pipeline dual-write:
		//
		//  1. Enqueue a row (heartbeat path).
		//  2. Enqueue a second row with the same idempotency_key →
		//     ErrDuplicateIdempotencyKey (UNIQUE contract).
		//  3. ClaimDue returns the row (drainer path).
		//  4. MarkProcessed flips processed_at IS NOT NULL.
		//  5. A subsequent ClaimDue returns no rows.
		//  6. EnqueueUsageEvent absorbs duplicates silently (the
		//     heartbeat pipeline treats redeliveries as "already
		//     recorded" — no error surfaced to the caller).
		//  7. CHECK constraint on kind rejects unknown values.
		_, err := db.Exec(`INSERT INTO tenants (id, name, plan, created_at, updated_at)
			VALUES ('t_meter_round', 'metering-roundtrip', 'pro', NOW(), NOW())`)
		require.NoError(t, err)

		repo := repository.NewBillingUsageRepository(db)
		ctx := context.Background()

		// 1. Enqueue.
		err = repo.Enqueue(ctx, &domain.MeterUsageEvent{
			TenantID:       "t_meter_round",
			Kind:           domain.MeterKindResidentSeconds,
			Quantity:       30,
			IdempotencyKey: "roundtrip:resident_seconds:bucket1",
			Provider:       "",
		})
		require.NoError(t, err, "Enqueue first row")

		// 2. Duplicate idempotency_key → ErrDuplicateIdempotencyKey.
		err = repo.Enqueue(ctx, &domain.MeterUsageEvent{
			TenantID:       "t_meter_round",
			Kind:           domain.MeterKindResidentSeconds,
			Quantity:       30,
			IdempotencyKey: "roundtrip:resident_seconds:bucket1",
		})
		require.ErrorIs(t, err, repository.ErrDuplicateIdempotencyKey,
			"second Enqueue with same idempotency_key must surface as ErrDuplicateIdempotencyKey")

		// 3. ClaimDue returns the row.
		rows, err := repo.ClaimDue(ctx, 50)
		require.NoError(t, err)
		require.Len(t, rows, 1, "ClaimDue must return the single unprocessed row")
		require.Equal(t, "t_meter_round", rows[0].TenantID)
		require.Equal(t, domain.MeterKindResidentSeconds, rows[0].Kind)
		require.Equal(t, int64(30), rows[0].Quantity)
		require.Equal(t, "roundtrip:resident_seconds:bucket1", rows[0].IdempotencyKey)
		require.NotNil(t, rows[0].ProcessedAt, "ClaimDue must stamp processed_at")

		// 4. MarkProcessed — already stamped by ClaimDue; this call
		// confirms the defensive "re-mark on already-processed row"
		// guard does not error (operator SQL poking).
		require.NoError(t, repo.MarkProcessed(ctx, rows[0].ID))

		// 5. A subsequent ClaimDue returns no rows.
		rows2, err := repo.ClaimDue(ctx, 50)
		require.NoError(t, err)
		require.Len(t, rows2, 0, "no unprocessed rows remain after MarkProcessed")

		// 6. EnqueueUsageEvent absorbs duplicates silently.
		require.NoError(t, repo.EnqueueUsageEvent(ctx, "t_meter_round",
			domain.MeterKindResidentSeconds, 60, "roundtrip:resident_seconds:bucket2"),
			"EnqueueUsageEvent first call")
		require.NoError(t, repo.EnqueueUsageEvent(ctx, "t_meter_round",
			domain.MeterKindResidentSeconds, 60, "roundtrip:resident_seconds:bucket2"),
			"EnqueueUsageEvent duplicate must absorb silently")

		// 7. CHECK constraint on kind: an invalid kind must error.
		_, err = db.Exec(`INSERT INTO billing_usage_events
			(tenant_id, kind, quantity, idempotency_key, provider)
			VALUES ($1, $2, 1, 'badsort', '')`, "t_meter_round", "not_a_real_kind")
		require.Error(t, err, "CHECK constraint on kind must reject unknown kinds")
		require.Contains(t, strings.ToLower(err.Error()), "check",
			"rejection must come from the CHECK constraint, not some other source")
	})
}

// newTestPostgres boots a postgres:16-alpine testcontainer. We use
// BasicWaitStrategies so the container reports "ready" only after
// pg_isready succeeds — without it the first connection from
// repository.NewDB can race the inner pg_isready loop on Mac/Windows
// runners and flake.
func newTestPostgres(t *testing.T, ctx context.Context) *tcpg.PostgresContainer {
	t.Helper()
	pgC, err := tcpg.Run(ctx,
		"postgres:16-alpine",
		tcpg.WithDatabase("edgecloud_test"),
		tcpg.WithUsername("test"),
		tcpg.WithPassword("test"),
		tcpg.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	require.NotNil(t, pgC)
	return pgC
}

// newDBFromContainer opens a *sqlx.DB via the production NewDB helper
// (internal/repository/db.go:27). Reusing the helper means the test
// exercises the same MaxOpenConns/MaxIdleConns/ConnMaxLifetime config
// as the API server, not a side-channel configuration.
func newDBFromContainer(t *testing.T, ctx context.Context, pgC *tcpg.PostgresContainer) *sqlx.DB {
	t.Helper()
	// testcontainers' ConnectionString returns `postgres://...?` with no
	// query params, which lib/pq parses as sslmode=require — Postgres
	// 16-alpine defaults to SSL enabled, so the connection fails with
	// "pq: SSL is not enabled on the server". Passing sslmode=disable
	// explicitly matches the production DSN format in
	// internal/config/config.go:DatabaseConfig.DSN().
	connStr, err := pgC.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := repository.NewDB(connStr)
	require.NoError(t, err)
	return db
}

// migrationsDir resolves the migrations directory from this file's
// own location via runtime.Caller(0). Avoids depending on the runner's
// working directory, so `go test ./migrations/...` from any CWD lands
// in the same place.
func migrationsDir(t *testing.T) string {
	t.Helper()
	_, here, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller failed")
	return filepath.Dir(here)
}

func assertTableExists(t *testing.T, db *sqlx.DB, name string) {
	t.Helper()
	var n int
	require.NoError(t, db.Get(&n,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name=$1",
		name))
	require.Equalf(t, 1, n, "table %q missing after migrations", name)
}

func assertTableAbsent(t *testing.T, db *sqlx.DB, name string) {
	t.Helper()
	var n int
	require.NoError(t, db.Get(&n,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name=$1",
		name))
	require.Equalf(t, 0, n, "table %q still present after rollback to v0", name)
}

func assertColumnsExist(t *testing.T, db *sqlx.DB, table string, columns []string) {
	t.Helper()
	for _, col := range columns {
		var n int
		require.NoError(t, db.Get(&n,
			"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name=$1 AND column_name=$2",
			table, col))
		require.Equalf(t, 1, n, "table %q is missing expected column %q after migrations", table, col)
	}
}

// assertColumnTypes verifies each (table, column) pair in types has
// the expected PostgreSQL udt_name. Single SELECT per column. Catches
// accidental type drift in a migration (e.g. TEXT → BIGINT, or
// VARCHAR(45) → INT4). The udt_name values match what wantTypes
// declares — see that map's doc comment for the encoding rules.
func assertColumnTypes(t *testing.T, db *sqlx.DB, types map[string]map[string]string) {
	t.Helper()
	for table, typeMap := range types {
		for col, want := range typeMap {
			var got string
			require.NoError(t, db.Get(&got, `
				SELECT udt_name FROM information_schema.columns
				 WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2`,
				table, col))
			require.Equalf(t, want, got,
				"table %q column %q has udt_name %q, want %q (type drifted in a migration?)",
				table, col, got, want)
		}
	}
}

// assertNotNull verifies each column in notNullCols has
// is_nullable='NO'. Columns in wantColumns but NOT in wantNotNull are
// implicitly asserted to be nullable — if a migration flips a column
// to NOT NULL, add it here; if it flips to NULL, remove it (and update
// the inline comment to reference the migration).
func assertNotNull(t *testing.T, db *sqlx.DB, notNullCols map[string][]string) {
	t.Helper()
	for table, cols := range notNullCols {
		for _, col := range cols {
			var n int
			require.NoError(t, db.Get(&n, `
				SELECT COUNT(*) FROM information_schema.columns
				 WHERE table_schema = 'public' AND table_name = $1
				   AND column_name = $2 AND is_nullable = 'NO'`,
				table, col))
			require.Equalf(t, 1, n,
				"table %q column %q should be NOT NULL but isn't (nullability drifted in a migration?)",
				table, col)
		}
	}
}

// assertIndexesExist verifies each named index lives in the public
// schema on the expected table. Queries pg_indexes by index name
// (unique within a schema); the returned tablename must match the
// expectation. Column ordering, included columns, and uniqueness
// properties are NOT asserted — out of scope for this layer.
func assertIndexesExist(t *testing.T, db *sqlx.DB, expected []IndexExpectation) {
	t.Helper()
	for _, want := range expected {
		var tablename string
		err := db.Get(&tablename, `
			SELECT tablename FROM pg_indexes
			 WHERE schemaname = 'public' AND indexname = $1`,
			want.Name)
		if errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected index %q (on table %q) is missing from public schema (DROPped or renamed in a migration?)", want.Name, want.Table)
		}
		require.NoError(t, err)
		require.Equalf(t, want.Table, tablename,
			"index %q lives on table %q, want %q (moved to a different table in a migration?)",
			want.Name, tablename, want.Table)
	}
}

// assertForeignKeys verifies every expected FK exists with its
// expected definition (pg_get_constraintdef() output). Uses pg_catalog
// directly because information_schema.referential_constraints
// renders inconsistently across PG versions.
//
// One SELECT per FK, joining pg_constraint with pg_class to scope to
// the public schema. The expected Definition string must match the
// pg_get_constraintdef() output verbatim.
func assertForeignKeys(t *testing.T, db *sqlx.DB, expected map[string][]ForeignKeyExpectation) {
	t.Helper()
	for table, fks := range expected {
		for _, want := range fks {
			var got string
			err := db.Get(&got, `
				SELECT pg_get_constraintdef(con.oid)
				  FROM pg_constraint con
				  JOIN pg_namespace nsp ON nsp.oid = con.connamespace
				 WHERE nsp.nspname = 'public'
				   AND con.conname = $1`,
				want.Constraint)
			if errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("table %q is missing expected foreign key %q (DROPped in a migration?)", table, want.Constraint)
			}
			require.NoError(t, err)
			require.Equalf(t, want.Definition, got,
				"foreign key %q on table %q has unexpected definition (got %q, want %q)",
				want.Constraint, table, got, want.Definition)
		}
	}
}

// assertCheckConstraints verifies every expected CHECK constraint
// exists with its expected clause (pg_get_constraintdef() output).
//
// Each map key is "table_name.constraint_name" (dot-separated, since
// constraint names are unique within a schema but not within a table).
// The value must match pg_get_constraintdef() output verbatim.
func assertCheckConstraints(t *testing.T, db *sqlx.DB, expected map[string]string) {
	t.Helper()
	for qualified, wantClause := range expected {
		// Parse "table_name.constraint_name" — table name contains
		// no dots in our schema; constraint names don't either.
		idx := strings.LastIndex(qualified, ".")
		require.Greaterf(t, idx, 0, "wantChecks key %q must be table.constraint format", qualified)
		constraintName := qualified[idx+1:]

		var got string
		err := db.Get(&got, `
			SELECT pg_get_constraintdef(con.oid)
			  FROM pg_constraint con
			  JOIN pg_namespace nsp ON nsp.oid = con.connamespace
			 WHERE nsp.nspname = 'public'
			   AND con.conname = $1`,
			constraintName)
		if errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected CHECK constraint %q is missing from public schema (DROPped or renamed in a migration?)", qualified)
		}
		require.NoError(t, err)
		require.Equalf(t, wantClause, got,
			"CHECK constraint %q has unexpected clause (loosened or tightened in a migration?)",
			qualified)
	}
}

// assertDefaults verifies every (table, column) in expected has the
// expected column_default. Catches DEFAULT drift: changing `DEFAULT
// 'free'` to `DEFAULT 'trial'` on tenants.plan would silently change
// every new tenant's plan tier without a code change.
//
// Columns NOT in this map are implicitly asserted to have
// column_default IS NULL — if a future migration adds a DEFAULT to a
// previously-default-less column, this test fails (forcing the
// contract update to be explicit).
func assertDefaults(t *testing.T, db *sqlx.DB, expected map[string]map[string]string) {
	t.Helper()
	for table, cols := range expected {
		for col, wantDefault := range cols {
			var got *string
			require.NoError(t, db.Get(&got, `
				SELECT column_default FROM information_schema.columns
				 WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2`,
				table, col))
			require.NotNilf(t, got, "table %q column %q has no column_default — expected %q (DEFAULT removed in a migration?)", table, col, wantDefault)
			require.Equalf(t, wantDefault, *got,
				"table %q column %q column_default drifted (got %q, want %q)",
				table, col, *got, wantDefault)
		}
	}
}

// assertMigrationsLexicallyOrdered guards against two classes of bug:
//
//  1. Non-zero-padded prefix: a new migration named `2_*.sql` instead
//     of `002_*.sql` would sort AFTER `10_*.sql`, breaking the apply
//     order silently. Catches it at PR time.
//
//  2. Missing pair: each `NNN_name.up.sql` must have a matching
//     `NNN_name.down.sql` (rubenv produces one Migration record per
//     file; an orphan up or down file would be tracked but never
//     produce SQL on one side of the round-trip).
//
// Note on order: with the split-file format, lexicographic order
// interleaves as `001.down.sql, 001.up.sql, 002.down.sql, 002.up.sql,
// ...` because 'd' < 'u'. That's fine — each side of the pair has
// the opposite direction's SQL as empty, so the net effect applies
// migrations in logical order.
func assertMigrationsLexicallyOrdered(t *testing.T, dir string) {
	t.Helper()
	entries, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	require.NoError(t, err)

	// Map each base name to the set of directions present.
	pairs := make(map[string]map[string]struct{})
	for _, e := range entries {
		name := filepath.Base(e)
		switch {
		case strings.HasSuffix(name, ".up.sql"):
			base := strings.TrimSuffix(name, ".up.sql")
			if pairs[base] == nil {
				pairs[base] = map[string]struct{}{}
			}
			pairs[base]["up"] = struct{}{}
		case strings.HasSuffix(name, ".down.sql"):
			base := strings.TrimSuffix(name, ".down.sql")
			if pairs[base] == nil {
				pairs[base] = map[string]struct{}{}
			}
			pairs[base]["down"] = struct{}{}
		default:
			t.Fatalf("migration file %q does not match *.up.sql or *.down.sql", name)
		}
	}

	// Every base must have both up and down files.
	for base, dirs := range pairs {
		_, hasUp := dirs["up"]
		_, hasDown := dirs["down"]
		require.Truef(t, hasUp, "missing .up.sql for %s", base)
		require.Truef(t, hasDown, "missing .down.sql for %s", base)
	}

	// Sort by the numeric prefix to detect `2_*.sql` < `10_*.sql` mistakes.
	bases := make([]string, 0, len(pairs))
	for base := range pairs {
		bases = append(bases, base)
	}
	sort.Strings(bases)
	// The first underscore-separated token must be a zero-padded
	// integer, AND the lex order must be non-decreasing in the parsed
	// integer. Multiple migrations can share a prefix (e.g. 005_add_last_good,
	// 005_api_key_hash_algorithm, 005_logs); equal prefixes are fine,
	// but a smaller int after a larger one means the prefix wasn't
	// zero-padded (e.g. "10_*" sort after "2_*" instead of before).
	prev := -1
	for _, base := range bases {
		idx := strings.Index(base, "_")
		require.Greaterf(t, idx, 0, "migration %q has no NNN_ prefix", base)
		num, err := strconv.Atoi(base[:idx])
		require.NoErrorf(t, err, "migration %q prefix is not a pure integer; zero-pad it (e.g. 002_)", base)
		require.GreaterOrEqualf(t, num, prev,
			"migration %q (numeric prefix %d) sorts before %d — looks like a non-zero-padded prefix",
			base, num, prev)
		prev = num
	}
}
