//go:build integration
// +build integration

// Package migrations_test also covers issue #58 / #709's strict-tuple
// keyset comparator on the deployments table. The cursor codec
// encodes the LAST visible row's (created_at, id) into the next
// request, and Postgres resolves the next page via
//
//	WHERE tenant_id = $1 AND app_name = $2
//	  AND (created_at < $3 OR (created_at = $3 AND id < $4))
//	ORDER BY created_at DESC, id DESC
//	LIMIT $5
//
// The comparator must handle two distinct cases:
//
//  1. Same-second inserts — `(created_at = $3 AND id < $4)` is the
//     only way to maintain strict total order. Without the tiebreaker,
//     a same-second insert would silently disappear between pages.
//  2. Strict-tuple unique — when both rows have distinct created_at
//     values, the `id < $4` branch is never evaluated. The composite
//     index (tenant_id, app_name, created_at DESC, id DESC) from
//     migration 036 covers both branches.
//
// This file is build-tag-gated so the default `go test ./...` CI run
// does NOT spin Docker. Run locally with:
//
//	cd edge-control-plane
//	go test -tags=integration -v -count=1 -run TestPagination ./migrations/...
//
// CI runs it under `go-test-integration` (services: postgres:16-alpine).
// See .github/workflows/ci.yml.

package migrations_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/rubenv/sql-migrate"
	"github.com/stretchr/testify/require"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/testutil"
)

// listPage mirrors the production repository's pagination shape:
// `(created_at DESC, id DESC)` with limit + 1 to detect the
// probe row. Mirrors deployment.go::ListByAppPaginated SQL. The
// `afterTS`/`afterID` pair together form the strict-tuple cursor;
// the SQL must read both columns, not just one.
func listPage(t *testing.T, db *sqlx.DB, tenantID, appName string, afterTS time.Time, afterID string, limit int) []string {
	t.Helper()
	if afterID == "" {
		// First page: empty cursor => no WHERE clause narrowing.
		// Mirrors the production repository's first-page branch.
		rows := []string{}
		require.NoError(t, db.Select(&rows, `
			SELECT id::text FROM deployments
			WHERE tenant_id = $1 AND app_name = $2
			ORDER BY created_at DESC, id DESC
			LIMIT $3
		`, tenantID, appName, limit+1))
		return rows
	}
	rows := []string{}
	require.NoError(t, db.Select(&rows, `
		SELECT id::text FROM deployments
		WHERE tenant_id = $1 AND app_name = $2
		  AND (created_at < $3::timestamptz
		       OR (created_at = $3::timestamptz AND id < $4::text))
		ORDER BY created_at DESC, id DESC
		LIMIT $5
	`, tenantID, appName, afterTS, afterID, limit+1))
	return rows
}

// TestPagination_ClashingTimestampsWalksCleanly seeds N deployments
// with deliberately clashing `created_at` timestamps (forces the
// strict-tuple tiebreaker into use) and walks the cursor chain to
// exhaustion. Asserts every row appears exactly once across the
// walked pages, in the expected (created_at DESC, id DESC) order.
//
// Without the `id < $4` tiebreaker, the same-second inserts would
// appear in unpredictable order across pages — and any insert with
// id lex-GREATER than the cursor row's id would silently drop out
// of subsequent pages.
func TestPagination_ClashingTimestampsWalksCleanly(t *testing.T) {
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

	// Apply every migration to a fresh DB.
	src := migrationsDir(t)
	_, err := migrate.Exec(db.DB, "postgres", &migrate.FileMigrationSource{Dir: src}, migrate.Up)
	require.NoError(t, err)

	tenantID := "t_pagination_clashing"
	appName := "myapp"
	// Seed the parent tenant — deployments.tenant_id has a FK to
	// tenants.id with ON DELETE CASCADE, so without this row the
	// INSERT fails.
	_, err = db.ExecContext(ctx, `
		INSERT INTO tenants (id, name) VALUES ($1, 'pagination-clashing-tenant')
	`, tenantID)
	require.NoError(t, err, "seed tenant")

	// All 7 rows share the same created_at — only the `id` column
	// breaks ties. Exercises the
	// `created_at = $3 AND id < $4` branch exclusively.
	baseTS := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	ids := []string{
		"d_a0000001", "d_a0000002", "d_a0000003", "d_a0000004",
		"d_a0000005", "d_a0000006", "d_a0000007",
	}
	for _, id := range ids {
		_, err := db.ExecContext(ctx, `
			INSERT INTO deployments
			  (id, tenant_id, app_name, status, hash, regions, created_at,
			   auto_rollback_enabled, signature, signing_key_id,
			   build_attestation, desired_replicas)
			VALUES ($1, $2, $3, 'deployed', $4, ARRAY['us-east-1']::text[], $5,
			        false, 'AQIDBA==', 'k_default', NULL, 0)
		`, id, tenantID, appName, "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899", baseTS)
		require.NoError(t, err, "insert %s", id)
	}

	// Walk the cursor chain with limit=2 (forces 4 pages: 2 + 2 + 2 + 1).
	const pageSize = 2
	collected := []string{}
	var afterTS time.Time
	var afterID string
	for {
		page := listPage(t, db, tenantID, appName, afterTS, afterID, pageSize)
		if len(page) == 0 {
			break
		}
		// limit+1 probe row: trim to visible.
		visible := page
		if len(visible) > pageSize {
			visible = visible[:pageSize]
		}
		collected = append(collected, visible...)
		if len(page) <= pageSize {
			break // final page (no probe row)
		}
		// Next cursor = last visible row's (ts, id).
		var ts time.Time
		require.NoError(t, db.GetContext(ctx, &ts,
			`SELECT created_at FROM deployments WHERE id = $1`, visible[len(visible)-1]))
		afterTS = ts
		afterID = visible[len(visible)-1]
	}

	// The walk's natural order is (created_at DESC, id DESC); under
	// a same-ts tiebreaker, that matches the lex-DESC of the IDs —
	// reverse insertion order.
	want := make([]string, len(ids))
	for i, id := range ids {
		want[len(ids)-1-i] = id
	}
	require.Equal(t, want, collected,
		"walked order should match (created_at DESC, id DESC) under same-ts tiebreaker")
	require.Len(t, collected, len(ids), "every row must appear exactly once")
}

// TestPagination_DistinctTimestampsWalksCleanly mirrors the
// clashing-timestamp test but with strictly distinct `created_at`
// values — exercises the `created_at < $3` branch. The composite
// index covers ORDER BY directly; the `id < $4` clause is never
// evaluated.
func TestPagination_DistinctTimestampsWalksCleanly(t *testing.T) {
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
	_, err := migrate.Exec(db.DB, "postgres", &migrate.FileMigrationSource{Dir: src}, migrate.Up)
	require.NoError(t, err)

	tenantID := "t_pagination_distinct"
	appName := "myapp"
	// Seed the parent tenant — deployments.tenant_id has a FK to
	// tenants.id with ON DELETE CASCADE, so without this row the
	// INSERT fails.
	_, err = db.ExecContext(ctx, `
		INSERT INTO tenants (id, name) VALUES ($1, 'pagination-distinct-tenant')
	`, tenantID)
	require.NoError(t, err, "seed tenant")

	baseTS := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	type seed struct {
		id string
		ts time.Time
	}
	ids := make([]seed, 5)
	for i := range ids {
		ids[i] = seed{
			id: fmt.Sprintf("d_b%07d", i+1),
			ts: baseTS.Add(time.Duration(i) * time.Second),
		}
	}
	for _, s := range ids {
		_, err := db.ExecContext(ctx, `
			INSERT INTO deployments
			  (id, tenant_id, app_name, status, hash, regions, created_at,
			   auto_rollback_enabled, signature, signing_key_id,
			   build_attestation, desired_replicas)
			VALUES ($1, $2, $3, 'deployed', $4, ARRAY['us-east-1']::text[], $5,
			        false, 'AQIDBA==', 'k_default', NULL, 0)
		`, s.id, tenantID, appName, "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899", s.ts)
		require.NoError(t, err, "insert %s", s.id)
	}

	// Walk cursor chain with limit=2.
	const pageSize = 2
	collected := []string{}
	var afterTS time.Time
	var afterID string
	for {
		page := listPage(t, db, tenantID, appName, afterTS, afterID, pageSize)
		if len(page) == 0 {
			break
		}
		visible := page
		if len(visible) > pageSize {
			visible = visible[:pageSize]
		}
		collected = append(collected, visible...)
		if len(page) <= pageSize {
			break
		}
		var ts time.Time
		require.NoError(t, db.GetContext(ctx, &ts,
			`SELECT created_at FROM deployments WHERE id = $1`, visible[len(visible)-1]))
		afterTS = ts
		afterID = visible[len(visible)-1]
	}

	want := make([]string, len(ids))
	for i, s := range ids {
		want[i] = s.id
	}
	sort.Slice(want, func(i, j int) bool { return want[i] > want[j] })
	require.Equal(t, want, collected,
		"walked order should match (created_at DESC, id DESC)")
	require.Len(t, collected, len(ids), "every row must appear exactly once")
}
