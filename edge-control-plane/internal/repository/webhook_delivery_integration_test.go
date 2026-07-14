//go:build integration
// +build integration

// Package integration_test (repository-level) hosts hermetic PostgreSQL
// round-trip tests for the webhook deliveries read path (issue #659,
// follow-up to #565).
//
// The tests answer the four invariants the new endpoint must hold
// against real PostgreSQL behavior (sqlmock can't catch tuple comparison
// semantics or EXPLAIN decisions):
//
//  1. Tenant isolation — a delivery row written under webhook A
//     belonging to tenant X is invisible when ListDeliveriesByWebhook
//     is queried with a webhook ID belonging to tenant Y. (Service
//     layer enforces ownership; the repo just scopes by webhook_id,
//     but the test pins the SQL never leaks across webhooks.)
//  2. Equal-timestamp paging — strict `(created_at, id) DESC` ordering
//     must produce no duplicates and no skipped rows when N deliveries
//     share the same created_at.
//  3. Concurrent-insert stability — when a new delivery arrives between
//     the first and second page reads, the descending cursor walk must
//     NOT see the new row (newer than the cursor anchor) and must NOT
//     skip any older rows the previous page already returned.
//  4. EXPLAIN evidence — the new query path is covered by the existing
//     idx_webhook_deliveries_webhook composite index from migration
//     015, so no new migration is required.
//
// Run:
//
//	cd edge-control-plane
//	SKIP_INTEGRATION_TESTS= go test -tags=integration -count=1 -v \
//	    ./internal/repository/...
package repository_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/rubenv/sql-migrate"
	"github.com/stretchr/testify/require"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/testutil"
)

const (
	intTenantA   = "t_wh_iso_a"
	intTenantB   = "t_wh_iso_b"
	intWebhookA1 = "wh_a_1"
	intWebhookA2 = "wh_a_2"
	intWebhookB1 = "wh_b_1"
)

// newWebhookIntegrationDB spins up (or connects to) a fresh Postgres,
// applies every migration, and returns the handle plus the closer.
// Mirrors newLogIntegrationDB at log_entry_integration_test.go:68.
func newWebhookIntegrationDB(t *testing.T, ctx context.Context) *sqlx.DB {
	t.Helper()
	var pgC *tcpg.PostgresContainer
	if os.Getenv("DATABASE_HOST") == "" {
		var err error
		pgC, err = tcpg.Run(ctx,
			"postgres:16-alpine",
			tcpg.WithDatabase("edgecloud_test"),
			tcpg.WithUsername("test"),
			tcpg.WithPassword("test"),
			tcpg.BasicWaitStrategies(),
		)
		require.NoError(t, err)
		t.Cleanup(func() {
			cctx, c := context.WithTimeout(context.Background(), 30*time.Second)
			defer c()
			_ = pgC.Terminate(cctx)
		})
	}

	dsn := ""
	if pgC != nil {
		var err error
		dsn, err = pgC.ConnectionString(ctx, "sslmode=disable")
		require.NoError(t, err)
	} else {
		host := os.Getenv("DATABASE_HOST")
		port := os.Getenv("DATABASE_PORT")
		if port == "" {
			port = "5432"
		}
		user := os.Getenv("DATABASE_USER")
		if user == "" {
			user = "test"
		}
		password := os.Getenv("DATABASE_PASSWORD")
		if password == "" {
			password = "test"
		}
		name := os.Getenv("DATABASE_NAME")
		if name == "" {
			name = "edgecloud_test"
		}
		sslmode := os.Getenv("DATABASE_SSLMODE")
		if sslmode == "" {
			sslmode = "disable"
		}
		dsn = fmt.Sprintf(
			"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
			host, port, user, password, name, sslmode,
		)
	}
	db, err := repository.NewDB(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Apply migrations so the `webhooks` and `webhook_deliveries`
	// tables and indexes exist.
	_, here, _, ok := runtime.Caller(0)
	require.True(t, ok)
	src := filepath.Join(filepath.Dir(here), "..", "..", "migrations")
	_, err = migrate.Exec(db.DB, "postgres", &migrate.FileMigrationSource{Dir: src}, migrate.Up)
	require.NoError(t, err)

	// Clean stale fixture rows from previous runs.
	_, err = db.ExecContext(ctx,
		`DELETE FROM webhook_deliveries WHERE webhook_id IN ($1, $2, $3)`,
		intWebhookA1, intWebhookA2, intWebhookB1,
	)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		`DELETE FROM webhooks WHERE id IN ($1, $2, $3)`,
		intWebhookA1, intWebhookA2, intWebhookB1,
	)
	require.NoError(t, err)

	return db
}

// seedWebhook inserts a webhook row.
func seedWebhook(t *testing.T, ctx context.Context, db *sqlx.DB, id, tenantID string) {
	t.Helper()
	_, err := db.ExecContext(ctx, `
		INSERT INTO webhooks (id, tenant_id, url, secret, events, description, enabled, created_at)
		VALUES ($1, $2, $3, $4, '{deploy}', 'test', true, NOW())
	`, id, tenantID, "https://hooks.example.com/"+id, "supersecret12345678")
	require.NoError(t, err)
}

// seedDelivery inserts one webhook_delivery row at a chosen wall-clock
// timestamp. TS is pinned (rather than relying on DEFAULT NOW()) so
// equal-timestamp tests can construct literal ties.
func seedDelivery(
	t *testing.T, ctx context.Context, db *sqlx.DB,
	webhookID, eventType, status string, statusCode int, ts time.Time,
) {
	t.Helper()
	_, err := db.ExecContext(ctx, `
		INSERT INTO webhook_deliveries
			(webhook_id, event_type, status, status_code, request_body, response_body, error_msg, attempt, max_attempts, created_at, completed_at)
		VALUES ($1, $2, $3, $4, '', '', '', 1, 3, $5, $5)
	`, webhookID, eventType, status, statusCode, ts)
	require.NoError(t, err)
}

// TestWebhookDeliveryRepoIntegration_TenantIsolationNeverLeaks pins
// that a delivery row attached to webhook B (tenant B) is never
// returned by ListDeliveriesByWebhook when called for webhook A1
// (tenant A). The repo signature intentionally only takes webhook_id;
// the service layer enforces tenant ownership. This test pins that
// the repo's WHERE clause never leaks across webhooks regardless of
// who's asking.
func TestWebhookDeliveryRepoIntegration_TenantIsolationNeverLeaks(t *testing.T) {
	if reason, ok := testutil.ShouldSkipIntegration("SKIP_INTEGRATION_TESTS"); ok {
		t.Skip(reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db := newWebhookIntegrationDB(t, ctx)

	seedWebhook(t, ctx, db, intWebhookA1, intTenantA)
	seedWebhook(t, ctx, db, intWebhookA2, intTenantA)
	seedWebhook(t, ctx, db, intWebhookB1, intTenantB)

	now := time.Now().UTC()
	// Tenant A: 3 deliveries under wh_a_1, 1 under wh_a_2.
	for i := 0; i < 3; i++ {
		seedDelivery(t, ctx, db, intWebhookA1, "deploy", "success", 200,
			now.Add(-time.Duration(i)*time.Second))
	}
	seedDelivery(t, ctx, db, intWebhookA2, "deploy", "success", 200,
		now.Add(-10*time.Second))

	// Tenant B: 2 deliveries under wh_b_1.
	seedDelivery(t, ctx, db, intWebhookB1, "deploy", "failed", 503, now.Add(-time.Second))
	seedDelivery(t, ctx, db, intWebhookB1, "deploy", "failed", 503, now.Add(-2*time.Second))

	repo := repository.NewWebhookRepository(db)

	// Query for webhook A1 — must see only the 3 rows under A1, none
	// from A2 or B1.
	gotA1, err := repo.ListDeliveriesByWebhook(ctx, repository.WebhookDeliveryListFilter{
		WebhookID: intWebhookA1,
		Limit:     100,
		HasCursor: false,
	})
	require.NoError(t, err)
	require.Len(t, gotA1, 3, "webhook A1 owns 3 deliveries; must see exactly 3")

	for _, d := range gotA1 {
		require.Equal(t, intWebhookA1, d.WebhookID,
			"webhook A1 query must not return rows from another webhook_id")
	}

	// And querying for B1 must return its 2 rows, none from A.
	gotB1, err := repo.ListDeliveriesByWebhook(ctx, repository.WebhookDeliveryListFilter{
		WebhookID: intWebhookB1,
		Limit:     100,
		HasCursor: false,
	})
	require.NoError(t, err)
	require.Len(t, gotB1, 2, "webhook B1 owns 2 deliveries; must see exactly 2")
	for _, d := range gotB1 {
		require.Equal(t, intWebhookB1, d.WebhookID)
	}
}

// TestWebhookDeliveryRepoIntegration_EqualTimestampsPageStrictlyByID
// pins that when N deliveries share an identical created_at, the
// strict-tuple predicate (created_at, id) keeps paging stable — no
// duplicates, no skipped rows across two pages.
func TestWebhookDeliveryRepoIntegration_EqualTimestampsPageStrictlyByID(t *testing.T) {
	if reason, ok := testutil.ShouldSkipIntegration("SKIP_INTEGRATION_TESTS"); ok {
		t.Skip(reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db := newWebhookIntegrationDB(t, ctx)

	seedWebhook(t, ctx, db, intWebhookA1, intTenantA)

	// 6 deliveries all sharing the same created_at. Postgres stores
	// timestamptz with microsecond resolution, so sharing one time
	// literal produces literal ties in the index — the only place
	// the tiebreak (id DESC) can be exercised.
	const total = 6
	now := time.Now().UTC().Truncate(time.Microsecond)
	for i := 0; i < total; i++ {
		seedDelivery(t, ctx, db, intWebhookA1, "deploy", "success", 200, now)
	}

	repo := repository.NewWebhookRepository(db)

	const pageSize = 3
	seenIDs := make(map[int64]int) // id -> visit count (must be 1)
	var order []int64

	cursor := repository.WebhookDeliveryListFilter{
		WebhookID: intWebhookA1,
		Limit:     pageSize,
		HasCursor: false,
	}
	for {
		rows, err := repo.ListDeliveriesByWebhook(ctx, cursor)
		require.NoError(t, err)
		if len(rows) == 0 {
			break
		}
		for _, r := range rows {
			require.Equal(t, now.UTC(), r.CreatedAt.UTC(),
				"expected pinned ts; row id=%d got %v", r.ID, r.CreatedAt)
			seenIDs[r.ID]++
			order = append(order, r.ID)
		}
		if len(rows) < pageSize {
			break
		}
		// Advance cursor to the last returned row.
		last := rows[len(rows)-1]
		cursor = repository.WebhookDeliveryListFilter{
			WebhookID: intWebhookA1,
			Limit:     pageSize,
			HasCursor: true,
			CursorTS:  last.CreatedAt,
			CursorID:  last.ID,
		}
	}

	require.Len(t, seenIDs, total, "must visit every row exactly once across pages")
	for id, count := range seenIDs {
		require.Equal(t, 1, count, "row id=%d visited %d times", id, count)
	}
	// IDs must be strictly DESCENDING across the entire cursor walk.
	for i := 1; i < len(order); i++ {
		require.Greater(t, order[i-1], order[i],
			"expected ts-desc/id-desc; saw id=%d before id=%d",
			order[i-1], order[i])
	}
}

// TestWebhookDeliveryRepoIntegration_NewerRowDoesNotEnterCursorWalk
// pins concurrent-insert stability: a row inserted between page 1
// and page 2 with a NEWER created_at than the cursor anchor must not
// appear in page 2 (it's strictly newer, so the strict-tuple predicate
// filters it out — and that's correct, since the descending walk
// visits OLDER rows).
func TestWebhookDeliveryRepoIntegration_NewerRowDoesNotEnterCursorWalk(t *testing.T) {
	if reason, ok := testutil.ShouldSkipIntegration("SKIP_INTEGRATION_TESTS"); ok {
		t.Skip(reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db := newWebhookIntegrationDB(t, ctx)

	seedWebhook(t, ctx, db, intWebhookA1, intTenantA)

	base := time.Now().UTC().Truncate(time.Second).Add(-1 * time.Hour)
	const preCount = 8
	// 8 OLD rows, ascending timestamps so DESC ordering pages
	// oldest→newest first.
	for i := 0; i < preCount; i++ {
		seedDelivery(t, ctx, db, intWebhookA1, "deploy", "success", 200,
			base.Add(time.Duration(i)*time.Second))
	}

	repo := repository.NewWebhookRepository(db)

	// Page 1: take the first half.
	const pageSize = 4
	first, err := repo.ListDeliveriesByWebhook(ctx, repository.WebhookDeliveryListFilter{
		WebhookID: intWebhookA1,
		Limit:     pageSize,
		HasCursor: false,
	})
	require.NoError(t, err)
	require.Len(t, first, pageSize)

	// Concurrent insert at a NEWER timestamp than anything we've
	// queried. The cursor walk is descending, so a newer row must
	// NOT show up in subsequent pages — it sits above the cursor.
	seedDelivery(t, ctx, db, intWebhookA1, "deploy", "success", 200,
		base.Add(2*time.Hour))

	// Page 2: continue with the cursor.
	last := first[len(first)-1]
	second, err := repo.ListDeliveriesByWebhook(ctx, repository.WebhookDeliveryListFilter{
		WebhookID: intWebhookA1,
		Limit:     pageSize,
		HasCursor: true,
		CursorTS:  last.CreatedAt,
		CursorID:  last.ID,
	})
	require.NoError(t, err)
	require.Len(t, second, pageSize,
		"page 2 must complete the older rows even after a newer insert")

	// Pages must be disjoint AND each page-2 row must be strictly
	// older than the cursor anchor.
	seen := make(map[int64]bool, preCount)
	for _, r := range first {
		seen[r.ID] = true
	}
	for _, r := range second {
		require.False(t, seen[r.ID], "row id=%d already returned on page 1", r.ID)
		seen[r.ID] = true
		require.True(t, r.CreatedAt.Before(last.CreatedAt) ||
			(r.CreatedAt.Equal(last.CreatedAt) && r.ID < last.ID),
			"row id=%d ts=%v violates cursor ordering against anchor (%v, %d)",
			r.ID, r.CreatedAt, last.CreatedAt, last.ID)
	}

	// The newer injected row must be reachable from a fresh FIRST
	// page (without cursor), proving it exists in the table — it
	// just mustn't have leaked into the cursor walk.
	fresh, err := repo.ListDeliveriesByWebhook(ctx, repository.WebhookDeliveryListFilter{
		WebhookID: intWebhookA1,
		Limit:     50,
		HasCursor: false,
	})
	require.NoError(t, err)
	require.Greater(t, len(fresh), preCount,
		"the freshly inserted newer row must exist in a fresh page-1 query")
}

// TestWebhookDeliveryRepoIntegration_ExplainUsesCompositeIndex pins
// EXPLAIN evidence: the new query path is covered by the existing
// idx_webhook_deliveries_webhook (webhook_id, created_at DESC) from
// migration 015 — no new migration is required.
//
// Required post-conditions:
//   - No sequential scan on `webhook_deliveries`.
//   - Index access on `idx_webhook_deliveries_webhook`.
//   - No Sort node — DESC ordering is satisfied by the index.
func TestWebhookDeliveryRepoIntegration_ExplainUsesCompositeIndex(t *testing.T) {
	if reason, ok := testutil.ShouldSkipIntegration("SKIP_INTEGRATION_TESTS"); ok {
		t.Skip(reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db := newWebhookIntegrationDB(t, ctx)

	seedWebhook(t, ctx, db, intWebhookA1, intTenantA)

	// Seed enough rows for the planner to prefer an index scan over
	// a seq scan.
	const volume = 2000
	base := time.Now().UTC().Add(-time.Duration(volume) * time.Second)
	for i := 0; i < volume; i++ {
		seedDelivery(t, ctx, db, intWebhookA1, "deploy", "success", 200,
			base.Add(time.Duration(i)*time.Second))
	}

	_, err := db.ExecContext(ctx, `ANALYZE webhook_deliveries`)
	require.NoError(t, err)

	// First-page EXPLAIN (no cursor).
	var plan string
	err = db.GetContext(ctx, &plan, `
		EXPLAIN (FORMAT TEXT)
		SELECT id, webhook_id, event_type, status, status_code,
		       error_msg, attempt, max_attempts, created_at, completed_at
		FROM webhook_deliveries
		WHERE webhook_id = $1
		ORDER BY created_at DESC, id DESC
		LIMIT $2
	`, intWebhookA1, 51)
	require.NoError(t, err)
	t.Logf("EXPLAIN first page:\n%s", plan)

	requirePlanNoBannedNode(t, plan, "Seq Scan on webhook_deliveries")
	requirePlanNoBannedNode(t, plan, "Sort")
	require.Contains(t, plan, "idx_webhook_deliveries_webhook",
		"plan must use idx_webhook_deliveries_webhook:\n%s", plan)

	// Cursor-page EXPLAIN (strict-tuple predicate).
	cursorTS := base.Add(100 * time.Second)
	var cursorPlan string
	err = db.GetContext(ctx, &cursorPlan, `
		EXPLAIN (FORMAT TEXT)
		SELECT id, webhook_id, event_type, status, status_code,
		       error_msg, attempt, max_attempts, created_at, completed_at
		FROM webhook_deliveries
		WHERE webhook_id = $1 AND (created_at, id) < ($2::timestamptz, $3)
		ORDER BY created_at DESC, id DESC
		LIMIT $4
	`, intWebhookA1, cursorTS, int64(500), 51)
	require.NoError(t, err)
	t.Logf("EXPLAIN cursor page:\n%s", cursorPlan)

	requirePlanNoBannedNode(t, cursorPlan, "Seq Scan on webhook_deliveries")
	requirePlanNoBannedNode(t, cursorPlan, "Sort")
	require.Contains(t, cursorPlan, "idx_webhook_deliveries_webhook",
		"cursor plan must use idx_webhook_deliveries_webhook:\n%s", cursorPlan)
}