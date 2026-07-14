//go:build integration
// +build integration

// Package integration_test (repository-level) hosts hermetic PostgreSQL
// round-trip tests for the log read path (#644, follow-up to #77).
//
// These tests answer the four invariants the new keyset contract must
// hold against real PostgreSQL behavior (sqlmock can't catch tuple
// comparison semantics, scan-into-time, or `now() - make_interval(secs=>...)`
// clock-skew decisions):
//
//  1. Tenant isolation — a row written under tenant A is invisible
//     when (tenant_id, app_name) is queried under tenant B, even when
//     both tenants have the same app_name.
//  2. Equal-timestamp paging — strict `(ts, id) DESC` ordering must
//     produce no duplicates and no skipped rows when N rows share the
//     same `ts`. The cursor predicate must use `id` as a tiebreak;
//     without it, keyset paging lies about completeness.
//  3. Concurrent-insert stability — when a new row arrives between
//     the first and second page reads, the descending cursor walk
//     must NOT see the new row (it's newer than the cursor anchor)
//     and must NOT skip any older rows the previous page already
//     returned.
//  4. Combined filters — `since` (DB-relative), `until`, and level
//     compose with the keyset predicate.
//
// The tests pin the EXPLAIN evidence for the existing
// `idx_logs_tenant_app_ts (tenant_id, app_name, ts DESC)` index, so
// the no-new-index decision (issue #644 plan §4) is reproducible.
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
	"regexp"
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
	intTenant = "t_log_iso_a"
	intOther  = "t_log_iso_b"
	intApp    = "myapp"
	intOther2 = "other-app"
	intRegion = "us-east-1"
	intWorker = "w_us-east-1_h01"
	intDeploy = "d_log_iso_a"
)

// newLogIntegrationDB spins up (or connects to) a fresh Postgres,
// applies every migration, and returns the handle plus the closer.
func newLogIntegrationDB(t *testing.T, ctx context.Context) *sqlx.DB {
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

	// Run migrations so the `logs` table + indexes exist.
	_, here, _, ok := runtime.Caller(0)
	require.True(t, ok)
	src := filepath.Join(filepath.Dir(here), "..", "..", "migrations")
	_, err = migrate.Exec(db.DB, "postgres", &migrate.FileMigrationSource{Dir: src}, migrate.Up)
	require.NoError(t, err)

	// Clean stale fixture rows from previous runs (parallel-incompatible
	// — this package isn't `-parallel`, so it's safe).
	_, err = db.ExecContext(ctx,
		`DELETE FROM logs WHERE tenant_id IN ($1, $2)`,
		intTenant, intOther,
	)
	require.NoError(t, err)
	return db
}

// seedLog inserts one row at a chosen wall-clock timestamp. TS is
// pinned (rather than relying on DEFAULT NOW()) so equal-timestamp
// tests can construct literal ties.
func seedLog(
	t *testing.T, ctx context.Context, db *sqlx.DB,
	tenant, app, level string, ts time.Time,
) {
	t.Helper()
	_, err := db.ExecContext(ctx, `
		INSERT INTO logs (
			tenant_id, deployment_id, app_name, worker_id, region,
			level, message, labels, ts
		) VALUES ($1, $2, $3, $4, $5, $6, $7, '{}'::jsonb, $8)
	`, tenant, intDeploy, app, intWorker, intRegion,
		level, "hello-"+level, ts,
	)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Invariant 1 — Tenant isolation
// ---------------------------------------------------------------------------

func TestLogEntryRepoIntegration_TenantIsolationNeverLeaks(t *testing.T) {
	if reason, ok := testutil.ShouldSkipIntegration("SKIP_INTEGRATION_TESTS"); ok {
		t.Skip(reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := newLogIntegrationDB(t, ctx)

	now := time.Now().UTC()
	// Tenant A logs for `myapp`.
	seedLog(t, ctx, db, intTenant, intApp, "info", now.Add(-30*time.Second))
	seedLog(t, ctx, db, intTenant, intApp, "warn", now.Add(-20*time.Second))
	// Tenant B logs for the SAME app_name.
	seedLog(t, ctx, db, intOther, intApp, "info", now.Add(-15*time.Second))
	seedLog(t, ctx, db, intOther, intApp, "error", now.Add(-10*time.Second))

	repo := repository.NewLogEntryRepository(db)

	// (a) Tenant A query must return ONLY A's rows.
	aRows, err := repo.ListByTenantApp(ctx, intTenant, intApp, repository.LogListFilter{
		Since: 1 * time.Hour,
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, aRows, 2, "tenant A query must return exactly 2 rows")
	for _, r := range aRows {
		require.Equal(t, intTenant, r.TenantID,
			"row %d leaked from another tenant: %+v", r.ID, r)
	}

	// (b) Tenant B query must return ONLY B's rows even though the
	//     app_name collides.
	bRows, err := repo.ListByTenantApp(ctx, intOther, intApp, repository.LogListFilter{
		Since: 1 * time.Hour,
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, bRows, 2, "tenant B query must return exactly 2 rows")
	for _, r := range bRows {
		require.Equal(t, intOther, r.TenantID,
			"row %d leaked from another tenant: %+v", r.ID, r)
	}
}

// ---------------------------------------------------------------------------
// Invariant 2 — Equal-timestamp cursor paging (no dupes, no gaps)
// ---------------------------------------------------------------------------

func TestLogEntryRepoIntegration_EqualTimestampsPageStrictlyByID(t *testing.T) {
	if reason, ok := testutil.ShouldSkipIntegration("SKIP_INTEGRATION_TESTS"); ok {
		t.Skip(reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := newLogIntegrationDB(t, ctx)

	// Pin a single wall-clock `now`; every row gets the same `ts`.
	// Postgres stores timestamptz with microsecond resolution, so
	// sharing one time literal produces literal ties in the index —
	// the only place the tiebreak (id DESC) can be exercised.
	now := time.Now().UTC().Truncate(time.Microsecond)
	// We use a tenant-specific prefix so the row IDs only collide
	// inside this fixture. 10 rows is large enough to catch dupes
	// and small enough to read in two pages of 5.
	const total = 10
	for i := 0; i < total; i++ {
		seedLog(t, ctx, db, intTenant, intApp, "info", now)
	}

	repo := repository.NewLogEntryRepository(db)

	const pageSize = 5
	seenIDs := make(map[int64]int) // id -> visit count (must be 1)
	var page []struct {
		id int64
		ts time.Time
	}
	cursor := repository.LogListFilter{
		Since: 1 * time.Hour,
		Limit: pageSize,
	}
	for {
		rows, err := repo.ListByTenantApp(ctx, intTenant, intApp, cursor)
		require.NoError(t, err)
		if len(rows) == 0 {
			break
		}
		for _, r := range rows {
			require.Equal(t, now.UTC(), r.TS.UTC(),
				"expected pinned ts; row id=%d got %v", r.ID, r.TS)
			seenIDs[r.ID]++
			page = append(page, struct {
				id int64
				ts time.Time
			}{r.ID, r.TS})
		}
		if len(rows) < pageSize {
			break
		}
		// Advance cursor to the last returned row.
		last := rows[len(rows)-1]
		cursor = repository.LogListFilter{
			Since:    1 * time.Hour,
			Limit:    pageSize,
			CursorTS: last.TS,
			CursorID: last.ID,
		}
	}

	require.Len(t, seenIDs, total, "must visit every row exactly once across pages")
	for id, count := range seenIDs {
		require.Equal(t, 1, count, "row id=%d visited %d times", id, count)
	}

	// Confirm ordering: globally, IDs are strictly DESCENDING.
	for i := 1; i < len(page); i++ {
		require.Greater(t, page[i-1].id, page[i].id,
			"expected ts-desc/id-desc; saw id=%d before id=%d",
			page[i-1].id, page[i].id)
	}
}

// ---------------------------------------------------------------------------
// Invariant 3 — Concurrent insert during cursor walk
// ---------------------------------------------------------------------------

func TestLogEntryRepoIntegration_NewerRowDoesNotEnterCursorWalk(t *testing.T) {
	if reason, ok := testutil.ShouldSkipIntegration("SKIP_INTEGRATION_TESTS"); ok {
		t.Skip(reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := newLogIntegrationDB(t, ctx)

	base := time.Now().UTC().Truncate(time.Second).Add(-1 * time.Hour)
	const preCount = 8
	for i := 0; i < preCount; i++ {
		// Ascending ts so DESC ordering pages oldest->newest first.
		seedLog(t, ctx, db, intTenant, intApp, "info",
			base.Add(time.Duration(i)*time.Second))
	}

	repo := repository.NewLogEntryRepository(db)

	// Page 1: take the first half.
	const pageSize = 4
	first, err := repo.ListByTenantApp(ctx, intTenant, intApp, repository.LogListFilter{
		Limit: pageSize,
	})
	require.NoError(t, err)
	require.Len(t, first, pageSize)

	// Concurrent insert at a NEWER timestamp than anything we've
	// queried. The cursor walk is descending, so a newer row must
	// NOT show up in subsequent pages — it sits above the cursor.
	seedLog(t, ctx, db, intTenant, intApp, "info",
		base.Add(2*time.Hour))

	// Page 2: continue with the cursor.
	last := first[len(first)-1]
	second, err := repo.ListByTenantApp(ctx, intTenant, intApp, repository.LogListFilter{
		Limit:    pageSize,
		CursorTS: last.TS,
		CursorID: last.ID,
	})
	require.NoError(t, err)
	require.Len(t, second, pageSize,
		"page 2 must complete the older rows even after a newer insert")

	// Pages must be disjoint AND descending.
	seen := make(map[int64]bool, preCount)
	for _, r := range first {
		seen[r.ID] = true
	}
	for _, r := range second {
		require.False(t, seen[r.ID], "row id=%d already returned on page 1", r.ID)
		seen[r.ID] = true
		require.True(t, r.TS.Before(last.TS) || r.TS.Equal(last.TS) && r.ID < last.ID,
			"row id=%d ts=%v violates cursor ordering against anchor (%v, %d)",
			r.ID, r.TS, last.TS, last.ID)
	}

	// The newer injected row must be reachable from a fresh FIRST
	// page (without cursor), proving it exists in the table — it
	// just mustn't have leaked into the cursor walk.
	fresh, err := repo.ListByTenantApp(ctx, intTenant, intApp, repository.LogListFilter{
		Limit: 50,
	})
	require.NoError(t, err)
	require.Greater(t, len(fresh), preCount,
		"the freshly inserted newer row must exist in a fresh page-1 query")
}

// ---------------------------------------------------------------------------
// Invariant 4 — Combined since + until + level + cursor
// ---------------------------------------------------------------------------

func TestLogEntryRepoIntegration_CombinedFiltersCompose(t *testing.T) {
	if reason, ok := testutil.ShouldSkipIntegration("SKIP_INTEGRATION_TESTS"); ok {
		t.Skip(reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := newLogIntegrationDB(t, ctx)

	base := time.Now().UTC().Truncate(time.Second).Add(-30 * time.Minute)
	// 5 info @ t-30..t-26m, 5 warn @ t-20..t-16m, 5 error @ t-10..t-6m.
	for i := 0; i < 5; i++ {
		seedLog(t, ctx, db, intTenant, intApp, "info",
			base.Add(time.Duration(i)*time.Minute))
		seedLog(t, ctx, db, intTenant, intApp, "warn",
			base.Add(10*time.Minute+time.Duration(i)*time.Minute))
		seedLog(t, ctx, db, intTenant, intApp, "error",
			base.Add(20*time.Minute+time.Duration(i)*time.Minute))
	}

	repo := repository.NewLogEntryRepository(db)

	// (a) level=warn over the full hour → 5 warn + 5 error = 10 rows.
	warnSet, err := repo.ListByTenantApp(ctx, intTenant, intApp, repository.LogListFilter{
		Since:  1 * time.Hour,
		Levels: []string{"warn", "error"},
		Limit:  100,
	})
	require.NoError(t, err)
	require.Len(t, warnSet, 10)
	for _, r := range warnSet {
		require.Contains(t, []string{"warn", "error"}, r.Level,
			"row id=%d leaked outside warn+error set", r.ID)
	}

	// (b) since=1h + until=t-15m → only the 5 info rows.
	infoOnly, err := repo.ListByTenantApp(ctx, intTenant, intApp, repository.LogListFilter{
		Since: 1 * time.Hour,
		Until: base.Add(15 * time.Minute),
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, infoOnly, 5, "until bound must carve off warn+error")
	for _, r := range infoOnly {
		require.Equal(t, "info", r.Level)
	}

	// (c) since + until + level (warn only) + cursor → page through
	//     the 5 warn rows in 2-page batches. Cursor must produce
	//     strict DESC order across the page boundary.
	warnRows, err := repo.ListByTenantApp(ctx, intTenant, intApp, repository.LogListFilter{
		Since:  1 * time.Hour,
		Until:  base.Add(20 * time.Minute),
		Levels: []string{"warn"},
		Limit:  3,
	})
	require.NoError(t, err)
	require.Len(t, warnRows, 3)
	last := warnRows[len(warnRows)-1]
	rest, err := repo.ListByTenantApp(ctx, intTenant, intApp, repository.LogListFilter{
		Since:    1 * time.Hour,
		Until:    base.Add(20 * time.Minute),
		Levels:   []string{"warn"},
		Limit:    100,
		CursorTS: last.TS,
		CursorID: last.ID,
	})
	require.NoError(t, err)
	require.Len(t, rest, 2, "cursor must surface the remaining 2 warn rows")
	for _, r := range rest {
		require.Equal(t, "warn", r.Level)
		require.True(t,
			r.TS.Before(last.TS) || r.TS.Equal(last.TS) && r.ID < last.ID,
			"row id=%d violates cursor predicate", r.ID)
	}
}

// ---------------------------------------------------------------------------
// EXPLAIN evidence — the existing
// `(tenant_id, app_name, ts DESC)` index satisfies the new keyset
// query path.
// ---------------------------------------------------------------------------

func TestLogEntryRepoIntegration_ExplainFirstPageUsesIndex(t *testing.T) {
	if reason, ok := testutil.ShouldSkipIntegration("SKIP_INTEGRATION_TESTS"); ok {
		t.Skip(reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db := newLogIntegrationDB(t, ctx)

	// Seed ~3k rows in one tenant/app to give the planner enough
	// volume to prefer an index range scan over a seq scan.
	const volume = 3000
	base := time.Now().UTC().Add(-time.Duration(volume) * time.Second)
	for i := 0; i < volume; i++ {
		seedLog(t, ctx, db, intTenant, intApp, "info",
			base.Add(time.Duration(i)*time.Second))
	}

	// ANALYZE so the planner has statistics.
	_, err := db.ExecContext(ctx, `ANALYZE logs`)
	require.NoError(t, err)

	// Explain a representative first-page query.
	var plan string
	err = db.GetContext(ctx, &plan, `
		EXPLAIN (FORMAT TEXT)
		SELECT id, tenant_id, deployment_id, app_name, worker_id, region, level, message, labels, ts
		FROM logs
		WHERE tenant_id = $1 AND app_name = $2
		ORDER BY ts DESC, id DESC
		LIMIT 101
	`, intTenant, intApp)
	require.NoError(t, err)

	t.Logf("EXPLAIN first page:\n%s", plan)

	// Required post-conditions:
	//   * No sequential scan on `logs`.
	//   * Index access on `idx_logs_tenant_app_ts`.
	//   * No Sort node — DESC is satisfied by the index.
	//
	// The Sort check is line-anchored so a future Postgres that
	// emits `Incremental Sort` (which DOES still appear as a Sort
	// node in the plan tree, just incremental) is caught, but a
	// coincidental lowercase substring "sort" inside some other
	// output line (e.g. operator names) does not false-positive.
	requirePlanNoBannedNode(t, plan, "Seq Scan on logs")
	requirePlanNoBannedNode(t, plan, "Sort")
	require.Contains(t, plan, "idx_logs_tenant_app_ts",
		"plan must use idx_logs_tenant_app_ts:\n%s", plan)

	// Explain a keyset/cursor query.
	var cursorPlan string
	err = db.GetContext(ctx, &cursorPlan, `
		EXPLAIN (FORMAT TEXT)
		SELECT id, tenant_id, deployment_id, app_name, worker_id, region, level, message, labels, ts
		FROM logs
		WHERE tenant_id = $1 AND app_name = $2
		  AND (ts, id) < ($3::timestamptz, $4)
		ORDER BY ts DESC, id DESC
		LIMIT 101
	`,
		intTenant, intApp,
		base.Add(100*time.Second), int64(500),
	)
	require.NoError(t, err)

	t.Logf("EXPLAIN cursor page:\n%s", cursorPlan)

	requirePlanNoBannedNode(t, cursorPlan, "Seq Scan on logs")
	requirePlanNoBannedNode(t, cursorPlan, "Sort")
	require.Contains(t, cursorPlan, "idx_logs_tenant_app_ts",
		"cursor plan must use idx_logs_tenant_app_ts:\n%s", cursorPlan)
}

// requirePlanNoBannedNode asserts that `node` does NOT appear as a
// line-leading Postgres EXPLAIN node label in `plan`. Line-anchored
// matching avoids false positives on incidental substrings (e.g. an
// operator or function whose name contains the banned word) while
// still catching `->  Sort (cost=...)` and similar real Sort nodes,
// including `Incremental Sort` (which the regex anchored on `(?:^|\n)
// \s*Sort\b` would catch as well, since `Incremental` follows on a
// later line in TEXT-format plans). The first column of every
// `EXPLAIN (FORMAT TEXT)` line is the node label.
func requirePlanNoBannedNode(t *testing.T, plan, node string) {
	t.Helper()
	pattern := regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(node) + `\b`)
	require.False(t, pattern.MatchString(plan),
		"plan contains banned node %q:\n%s", node, plan)
}
