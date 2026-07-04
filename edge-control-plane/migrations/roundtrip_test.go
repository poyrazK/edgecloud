//go:build integration
// +build integration

// Package migrations_test exercises every migration in this directory
// against a real Postgres via testcontainers, proving two contracts:
//
//  1. Forward apply: every *.sql file's Up section parses under
//     rubenv/sql-migrate and applies cleanly to a fresh database.
//     Catches malformed markers, partial-index / CONCURRENTLY-in-TX
//     issues, and inner BEGIN/COMMIT-vs-outer-TX collisions that the
//     sqlmock-based repository tests silently allow.
//
//  2. Round-trip reversibility: rolling all the way back to version 0
//     and reapplying succeeds. Catches asymmetries between an
//     *.up.sql body and its corresponding *.down.sql body — e.g. a
//     migration that adds a column without dropping it on rollback,
//     leaving subsequent reapply in an inconsistent state.
//
// The file is build-tag-gated so the default `go test ./...` CI run
// does NOT spin Docker. Run locally with:
//
//	cd edge-control-plane
//	go test -tags=integration -v -count=1 ./migrations/...
//
// CI runs it under `go-test-integration` (services: postgres:16-alpine).
// See .github/workflows/ci.yml.

package migrations_test

import (
	"context"
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

// expectedMigrationCount must stay in lockstep with the number of *.sql
// files in this directory. The apply + rollback assertions both check
// against this number so a drift (e.g. someone adds 014_*.sql without
// updating this constant) fails loudly.
const expectedMigrationCount = 18

// wantTables is the post-013 expected set of public-schema tables.
// Update this when adding a migration that creates a new table. The
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
		require.Equal(t, expectedMigrationCount, n)

		// rubenv tracks applied migrations in gorp_migrations (default
		// TableName; verified at migrate.go:50-55). Cross-check via
		// the tracking table instead of trusting the return value alone
		// — protects against future library changes to count semantics.
		var tracked int
		require.NoError(t, db.Get(&tracked, "SELECT COUNT(*) FROM gorp_migrations"))
		require.Equal(t, expectedMigrationCount, tracked)

		for _, want := range wantTables {
			assertTableExists(t, db, want)
		}
	})

	t.Run("DownReversesAllToVersionZero", func(t *testing.T) {
		// migrate.Exec(Down) walks every applied migration in reverse
		// and applies each Down section. ExecVersion(0, Down) would
		// fail because rubenv's planner looks up the target version
		// via VersionInt() (migrate.go:686) and no migration has
		// version-int 0 — the prefix regex starts at 1.
		n, err := migrate.Exec(db.DB, "postgres", &migrate.FileMigrationSource{Dir: src}, migrate.Down)
		require.NoError(t, err)
		require.Equal(t, expectedMigrationCount, n)

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
		require.Equal(t, expectedMigrationCount, n)
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
