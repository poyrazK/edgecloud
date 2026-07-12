//go:build integration
// +build integration

// Package integration_test hosts the cross-language e2e for issue #613:
// a full CP+worker rollback round-trip over real NATS.
//
// This file is the Go half. The Rust half lives at
// edge-worker/tests/cp_rollback_e2e.rs. The two halves share one NATS
// container, orchestrated by scripts/rollback-e2e.sh via sentinel files
// in /tmp/edge-e2e/. Mirrors the cross-language precedent set by PR #652
// (issue #611 wire-contract test).
//
// Flow:
//
//  1. Boot testcontainers Postgres + testcontainers NATS (JetStream on).
//  2. Run migrations, ensure the edgecloud-tasks stream exists.
//  3. Insert a tenant + two deployments (d_a, d_b) with valid Ed25519
//     signatures over the same handler.wasm bytes, distinct IDs.
//  4. ActivateDeployment(d_a) → OutboxDrainer.Tick() → publishes
//     TaskUpdate{apps: {myapp: d_a}} to edgecloud.tasks.test-region.
//  5. ActivateDeployment(d_b) → captures d_a as last_good → publishes
//     TaskUpdate{apps: {myapp: d_b}}.
//  6. RollbackDeployment(tenant, myapp) → tx swaps active back to d_a,
//     clears last_good → publishes TaskUpdate{apps: {myapp: d_a}}.
//  7. Wait for the Rust half to write /tmp/edge-e2e/rust-done (the Rust
//     half asserts the heartbeat deployment_id flips A→B→A).
//  8. Exit; testcontainers tear down.
//
// The Go half does NOT assert anything about worker behavior — that's
// the Rust half's job. We only verify that the CP side (1) writes the
// right outbox rows, (2) drains them to real NATS via a real
// JetStream publisher. The end-to-end "did the worker receive them and
// swap?" is proven by Rust observing heartbeat transitions.
//
// Run locally:
//
//	cd edge-control-plane
//	go test -tags=integration -count=1 -v ./internal/integration/...
//
// Or via the shell driver (also drives the Rust half):
//
//	bash scripts/rollback-e2e.sh
package integration_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
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
	tc "github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"

	natsctl "github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
	sigpkg "github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/signing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/testutil"
)

// sentinelDir is the shared directory the shell driver creates before
// launching us. /tmp/edge-e2e/nats-url holds the NATS URL the Go half
// writes; /tmp/edge-e2e/rust-ready proves the Rust subscriber is live
// before the Go half starts publishing; /tmp/edge-e2e/rust-done tells
// the Go half the worker has finished its assertions.
const sentinelDir = "/tmp/edge-e2e"

const (
	testRegion    = "test-region"
	testTenantID  = "t_rollback_e2e"
	testAppName   = "myapp"
	testArtifact  = "handler.wasm" // matches edge-worker/tests/fixtures/handler.wasm
	deploymentIDA = "d_e2e_a"
	deploymentIDB = "d_e2e_b"
	testKeyID     = "test-kid"
)

// TestRollbackE2E is the Go half of the cross-language rollback
// round-trip. See the package doc for the full flow.
//
// CI runs this in the `go-test-integration` matrix; locally it
// requires Docker. SKIP_INTEGRATION_TESTS=1 bypasses both halves.
func TestRollbackE2E(t *testing.T) {
	if reason, ok := testutil.ShouldSkipIntegration("SKIP_INTEGRATION_TESTS"); ok {
		t.Skip(reason)
	}

	// 15-minute budget covers: container boot (≤30s) + 6 phases ×
	// ~10s of drain/swap on the worker = ~90s of CP work, plus the
	// Rust half's cargo build (cold-cache CI can take 2-3 minutes
	// after rust-cache restores) and ~15s of test runtime. The
	// orchestrator's RUST_DONE_WAIT (default 6m) is the load-bearing
	// budget for the wait on rust-done — we need this ctx alive at
	// least that long, otherwise NATS tears down while the Rust
	// half is mid-flight.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// --- 1. Boot testcontainers Postgres (or use the CI service when
	//     DATABASE_HOST is set) + testcontainers NATS (JetStream) ---
	//
	// The CI `rollback-e2e` job provides a system Postgres service
	// (matching `go-test-integration`'s shape) because testcontainers
	// Postgres occasionally races the runner's Docker daemon on cold
	// runners (`host port waiting failed`). When DATABASE_HOST is
	// set, we connect to the service directly; otherwise we spin up
	// a testcontainer for hermetic local runs.
	pgC := newE2EPostgres(t, ctx)
	if pgC != nil {
		t.Cleanup(func() {
			cctx, c := context.WithTimeout(context.Background(), 30*time.Second)
			defer c()
			_ = pgC.Terminate(cctx)
		})
	}

	natsC := newE2ENATS(t, ctx)
	t.Cleanup(func() {
		cctx, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = natsC.Terminate(cctx)
	})

	natsURL, err := natsC.ConnectionString(ctx)
	require.NoError(t, err)

	// --- 2. Connect, run migrations, ensure the JetStream stream ---
	db := newE2EDB(t, ctx, pgC)
	t.Cleanup(func() { _ = db.Close() })

	src := e2eMigrationsDir(t)
	_, err = migrate.Exec(db.DB, "postgres", &migrate.FileMigrationSource{Dir: src}, migrate.Up)
	require.NoError(t, err)

	publisher, err := natsctl.NewNATSPublisher(natsURL)
	require.NoError(t, err)
	t.Cleanup(func() { publisher.Conn().Close() })

	// OutboxDrainer calls js.Publish but does NOT ensure the stream
	// exists — the wire contract requires edgecloud-tasks with subjects
	// edgecloud.tasks.>. Ensure it here so the first Tick can publish.
	err = publisher.EnsureStream(natsctl.StreamConfig{
		Name:     natsctl.TaskStreamName,
		Subjects: []string{"edgecloud.tasks.>"},
	})
	require.NoError(t, err)

	// --- 3. Seed tenant, deployments, quota ---
	keyring := e2eNewKeyring(t)
	sha := e2eArtifactHash(t)
	insertE2ETenant(t, ctx, db, testTenantID)
	insertE2EQuota(t, ctx, db, testTenantID)
	sigA := e2eSign(t, keyring, sha, deploymentIDA)
	sigB := e2eSign(t, keyring, sha, deploymentIDB)
	insertE2EDeployment(t, ctx, db, deploymentIDA, testTenantID, testAppName, sha, sigA)
	insertE2EDeployment(t, ctx, db, deploymentIDB, testTenantID, testAppName, sha, sigB)

	// Diagnostic: confirm the rows are visible to the same *sqlx.DB
	// that ActivateDeployment will query. Insertions can succeed
	// against a different connection / different database than the
	// SELECT runs on if the test's DB wiring is split (e.g., pgC
	// returned a working container but newE2EDB connected to the CI
	// service postgres). Logging the count + ids surfaces this in CI.
	var count int
	require.NoError(t, db.GetContext(ctx, &count,
		`SELECT COUNT(*) FROM deployments WHERE id IN ($1, $2)`,
		deploymentIDA, deploymentIDB))
	require.Equal(t, 2, count, "expected 2 deployments rows after insert; got %d", count)
	t.Logf("e2e: verified %d deployment rows present (ids=%s, %s)",
		count, deploymentIDA, deploymentIDB)

	// --- 4. Wire DeploymentService + OutboxDrainer against real pub ---
	deploymentRepo := repository.NewDeploymentRepository(db)
	// Diagnostic: run the exact GetByID call that ActivateDeployment
	// will run, with full error logging. If the row IS visible via
	// raw SELECT but NOT via GetByID, the diff is a scan error
	// (NULL → typed field mismatch) that the test currently hides
	// behind `if err != nil || deployment == nil { return
	// "deployment not found" }`.
	if d, err := deploymentRepo.GetByID(ctx, deploymentIDA); err != nil || d == nil {
		var rawCount int
		_ = db.GetContext(ctx, &rawCount,
			`SELECT COUNT(*) FROM deployments WHERE id = $1`, deploymentIDA)
		var typedCount int
		_ = db.GetContext(ctx, &typedCount,
			`SELECT COUNT(*) FROM deployments WHERE id = $1 AND tenant_id = $2 AND app_name = $3`,
			deploymentIDA, testTenantID, testAppName)
		t.Logf("e2e: diagnostic GetByID err=%v d=%v raw_count=%d typed_count=%d tenant=%s app=%s",
			err, d, rawCount, typedCount, testTenantID, testAppName)
	} else {
		t.Logf("e2e: diagnostic GetByID succeeded id=%s tenant=%s app=%s",
			d.ID, d.TenantID, d.AppName)
	}
	deploymentSvc := service.NewDeploymentService(
		db,
		repository.NewDeploymentRepository(db),
		repository.NewActiveDeploymentRepository(db),
		repository.NewAppEnvRepository(db),
		repository.NewQuotaRepository(db),
		repository.NewMemoryQuotaRepository,
		repository.NewTenantRepository(db),
		repository.NewOutboxRepository(db),
		nil, // artifactStore — unused for activate/rollback (in-tx path doesn't touch it)
		publisher,
		testRegion,
		keyring,
	)
	// Real outbox drainer ticking every 100ms so each phase is observable
	// within ~1s of the writer's commit.
	drainer := service.NewOutboxDrainer(
		repository.NewOutboxRepository(db),
		publisher,
		100*time.Millisecond,
		50,
		10,
	)

	// --- 5. Hand the NATS URL to the Rust half, then wait for rust-ready ---
	writeSentinel(t, "nats-url", natsURL)
	t.Logf("wrote nats-url=%s; waiting for rust-ready", natsURL)
	// 10 minutes covers cold-cache cargo build (3-4m on a fresh
	// runner with rust-cache miss) + test setup + a generous margin.
	// The orchestrator's RUST_DONE_WAIT (default 6m) sits below
	// this so a Rust half crash surfaces as "rust-done timeout"
	// rather than this fatalling first.
	waitForSentinel(t, "rust-ready", 10*time.Minute)

	// --- 6. Phase 1 — activate d_a ---
	// The fourth parameter is the idempotency key. We pass "" for all
	// three phases; the production handler (issue #439 / PR #603)
	// treats "" as "no cache lookup" so reusing it across calls is
	// safe today. If a future change (e.g. #636) starts caching
	// empty-string keys, swap each "" for a fresh uuid.New().String().
	require.NoError(t, deploymentSvc.ActivateDeployment(ctx, testTenantID, testAppName, deploymentIDA, ""))
	drainUntilStable(t, drainer, ctx, 2*time.Second)
	t.Logf("phase 1 (activate %s) drained", deploymentIDA)

	// --- 7. Phase 2 — activate d_b (captures d_a as last_good) ---
	require.NoError(t, deploymentSvc.ActivateDeployment(ctx, testTenantID, testAppName, deploymentIDB, ""))
	drainUntilStable(t, drainer, ctx, 2*time.Second)
	t.Logf("phase 2 (activate %s) drained; last_good should now be %s", deploymentIDB, deploymentIDA)

	// --- 8. Phase 3 — rollback (swaps active back to d_a) ---
	rolledBackID, err := deploymentSvc.RollbackDeployment(ctx, testTenantID, testAppName, "")
	require.NoError(t, err)
	require.Equal(t, deploymentIDA, rolledBackID, "RollbackDeployment should return the rolled-back-to id")
	drainUntilStable(t, drainer, ctx, 2*time.Second)
	t.Logf("phase 3 (rollback to %s) drained", rolledBackID)

	// --- 9. Wait for the Rust half's assertions to finish ---
	// 5 minutes covers the Rust half's cargo build (cold-cache CI
	// can take 2-3 minutes after rust-cache restores) plus ~15s of
	// actual test runtime. The orchestrator's RUST_DONE_WAIT
	// (default 6m) is the outer ceiling.
	waitForSentinel(t, "rust-done", 5*time.Minute)
	t.Logf("rust-done received; tearing down")
}

// --- sentinel helpers ---

func writeSentinel(t *testing.T, name, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(sentinelDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sentinelDir, name), []byte(content), 0o644))
}

func waitForSentinel(t *testing.T, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	path := filepath.Join(sentinelDir, name)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waitForSentinel(%s): timed out after %v", name, timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// drainUntilStable ticks the drainer until no more due rows exist or
// the deadline elapses. A Tick is one-shot; we loop because a Tick in
// flight at the deadline could leave one row unprocessed. Using 100ms
// drain interval + 2s budget gives ~20 ticks of headroom.
func drainUntilStable(t *testing.T, d *service.OutboxDrainer, ctx context.Context, max time.Duration) {
	t.Helper()
	deadline := time.Now().Add(max)
	for {
		d.Tick(ctx)
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// --- container helpers (mirror migrations/roundtrip_test.go shape) ---

// newE2EPostgres returns a testcontainer-managed Postgres when
// DATABASE_HOST is unset (hermetic local runs) and nil when the
// CI service postgres is reachable via env vars. Callers MUST check
// for nil before calling `Terminate`.
//
// We don't actually return the *sqlx.DB here because `pgC` is bound
// to a local var, and we want the env-fallback path to short-circuit
// before any testcontainer call. The connection string is computed
// once by `newE2EDB` below.
func newE2EPostgres(t *testing.T, ctx context.Context) *tcpg.PostgresContainer {
	t.Helper()
	if os.Getenv("DATABASE_HOST") != "" {
		t.Logf("newE2EPostgres: using CI service postgres at %s", os.Getenv("DATABASE_HOST"))
		return nil
	}
	c, err := tcpg.Run(ctx,
		"postgres:16-alpine",
		tcpg.WithDatabase("edgecloud_test"),
		tcpg.WithUsername("test"),
		tcpg.WithPassword("test"),
		tcpg.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	return c
}

func newE2ENATS(t *testing.T, ctx context.Context) *tcnats.NATSContainer {
	t.Helper()
	// Default `RunContainer` already sets `-js` (testcontainers-go
	// v0.43.0/modules/nats/nats.go default Cmd). Don't pass
	// `WithArgument("-js", "true")` — it assembles
	// `nats-server --js true` and NATS treats `true` as an unknown
	// positional, prints help, and exits with code 0.
	c, err := tcnats.RunContainer(ctx,
		tc.WithImage("nats:2.10-alpine"),
	)
	require.NoError(t, err)
	return c
}

func newE2EDB(t *testing.T, ctx context.Context, pgC *tcpg.PostgresContainer) *sqlx.DB {
	t.Helper()
	// Service-postgres path: assemble the DSN from the CI env vars.
	// Mirrors what go-test-integration uses for its service block.
	if pgC == nil {
		host := os.Getenv("DATABASE_HOST")
		require.NotEmpty(t, host, "DATABASE_HOST must be set when no Postgres container")
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
		connStr := fmt.Sprintf(
			"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
			host, port, user, password, name, sslmode,
		)
		db, err := repository.NewDB(connStr)
		require.NoError(t, err)
		return db
	}
	connStr, err := pgC.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := repository.NewDB(connStr)
	require.NoError(t, err)
	return db
}

func e2eMigrationsDir(t *testing.T) string {
	t.Helper()
	_, here, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller failed")
	// this file lives at internal/integration/ — migrations are two
	// directories up + migrations/.
	return filepath.Join(filepath.Dir(here), "..", "..", "migrations")
}

// --- keyring + signing helpers ---

// e2eNewKeyring generates a fresh Ed25519 keypair, wraps it in a
// Signer, then a single-entry Keyring. Mirrors how the production
// signer is loaded (signing.LoadFromRaw + signing.KeyringFromSigner)
// but avoids touching env vars — the test is hermetic.
func e2eNewKeyring(t *testing.T) *sigpkg.Keyring {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_ = pub
	signer, err := sigpkg.LoadFromRaw(priv, testKeyID)
	require.NoError(t, err)
	return sigpkg.KeyringFromSigner(signer, testKeyID)
}

// e2eArtifactHash returns the SHA-256 hex of fake artifact bytes. The
// actual bytes don't matter — the worker's verifier validates the
// signature against this hash + the deployment_id, and the Go half
// signs the same (hash, deployment_id) pair. The Rust half's wiremock
// returns arbitrary bytes; the worker reads the deployment row's
// hash + signature and verifies against the actual downloaded bytes.
func e2eArtifactHash(t *testing.T) string {
	t.Helper()
	h := sha256.Sum256([]byte(testArtifact))
	return hex.EncodeToString(h[:])
}

func e2eSign(t *testing.T, k *sigpkg.Keyring, hashHex, deploymentID string) string {
	t.Helper()
	sig, kid, err := k.Sign(hashHex, deploymentID)
	require.NoError(t, err)
	require.Equal(t, testKeyID, kid)
	return sig
}

// --- DB seed helpers ---

func insertE2ETenant(t *testing.T, ctx context.Context, db *sqlx.DB, tenantID string) {
	t.Helper()
	_, err := db.ExecContext(ctx, `
		INSERT INTO tenants (id, name, plan)
		VALUES ($1, $2, 'free')
		ON CONFLICT (id) DO NOTHING
	`, tenantID, "Rollback E2E Tenant")
	require.NoError(t, err)
}

func insertE2EQuota(t *testing.T, ctx context.Context, db *sqlx.DB, tenantID string) {
	t.Helper()
	_, err := db.ExecContext(ctx, `
		INSERT INTO quotas (
			tenant_id, max_deployments, max_apps, max_workers,
			max_memory_mb, max_outbound_mb
		)
		VALUES ($1, 100, 50, 10, 512, 1024)
		ON CONFLICT (tenant_id) DO NOTHING
	`, tenantID)
	require.NoError(t, err)
}

func insertE2EDeployment(
	t *testing.T, ctx context.Context, db *sqlx.DB,
	id, tenantID, appName, hash, signature string,
) {
	t.Helper()
	// Schema columns: signature + signing_key_id (017), auto_rollback_enabled (009),
	// regions (008). Everything else defaults.
	_, err := db.ExecContext(ctx, `
		INSERT INTO deployments (
			id, tenant_id, app_name, status, hash,
			signature, signing_key_id, auto_rollback_enabled, regions
		)
		VALUES (
			$1, $2, $3, 'deployed', $4,
			$5, $6, false, ARRAY[$7]
		)
	`,
		id, tenantID, appName, hash,
		signature, testKeyID, testRegion,
	)
	require.NoError(t, err)
}
