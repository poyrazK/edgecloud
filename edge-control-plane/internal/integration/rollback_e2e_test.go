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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// --- 1. Boot testcontainers Postgres + NATS (JetStream) ---
	pgC := newE2EPostgres(t, ctx)
	t.Cleanup(func() {
		cctx, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = pgC.Terminate(cctx)
	})

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

	// --- 4. Wire DeploymentService + OutboxDrainer against real pub ---
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
	waitForSentinel(t, "rust-ready", 2*time.Minute)

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
	waitForSentinel(t, "rust-done", 90*time.Second)
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

func newE2EPostgres(t *testing.T, ctx context.Context) *tcpg.PostgresContainer {
	t.Helper()
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
	c, err := tcnats.RunContainer(ctx,
		tc.WithImage("nats:2.10-alpine"),
		// Without JetStream, js.Publish in publisher.go returns
		// "nats: JetStream not enabled" — Activate succeeds (the outbox
		// row lands in Postgres), but the drainer tick that publishes
		// to NATS fails. The -js flag enables JetStream on the server.
		tcnats.WithArgument("-js", "true"),
	)
	require.NoError(t, err)
	return c
}

func newE2EDB(t *testing.T, ctx context.Context, pgC *tcpg.PostgresContainer) *sqlx.DB {
	t.Helper()
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
