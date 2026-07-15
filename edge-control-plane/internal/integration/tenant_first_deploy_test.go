//go:build integration
// +build integration

// Fresh-tenant → first-deploy → outbox fan-out → running-app heartbeat
// → POST /api/internal/tokens/tenant (403 → 200) end-to-end smoke test
// for issue #646.
//
// This test is the regression pin for issue #642 ("Worker JWT
// cold-boot window"). It exercises a brand-new tenant against a
// brand-new worker — the path from `POST /api/v1/tenants` through
// owner-key issuance, free-tier quota, `POST /api/v1/deploy/{appName}`,
// `POST /api/v1/apps/{appName}/activate/{id}`, the outbox fan-out via
// the real NATS publisher + real OutboxDrainer, a fake worker
// receiving the task_update, a running-app heartbeat, and finally a
// `POST /api/internal/tokens/tenant` call that MUST transition from
// 403 (cold boot) to 200 (after the running-app heartbeat).
//
// The cold-boot 403 and the post-heartbeat 200 are both asserted, plus
// an intermediate 403 after a capacity-only heartbeat to pin the seam:
// "any heartbeat is not enough — only a running-app heartbeat unblocks
// the hosting gate" (issue #491 constraint #2).
//
// This test does NOT fix issue #642. It exposes the seam so any future
// fix has a green path. The prerequisite for the 200 to land is
// issue #713 (a shared *middleware.WorkerKeyCache between the internal
// handler and the route-middleware workerAuth — see app.go around
// `workerKeyCache := middleware.NewWorkerKeyCache(workerRepo.GetPublicKey)`).
//
// The fake worker is a Go-level fixture, not a real Rust `edge-worker`
// subprocess. It directly seeds the `workers` row + the enrolled
// public_key (skipping the issue #712/#714 bootstrap handshake),
// publishes hand-crafted HeartbeatMessages over NATS, and listens on
// edgecloud.tasks.<region> for the task_update. The Rust enrollment
// path is a separate #712 follow-up.
//
// Run locally:
//
//	cd edge-control-plane
//	go test -tags=integration -v -count=1 -run TestTenantFirstDeploy ./internal/integration/...
//
// Or via the smoke-test driver (no Rust half required):
//
//	bash scripts/dev-first-deploy-smoke.sh
package integration_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/app"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/config"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	natsctl "github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/signing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
	"github.com/golang-jwt/jwt/v5"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/lib/pq"
	migrate "github.com/rubenv/sql-migrate"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"

	natsio "github.com/nats-io/nats.go"
)

// emptyFS is a zero-value embed.FS — app.New requires one, but the
// smoke test never serves /docs/, so an empty FS is correct.
var emptyFS embed.FS

// freshFirstDeployTestConstants are local to this file; the cross-test
// e2eJWTSecret / e2eInternalToken from worker_cp_e2e_test.go live in a
// different package (handler_test) and aren't directly importable.
// Reusing the same strings (32-byte HS256 secret; the X-Internal-Token
// shared secret) keeps the wire shape identical to the other e2e
// tests so the test never diverges from the production wiring.
const (
	firstDeployJWTSecret     = "this-is-a-32-byte-test-secret-x!"
	firstDeployInternalToken = "test-internal-token"
	firstDeployBootstrapSec  = "test-bootstrap-secret-that-is-long-enough-32!"
	firstDeployIssuer        = "edgecloud"
	firstDeployRegion        = "fra"
	firstDeployWorkerID      = "w_fra_first_deploy"
	firstDeployAppName       = "first-deploy-app"
)

// TestTenantFirstDeploy runs the full issue #646 smoke scenario end
// to end. Linear flow — no t.Run subtests — because the scenario is
// one ordered sequence and require.Eventually already covers the
// polling primitives we need.
func TestTenantFirstDeploy(t *testing.T) {
	if reason, ok := skipIfNoIntegrationEnv(); ok {
		t.Skip(reason)
	}

	// 120s budget covers: container boot (≤30s cold) + first heartbeat
	// round-trip + activation + drainer tick + running heartbeat +
	// hosting-gate transition + JWT verify. Generous margin under the
	// 15-min budget the rollback_e2e test uses.
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	t.Log("bootstrapping postgres testcontainer")
	pgC := newFirstDeployPostgres(t, ctx)
	if pgC != nil {
		t.Cleanup(func() {
			cctx, c := context.WithTimeout(context.Background(), 30*time.Second)
			defer c()
			_ = pgC.Terminate(cctx)
		})
	}

	t.Log("starting nats testcontainer")
	natsC := newFirstDeployNATS(t, ctx)
	t.Cleanup(func() {
		cctx, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = natsC.Terminate(cctx)
	})
	natsURL, err := natsC.ConnectionString(ctx)
	require.NoError(t, err)

	t.Log("applying migrations")
	db := newFirstDeployDB(t, ctx, pgC)
	t.Cleanup(func() { _ = db.Close() })

	src := firstDeployMigrationsDir(t)
	_, err = migrate.Exec(db.DB, "postgres", &migrate.FileMigrationSource{Dir: src}, migrate.Up)
	require.NoError(t, err)

	t.Log("ensuring task stream")
	publisher, err := natsctl.NewNATSPublisher(natsURL)
	require.NoError(t, err)
	t.Cleanup(func() { publisher.Conn().Close() })
	require.NoError(t, publisher.EnsureStream(natsctl.StreamConfig{
		Name:     natsctl.TaskStreamName,
		Subjects: []string{"edgecloud.tasks.>"},
	}))

	t.Log("building control plane")
	srv, artifactStore, workerRepo, appObj := newFirstDeployCP(t, db, publisher)
	t.Cleanup(srv.Close)

	// Subscribe the heartbeats BEFORE anything else publishes — the
	// CP starts its own subscriber when SubscribeHeartbeats runs, but
	// the order here doesn't matter: ChanSubscribe is idempotent on
	// the broker side; both subscribers get every message.
	nc, err := natsio.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(func() { nc.Close() })

	t.Log("starting heartbeat subscriber (CP-side)")
	hbDone := make(chan struct{})
	hbCtx, hbCancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		hbCancel()
		<-hbDone
	})
	require.NoError(t, appObj.WorkerSvc.SubscribeHeartbeats(hbCtx))
	go func() { <-hbCtx.Done(); close(hbDone) }()

	// Step 3 — provision a fresh tenant.
	t.Log("bootstrapping tenant")
	tenantID, rawAPIKey := firstDeployNewTenant(t, srv.URL)

	// Step 4 — DB-level assertions on the fresh-tenant rows.
	t.Log("fresh-tenant rows asserted")
	firstDeployAssertFreshTenantRows(t, ctx, db, tenantID, rawAPIKey, srv.URL)

	// Step 5 — fake worker: insert row + enrolled Ed25519 public key.
	// No worker_status row yet — that IS the cold-boot state.
	t.Log("inserting fake worker")
	pubKey, workerJWT := firstDeployNewFakeWorker(t, ctx, db, workerRepo)

	// Step 6 — first POST /api/internal/tokens/tenant → 403 cold boot.
	t.Log("tenant token (cold) -> checking")
	status, _ := firstDeployPostTenantToken(t, srv.URL, workerJWT, tenantID)
	require.Equal(t, http.StatusForbidden, status,
		"first POST /tokens/tenant before any heartbeat must 403 (cold-boot gate)")

	// Step 7 — publish empty-app capacity heartbeat so worker_status
	// materializes with positive free_slots.
	t.Log("publishing capacity heartbeat")
	firstDeployPublishHeartbeat(t, nc, firstDeployWorkerID, firstDeployRegion, map[string]domain.AppStatus{
		// empty apps map — capacity-only signal.
	}, 4)
	firstDeployEventuallyWorkerStatus(t, ctx, db, firstDeployWorkerID, func(ws domain.WorkerStatus) bool {
		return ws.FreeSlots > 0
	})

	// Step 8 — second POST /tokens/tenant → STILL 403. Capacity-only
	// heartbeat unblocks SumFreeSlotsByRegion but does NOT add the
	// tenant to worker_status.apps with status='running'. The
	// hosting-gate (issue #491 constraint #2) keeps the 403.
	t.Log("tenant token (capacity-only) -> checking")
	status, _ = firstDeployPostTenantToken(t, srv.URL, workerJWT, tenantID)
	require.Equal(t, http.StatusForbidden, status,
		"second POST /tokens/tenant after capacity-only heartbeat must STILL 403 — capacity-only heartbeat doesn't add an app to worker_status.apps")

	// Step 9 — subscribe the fake worker to edgecloud.tasks.fra
	// BEFORE we trigger the activation, so the publish doesn't race
	// the subscription.
	t.Log("subscribing fake worker to edgecloud.tasks.fra")
	taskCh := firstDeploySubscribeWorkerTasks(t, nc, firstDeployRegion)

	// Step 10 — deploy. Use the tenant's owner API key as the bearer.
	t.Log("deploying empty wasm")
	wasmBytes := freshWasmBytes()
	deploymentID, deployStatus := firstDeployPostDeploy(t, srv.URL, rawAPIKey, firstDeployAppName, wasmBytes)
	require.Equal(t, http.StatusCreated, deployStatus, "deploy must 201")
	require.NotEmpty(t, deploymentID)

	// Step 11 — post-deploy DB assertions. Apps row exists, deployments
	// row has signature + signing_key_id, artifact on disk. No
	// active_deployments row, no outbox row yet.
	t.Log("first-deploy rows asserted")
	firstDeployAssertFirstDeployRows(t, ctx, db, artifactStore, tenantID, firstDeployAppName, deploymentID, wasmBytes)

	// Step 12 — activate. Issue an Idempotency-Key per the #439/#603
	// pattern.
	t.Log("activating")
	actStatus, _ := firstDeployPostActivate(t, srv.URL, rawAPIKey, firstDeployAppName, deploymentID)
	require.Equal(t, http.StatusOK, actStatus, "activate must 200")

	// Step 13 — activation committed. One active_deployments row + one
	// pending task_update outbox row. Drainer is NOT started yet, so
	// the snapshot is deterministic.
	t.Log("activation committed rows asserted")
	firstDeployAssertActivationCommitted(t, ctx, db, tenantID, firstDeployAppName, deploymentID)

	// Step 14 — start the outbox drainer. From this point onward the
	// pending row will flip to `published` within ~1s. The drainer
	// has no Stop method — cancellation breaks its loop. We don't
	// need a reference to the returned drainer here; the goroutine
	// runs for the test's lifetime.
	t.Log("starting outbox drainer")
	_ = firstDeployStartDrainer(t, ctx, db, publisher)

	// Step 15 — wait for the fake worker to receive task_update.
	t.Log("waiting for fake worker to receive task_update")
	gotMsg := firstDeployReceiveWorkerTask(t, taskCh, 10*time.Second)
	require.Equal(t, natsctl.TaskMessageKindTaskUpdate, gotMsg.Type,
		"received message must be type=task_update")
	require.Equal(t, tenantID, gotMsg.TenantID)
	require.Contains(t, gotMsg.Apps, firstDeployAppName,
		"task_update must carry the deployed app")
	appCfg := gotMsg.Apps[firstDeployAppName]
	require.Equal(t, deploymentID, appCfg.DeploymentID)
	require.NotEmpty(t, appCfg.DeploymentSignature, "task_update app config must carry the Ed25519 signature")

	// Step 16 — outbox row flipped to published.
	t.Log("outbox row published")
	firstDeployEventuallyOutboxPublished(t, ctx, db, deploymentID)

	// Step 17 — publish a running heartbeat for the deployed app.
	t.Log("publishing running heartbeat")
	firstDeployPublishHeartbeat(t, nc, firstDeployWorkerID, firstDeployRegion, map[string]domain.AppStatus{
		firstDeployAppName: {
			Status:       "running",
			TenantID:     tenantID,
			DeploymentID: deploymentID,
			Port:         8080,
		},
	}, 3)
	firstDeployEventuallyWorkerStatus(t, ctx, db, firstDeployWorkerID, func(ws domain.WorkerStatus) bool {
		var apps map[string]domain.AppStatus
		if len(ws.Apps) == 0 {
			return false
		}
		if err := json.Unmarshal(ws.Apps, &apps); err != nil {
			return false
		}
		a, ok := apps[firstDeployAppName]
		return ok && a.Status == "running" && a.DeploymentID == deploymentID && a.TenantID == tenantID
	})

	// Step 19 — third POST /tokens/tenant → 200. The hosting gate is
	// now satisfied: worker_status.apps[<app>].status == "running"
	// for the target tenant.
	t.Log("tenant token (running) -> checking")
	status, body := firstDeployPostTenantToken(t, srv.URL, workerJWT, tenantID)
	require.Equal(t, http.StatusOK, status,
		"third POST /tokens/tenant after running heartbeat must 200; body=%s", string(body))

	var resp struct {
		Token     string `json:"token"`
		ExpiresAt int64  `json:"expires_at"`
		TenantID  string `json:"tenant_id"`
	}
	require.NoError(t, json.Unmarshal(body, &resp))
	require.NotEmpty(t, resp.Token, "response must carry a token")
	require.Equal(t, tenantID, resp.TenantID)
	require.Greater(t, resp.ExpiresAt, time.Now().Unix(),
		"expires_at must be in the future")

	// Verify the token under the production WorkerJWTConfig. The
	// token's kid must be in the wkr_ namespace, and the secret it
	// verifies under must be the HKDF-derived per-worker secret.
	t.Log("verifying minted token claims + signature")
	parsed, err := jwt.ParseWithClaims(resp.Token, &middleware.WorkerClaims{}, func(tok *jwt.Token) (interface{}, error) {
		// Sign the token with the per-worker HKDF-derived secret.
		// Re-derive it the same way the production keyring does.
		kid, _ := tok.Header["kid"].(string)
		require.True(t, signing.IsWorkerKID(kid),
			"minted token kid %q must be in the wkr_ namespace", kid)
		return signing.DeriveWorkerSecret([]byte(firstDeployJWTSecret), firstDeployWorkerID, tenantID, firstDeployRegion, pubKey)
	})
	require.NoError(t, err, "token must verify under per-worker HKDF secret")
	require.True(t, parsed.Valid, "token must be valid")
	claims, ok := parsed.Claims.(*middleware.WorkerClaims)
	require.True(t, ok)
	require.Equal(t, firstDeployWorkerID, claims.WorkerID)
	require.Equal(t, tenantID, claims.TenantID)
	require.Equal(t, firstDeployRegion, claims.Region)

	t.Logf("PASS — kid=%v claims.WorkerID=%v claims.TenantID=%v", claims.WorkerID, claims.WorkerID, claims.TenantID)
}

// ── helpers ──────────────────────────────────────────────────────────

// skipIfNoIntegrationEnv mirrors testutil.ShouldSkipIntegration but is
// inlined so the file has no cross-package testutil coupling. Returns
// (reason, true) when the test should skip.
func skipIfNoIntegrationEnv() (string, bool) {
	if os.Getenv("SKIP_INTEGRATION_TESTS") != "" {
		return "SKIP_INTEGRATION_TESTS set", true
	}
	if _, err := os.Stat("/var/run/docker.sock"); err != nil {
		return "docker socket unavailable; integration tests require Docker", true
	}
	return "", false
}

// freshWasmBytes returns the canonical Wasm magic + a deterministic
// version byte + a small body. Real Wasmtime doesn't load this — the
// smoke test never instantiates the component. We just need bytes that
// pass `MaxArtifactSize` and hash deterministically for the
// post-deploy assertion.
func freshWasmBytes() []byte {
	out := make([]byte, 256)
	copy(out[0:8], []byte{0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00}) // \0asm magic + version 1
	for i := 8; i < len(out); i++ {
		out[i] = byte(i)
	}
	return out
}

// newFirstDeployPostgres boots a testcontainer-managed Postgres
// (postgres:16-alpine) unless DATABASE_HOST is set, in which case we
// use the CI service — mirroring migrations/roundtrip_test.go's shape
// and rollback_e2e_test.go's helper. Returns nil when the CI service
// is in use; callers MUST check for nil before calling Terminate.
func newFirstDeployPostgres(t *testing.T, ctx context.Context) *tcpg.PostgresContainer {
	t.Helper()
	if os.Getenv("DATABASE_HOST") != "" {
		t.Logf("using CI service postgres at %s", os.Getenv("DATABASE_HOST"))
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

// newFirstDeployNATS boots testcontainers NATS (JetStream on by
// default in testcontainers-go modules/nats RunContainer — do NOT
// pass WithArgument("-js","true")).
func newFirstDeployNATS(t *testing.T, ctx context.Context) *tcnats.NATSContainer {
	t.Helper()
	c, err := tcnats.RunContainer(ctx, tc.WithImage("nats:2.10-alpine"))
	require.NoError(t, err)
	return c
}

// newFirstDeployDB connects via the production repository.NewDB helper
// so the test exercises the same MaxOpenConns/MaxIdleConns/
// ConnMaxLifetime config as the API server.
func newFirstDeployDB(t *testing.T, ctx context.Context, pgC *tcpg.PostgresContainer) *sqlx.DB {
	t.Helper()
	var connStr string
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
		connStr = fmt.Sprintf(
			"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
			host, port, user, password, name, sslmode,
		)
	} else {
		var err error
		connStr, err = pgC.ConnectionString(ctx, "sslmode=disable")
		require.NoError(t, err)
	}
	db, err := repository.NewDB(connStr)
	require.NoError(t, err)
	return db
}

// firstDeployMigrationsDir resolves the migrations directory from this
// file's own location via runtime.Caller(0) — works regardless of the
// runner's working directory.
func firstDeployMigrationsDir(t *testing.T) string {
	t.Helper()
	_, here, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller failed")
	// this file lives at internal/integration/; migrations are two
	// directories up.
	return filepath.Clean(filepath.Join(filepath.Dir(here), "..", "..", "migrations"))
}

// newFirstDeployCP boots a fresh httptest.Server backed by the
// production app.New composition root. Wires the production NATS
// publisher so the outbox drainer can deliver task_update messages on
// activation. Returns the server, the artifact store (for on-disk
// post-deploy assertions), the worker repo (for fake-worker pubkey
// persistence), and the *App (so the test can call SubscribeHeartbeats).
func newFirstDeployCP(
	t *testing.T,
	db *sqlx.DB,
	publisher *natsctl.NATSPublisher,
) (*httptest.Server, storage.ArtifactStore, *repository.WorkerRepository, *app.App) {
	t.Helper()

	// The signing keyring uses the all-zero 32-byte seed — same as
	// worker_cp_e2e_test.go's `signing.TestKeyring(t)` so the
	// production keyring (loaded from the same env) signs with the
	// same Ed25519 key. The row's signing_key_id will be
	// signing.TestKeyID.
	keyPath := filepath.Join(t.TempDir(), "test_signing.key")
	require.NoError(t, os.WriteFile(keyPath, make([]byte, 32), 0o600))
	keyBytes, err := os.ReadFile(keyPath)
	require.NoError(t, err)
	keyHexMaterial := hex.EncodeToString(keyBytes)

	artifactPath := t.TempDir()
	cfg := &config.Config{
		Database: config.DatabaseConfig{
			Host: "ignored", Port: 5432, User: "test", Password: "test", Name: "test", SSLMode: "disable",
		},
		NATS: config.NATSConfig{URL: "nats://localhost:4222"},
		App:  config.AppConfig{Host: "0.0.0.0", Port: 8080, Env: "test"},
		JWT: config.JWTConfig{
			Secret: firstDeployJWTSecret,
			Issuer: firstDeployIssuer,
			TTL:    24,
		},
		Region:          firstDeployRegion,
		BootstrapSecret: firstDeployBootstrapSec,
		InternalToken:   firstDeployInternalToken,
		Storage: config.StorageConfig{
			ArtifactBackend: "fs",
			ArtifactPath:    artifactPath,
		},
		Signing: config.SigningConfig{
			Keyring: "test-k1 = " + keyHexMaterial,
			KeyID:   "test-k1",
		},
	}

	artifactStore := storage.NewFSArtifactStore(artifactPath)
	application := app.New(cfg, db, publisher, artifactStore, emptyFS)
	require.NotNil(t, application)
	require.NotNil(t, application.Handler)

	srv := httptest.NewServer(application.Handler)

	workerRepo := repository.NewWorkerRepository(db)
	return srv, artifactStore, workerRepo, application
}

// firstDeployNewTenant calls POST /api/v1/tenants with a free plan and
// a per-test unique name. Returns (tenant_id, raw_api_key).
func firstDeployNewTenant(t *testing.T, serverURL string) (string, string) {
	t.Helper()

	// Unique name so re-runs against a shared DB don't collide on the
	// tenant name unique index (tenant names are unique per cluster).
	name := fmt.Sprintf("first-deploy-%d", time.Now().UnixNano())
	body, err := json.Marshal(map[string]string{
		"name":     name,
		"plan":     "free",
		"key_name": "smoke-key",
	})
	require.NoError(t, err)

	resp, err := http.Post(serverURL+"/api/v1/tenants",
		"application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode,
		"Bootstrap must 201; body=%s", readBody(resp.Body))

	var out struct {
		TenantID string `json:"tenant_id"`
		APIKey   string `json:"api_key"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.NotEmpty(t, out.TenantID, "BootstrapResponse missing tenant_id")
	require.NotEmpty(t, out.APIKey, "BootstrapResponse missing api_key")
	return out.TenantID, out.APIKey
}

// firstDeployAssertFreshTenantRows is the post-Bootstrap read-side pin
// for issue #646. We assert:
//
//   - tenants row exists, disabled_at IS NULL, allowlisted_destinations
//     is empty (free-tier default).
//   - quotas row matches domain.QuotaForPlan("free") defaults.
//   - api_keys has exactly one row, role='owner', lookup_hash is a
//     non-empty Argon2id hash.
//   - one authenticated round-trip proves the API key verifies end to
//     end against the bearer auth middleware.
func firstDeployAssertFreshTenantRows(
	t *testing.T, ctx context.Context, db *sqlx.DB,
	tenantID, rawAPIKey, serverURL string,
) {
	t.Helper()

	var (
		disabledAt        *time.Time
		allowlistedDest   []string
	)
	require.NoError(t, db.QueryRowxContext(ctx, `
		SELECT disabled_at, allowlisted_destinations
		FROM tenants WHERE id = $1
	`, tenantID).Scan(&disabledAt, pq.Array(&allowlistedDest)))
	require.Nil(t, disabledAt, "fresh tenant must have disabled_at IS NULL")
	require.Empty(t, allowlistedDest,
		"fresh tenant must have empty allowlisted_destinations (free-tier default)")

	// Compare quotas row to the free plan defaults from the single
	// source of truth (domain.QuotaForPlan).
	free, err := domain.QuotaForPlan("free")
	require.NoError(t, err)
	var (
		maxDeployments int
		maxMemoryMB    int
		maxOutboundMB  int
		maxReqPerMonth int
	)
	require.NoError(t, db.QueryRowxContext(ctx, `
		SELECT max_deployments, max_memory_mb, max_outbound_mb, max_requests_per_month
		FROM quotas WHERE tenant_id = $1
	`, tenantID).Scan(&maxDeployments, &maxMemoryMB, &maxOutboundMB, &maxReqPerMonth))
	require.Equal(t, free.MaxDeployments, maxDeployments,
		"free quotas.max_deployments mismatch")
	require.Equal(t, free.MaxMemoryMB, maxMemoryMB,
		"free quotas.max_memory_mb mismatch")
	require.Equal(t, free.MaxOutboundMB, maxOutboundMB,
		"free quotas.max_outbound_mb mismatch")
	require.Equal(t, free.MaxRequestsPerMonth, maxReqPerMonth,
		"free quotas.max_requests_per_month mismatch")

	// api_keys: exactly one row, role=owner, lookup_hash non-empty.
	var (
		role       string
		lookupHash string
	)
	require.NoError(t, db.QueryRowxContext(ctx, `
		SELECT role, COALESCE(lookup_hash, '') FROM api_keys WHERE tenant_id = $1 LIMIT 1
	`, tenantID).Scan(&role, &lookupHash))
	require.Equal(t, "owner", role, "BootstrapTenant must mint an owner-role API key")
	require.NotEmpty(t, lookupHash, "api_keys.lookup_hash must be a non-empty Argon2id hash")

	// One authenticated round-trip — GET /api/v1/apps with the bearer.
	// A 200 (empty list) proves the bearer hash path end-to-end.
	req, err := http.NewRequest(http.MethodGet, serverURL+"/api/v1/apps", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+rawAPIKey)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"owner bearer must 200 on /api/v1/apps; body=%s", readBody(resp.Body))
}

// firstDeployNewFakeWorker generates an Ed25519 keypair, inserts a
// workers row with that public key, and mints a legacy wildcard
// worker JWT (the bootstrap-style JWT a worker would carry before it
// has done any work). Returns the hex-encoded public key + the JWT
// string.
//
// We seed the workers row directly rather than going through the
// bootstrap handshake (issue #712/#714 follow-up) because the smoke
// test is pinning the post-heartbeat 200, not the bootstrap path.
// TODO(#712): route this through the real three-phase handshake once
// it lands.
func firstDeployNewFakeWorker(
	t *testing.T, ctx context.Context, db *sqlx.DB,
	workerRepo *repository.WorkerRepository,
) (string, string) {
	t.Helper()

	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	pubHex := hex.EncodeToString(pub)

	// workers row: tenant_id is NOT NULL FK to tenants — use "*"
	// (wildcard). The workers table allows the wildcard; the
	// production bootstrap inserts tenant_id="*" too.
	_, err = db.ExecContext(ctx, `
		INSERT INTO workers (id, region, tenant_id, memory_mb)
		VALUES ($1, $2, '*', 4096)
		ON CONFLICT (id) DO NOTHING
	`, firstDeployWorkerID, firstDeployRegion)
	require.NoError(t, err)

	// Set the public key via the production repository helper so the
	// loader closure wired into WorkerKeyCache (workerRepo.GetPublicKey)
	// finds it.
	_, err = workerRepo.SetPublicKey(ctx, firstDeployWorkerID, pubHex)
	require.NoError(t, err)

	// Mint a legacy wildcard worker JWT — same shape as the
	// bootstrap-issued JWT a worker would carry on first boot.
	claims := middleware.WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    firstDeployIssuer,
			Subject:   firstDeployWorkerID,
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: firstDeployWorkerID,
		TenantID: "*",
		Region:   firstDeployRegion,
		Role:     middleware.RoleWorker,
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString([]byte(firstDeployJWTSecret))
	require.NoError(t, err)

	return pubHex, signed
}

// firstDeployPublishHeartbeat publishes a single HeartbeatMessage via
// core NATS (NOT JetStream) — heartbeats are not durable. The
// SubscribeHeartbeats path on the CP side consumes them with
// nc.ChanSubscribe("edgecloud.heartbeats.>", ch).
func firstDeployPublishHeartbeat(
	t *testing.T, nc *natsio.Conn, workerID, region string,
	apps map[string]domain.AppStatus, freeSlots uint32,
) {
	t.Helper()

	hb := natsctl.HeartbeatMessage{
		Type:      "heartbeat",
		Timestamp: time.Now(),
		WorkerID:  workerID,
		Region:    region,
		Apps:      apps,
		ClusterHeadroom: &natsctl.ClusterHeadroom{
			AppSlots:  freeSlots,
			FreeSlots: freeSlots,
		},
	}
	payload, err := json.Marshal(hb)
	require.NoError(t, err)
	require.NoError(t, nc.Publish("edgecloud.heartbeats."+region, payload))
	require.NoError(t, nc.Flush())
}

// firstDeployEventuallyWorkerStatus polls worker_status until the
// supplied predicate returns true or 5s elapses.
func firstDeployEventuallyWorkerStatus(
	t *testing.T, ctx context.Context, db *sqlx.DB, workerID string,
	predicate func(ws domain.WorkerStatus) bool,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		var ws domain.WorkerStatus
		err := db.QueryRowxContext(ctx, `
			SELECT worker_id, apps, last_report, free_slots, cluster_headroom,
			       port_pool_exhausted_count, last_exhaustion_at
			FROM worker_status WHERE worker_id = $1
		`, workerID).StructScan(&ws)
		if err != nil {
			return false
		}
		return predicate(ws)
	}, 5*time.Second, 50*time.Millisecond,
		"worker_status predicate not satisfied for worker %s", workerID)
}

// firstDeployPostTenantToken POSTs to /api/internal/tokens/tenant with
// the worker JWT bearer and the supplied tenant_id. Returns
// (status_code, response_body).
func firstDeployPostTenantToken(
	t *testing.T, serverURL, workerJWT, tenantID string,
) (int, []byte) {
	t.Helper()

	body, err := json.Marshal(map[string]string{"tenant_id": tenantID})
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, serverURL+"/api/internal/tokens/tenant", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+workerJWT)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, respBody
}

// firstDeploySubscribeWorkerTasks subscribes to the task subject for
// the supplied region. Returns the message channel; callers receive
// from it directly and decode the *natsctl.TaskMessage themselves.
func firstDeploySubscribeWorkerTasks(
	t *testing.T, nc *natsio.Conn, region string,
) chan *natsio.Msg {
	t.Helper()
	ch := make(chan *natsio.Msg, 16)
	_, err := nc.ChanSubscribe("edgecloud.tasks."+region, ch)
	require.NoError(t, err)
	require.NoError(t, nc.Flush())
	return ch
}

// firstDeployReceiveWorkerTask waits up to `timeout` for a task_update
// message on the channel and returns the decoded TaskMessage.
// Any other message type causes a fatal failure — the only thing the
// drainer publishes on edgecloud.tasks.<region> during activate is a
// task_update for this test.
func firstDeployReceiveWorkerTask(
	t *testing.T, ch chan *natsio.Msg, timeout time.Duration,
) *natsctl.TaskMessage {
	t.Helper()

	deadline := time.After(timeout)
	for {
		select {
		case raw := <-ch:
			var msg natsctl.TaskMessage
			if err := json.Unmarshal(raw.Data, &msg); err != nil {
				t.Fatalf("decode task_update: %v", err)
			}
			return &msg
		case <-deadline:
			t.Fatalf("timeout waiting for task_update on edgecloud.tasks.%s after %v", firstDeployRegion, timeout)
		}
	}
}

// firstDeployPostDeploy POSTs the wasm bytes to
// /api/v1/deploy/{appName} as multipart/form-data. Returns the
// deployment_id from the response.
func firstDeployPostDeploy(
	t *testing.T, serverURL, rawAPIKey, appName string, wasmBytes []byte,
) (string, int) {
	t.Helper()

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", "artifact.wasm")
	require.NoError(t, err)
	_, err = fw.Write(wasmBytes)
	require.NoError(t, err)
	require.NoError(t, mw.Close())

	req, err := http.NewRequest(http.MethodPost,
		serverURL+"/api/v1/deploy/"+appName, &buf)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+rawAPIKey)
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	if resp.StatusCode != http.StatusCreated {
		return "", resp.StatusCode
	}
	var out struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(body, &out))
	require.NotEmpty(t, out.ID, "deploy response missing id; body=%s", string(body))
	return out.ID, resp.StatusCode
}

// firstDeployPostActivate POSTs to /api/v1/apps/{appName}/activate/{deploymentID}
// with an Idempotency-Key header (issue #439 pattern).
func firstDeployPostActivate(
	t *testing.T, serverURL, rawAPIKey, appName, deploymentID string,
) (int, []byte) {
	t.Helper()

	url := serverURL + "/api/v1/apps/" + appName + "/activate/" + deploymentID
	req, err := http.NewRequest(http.MethodPost, url, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+rawAPIKey)
	req.Header.Set("Idempotency-Key", newIdempotencyKey())
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, body
}

// firstDeployAssertFirstDeployRows checks the post-deploy DB state:
// apps row exists, deployments row has signature + signing_key_id,
// artifact is on disk. NO active_deployments, NO outbox row yet.
func firstDeployAssertFirstDeployRows(
	t *testing.T, ctx context.Context, db *sqlx.DB, store storage.ArtifactStore,
	tenantID, appName, deploymentID string, wasmBytes []byte,
) {
	t.Helper()

	// apps row exists.
	var appCount int
	require.NoError(t, db.QueryRowxContext(ctx,
		`SELECT COUNT(*) FROM apps WHERE tenant_id = $1 AND name = $2`,
		tenantID, appName).Scan(&appCount))
	require.Equal(t, 1, appCount, "exactly one apps row must exist post-deploy")

	// deployments row has signature + signing_key_id.
	var (
		sig        string
		signingKID string
		storedHash string
	)
	require.NoError(t, db.QueryRowxContext(ctx, `
		SELECT signature, signing_key_id, hash
		FROM deployments WHERE id = $1 AND tenant_id = $2 AND app_name = $3
	`, deploymentID, tenantID, appName).Scan(&sig, &signingKID, &storedHash))
	require.NotEmpty(t, sig, "deployments.signature must be non-empty post-deploy")
	require.NotEmpty(t, signingKID, "deployments.signing_key_id must be non-empty post-deploy")

	// Re-hash and compare — the row's hash must equal sha256(wasmBytes).
	sum := sha256.Sum256(wasmBytes)
	require.Equal(t, hex.EncodeToString(sum[:]), storedHash,
		"row.hash must equal sha256 of the uploaded wasm")

	// Artifact exists on disk and round-trips.
	rdr, err := store.Open(ctx, tenantID, appName, deploymentID)
	require.NoError(t, err, "artifact must be on disk post-deploy")
	defer rdr.Close()
	got, err := io.ReadAll(rdr)
	require.NoError(t, err)
	require.Equal(t, wasmBytes, got, "on-disk artifact bytes must equal uploaded bytes")

	// No active_deployments row yet.
	var adCount int
	require.NoError(t, db.QueryRowxContext(ctx, `
		SELECT COUNT(*) FROM active_deployments WHERE tenant_id = $1 AND app_name = $2
	`, tenantID, appName).Scan(&adCount))
	require.Zero(t, adCount, "active_deployments row must NOT exist post-deploy (only after activate)")

	// No outbox row yet.
	var outboxCount int
	require.NoError(t, db.QueryRowxContext(ctx, `
		SELECT COUNT(*) FROM outbox WHERE dedupe_key = $1
	`, deploymentID).Scan(&outboxCount))
	require.Zero(t, outboxCount, "outbox row must NOT exist post-deploy (only after activate)")
}

// firstDeployAssertActivationCommitted checks the post-activate DB
// state: one active_deployments row + one PENDING task_update outbox
// row. The drainer is NOT yet started, so this is a deterministic
// snapshot.
func firstDeployAssertActivationCommitted(
	t *testing.T, ctx context.Context, db *sqlx.DB,
	tenantID, appName, deploymentID string,
) {
	t.Helper()

	// active_deployments row.
	var adID string
	require.NoError(t, db.QueryRowxContext(ctx, `
		SELECT deployment_id FROM active_deployments
		WHERE tenant_id = $1 AND app_name = $2
	`, tenantID, appName).Scan(&adID))
	require.Equal(t, deploymentID, adID,
		"active_deployments.deployment_id must equal the just-activated id")

	// Pending outbox row.
	var (
		kind     string
		status   string
		payload  []byte
	)
	require.NoError(t, db.QueryRowxContext(ctx, `
		SELECT kind, status, payload
		FROM outbox WHERE dedupe_key = $1
	`, deploymentID).Scan(&kind, &status, &payload))
	require.Equal(t, "task_update", kind, "outbox row kind must be task_update")
	require.Equal(t, "pending", status,
		"outbox row status must be pending (drainer not yet started)")

	// Decode the payload and assert the wire shape mirrors what the
	// fake worker will receive.
	var msg natsctl.TaskMessage
	require.NoError(t, json.Unmarshal(payload, &msg))
	require.Equal(t, natsctl.TaskMessageKindTaskUpdate, msg.Type)
	require.Equal(t, tenantID, msg.TenantID)
	require.Contains(t, msg.Apps, appName)
	appCfg := msg.Apps[appName]
	require.Equal(t, deploymentID, appCfg.DeploymentID)
	require.NotEmpty(t, appCfg.DeploymentSignature, "outbox payload must carry the Ed25519 signature")
}

// firstDeployStartDrainer wires the production OutboxDrainer at 100ms
// tick (matches rollback_e2e_test.go's posture) and runs it in a
// goroutine bound to the test context. The drainer has no Stop method
// — cancellation breaks the loop. Returns the drainer so callers can
// drive Tick deterministically if needed; the production-style tick
// loop here matches what app.RunBackground would do.
func firstDeployStartDrainer(
	t *testing.T, ctx context.Context, db *sqlx.DB, pub *natsctl.NATSPublisher,
) *service.OutboxDrainer {
	t.Helper()

	d := service.NewOutboxDrainer(repository.NewOutboxRepository(db), pub,
		100*time.Millisecond, 50, 10)
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.Tick(ctx)
			}
		}
	}()
	return d
}

// firstDeployEventuallyOutboxPublished polls the outbox row's status
// until it flips to 'published' or 5s elapses.
func firstDeployEventuallyOutboxPublished(
	t *testing.T, ctx context.Context, db *sqlx.DB, dedupeKey string,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		var status string
		err := db.QueryRowxContext(ctx,
			`SELECT status FROM outbox WHERE dedupe_key = $1`, dedupeKey).Scan(&status)
		if err != nil {
			return false
		}
		return status == "published"
	}, 5*time.Second, 50*time.Millisecond,
		"outbox row for dedupe_key=%s never flipped to published", dedupeKey)
}

// newIdempotencyKey returns a fresh UUID-v4-shaped hex string. The
// Activate handler only checks the [a-fA-F0-9-]{8,128} format, so any
// sufficiently long hex+dasg string works.
func newIdempotencyKey() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// readBody reads and returns the response body bytes for diagnostic
// messages. Truncated to 4 KiB.
func readBody(r io.Reader) string {
	const maxDump = 4 << 10
	b, _ := io.ReadAll(io.LimitReader(r, maxDump))
	// base64-encode to keep binary garbage out of test logs.
	return base64.StdEncoding.EncodeToString(b)
}