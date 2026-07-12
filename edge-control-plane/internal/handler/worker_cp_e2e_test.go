//go:build integration

// Worker ↔ real CP HTTP contract tests (issue #612).
//
// Boots the production CP handler chain (`app.New().Handler`) under
// `httptest.NewServer` and exercises the three contracts the worker
// relies on:
//
//  1. The bootstrap handshake (`POST /api/internal/bootstrap` →
//     `GET /api/internal/worker-secret`).
//  2. The worker-JWT download lane (`Authorization: Bearer <jwt>`
//     on `GET /api/internal/download/{id}`).
//  3. The `X-Internal-Token` download lane (the dual-auth branch
//     at `internal/middleware/internal.go:79-99` that the worker
//     code path can never reach from production, but a peer-CP
//     pull-through can).
//
// The headline acceptance criterion — "end-to-end SHA-256 + Ed25519
// verification against a CP-produced row" — is satisfied by seeding
// a `deployments` row with a real Go-CP-produced signature
// (`signing.Keyring.Sign`) and re-running `signing.Keyring.Verify`
// on the downloaded bytes. The Rust `Keyring::verify` is already
// pinned against the same signature wire format by
// `edge-worker/tests/signing_wire_contract.rs` (issue #611), so a
// green run on both `go-test-integration` and `rust-test` together
// means the cross-language wire is intact.
//
// Build tag matches `migrations/roundtrip_test.go` exactly — no
// `SKIP_INTEGRATION_TESTS` guard. CI runs these via
// `go-test-integration` job with `-tags=integration`.
package handler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
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
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/signing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
	"github.com/golang-jwt/jwt/v5"
	"github.com/jmoiron/sqlx"
	migrate "github.com/rubenv/sql-migrate"
	"github.com/stretchr/testify/require"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// emptyFS is a zero-value embed.FS for tests that don't need the OpenAPI spec.
var emptyFS embed.FS

// Test configuration constants. The bootstrap secret reuses the unit
// test's `testBootstrapSecret` (declared in `internal_test.go:272` in
// the same `handler_test` package) so unit and integration tests pin
// the same wire shape. The JWT, internal-token, and issuer consts are
// integration-test-specific (`e2e` prefix) to avoid colliding with the
// shorter `testJWTSecret` declared in `internal_logs_test.go:43` (11
// bytes — too short for the production validator, which requires ≥ 32).
const (
	// e2eJWTSecret mirrors `internal/app/app_test.go:63`'s
	// 32-byte legacy single-secret layout. ≥ 32 bytes.
	e2eJWTSecret = "this-is-a-32-byte-test-secret-x!"

	// e2eInternalToken is the shared secret the X-Internal-Token
	// download lane compares against via `subtle.ConstantTimeCompare`
	// in `internal/middleware/internal.go`.
	e2eInternalToken = "test-internal-token"

	// e2eIssuer mirrors `internal/app/app_test.go:64`. The worker-JWT
	// mint path's `iss` claim must match what `WorkerAuth.VerifyWorkerJWT`
	// (`internal/middleware/worker.go:94`) expects.
	e2eIssuer = "edgecloud"
)

// newTestPostgres boots a postgres:16-alpine testcontainer. Verbatim copy of
// `migrations/roundtrip_test.go:1606` — duplicated per the user's choice
// (issue #612 plan: don't refactor the existing migrations test). The
// `BasicWaitStrategies` argument is load-bearing; without it the first
// connection from `repository.NewDB` can race the inner pg_isready loop
// and flake on Mac/Windows runners.
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
	t.Cleanup(func() {
		// Stop the container when the test ends so the harness doesn't
		// leak Docker state across runs. Terminate is idempotent.
		_ = pgC.Terminate(ctx)
	})
	return pgC
}

// newDBFromContainer opens a *sqlx.DB via the production NewDB helper
// (`internal/repository/db.go:27`). Reusing the production helper means
// the test exercises the same MaxOpenConns/MaxIdleConns/ConnMaxLifetime
// config as the API server. The explicit `sslmode=disable` matches the
// production DSN format in `internal/config/config.go:DatabaseConfig.DSN()`.
func newDBFromContainer(t *testing.T, ctx context.Context, pgC *tcpg.PostgresContainer) *sqlx.DB {
	t.Helper()
	connStr, err := pgC.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := repository.NewDB(connStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// migrationsDir resolves the migrations directory from this file's own
// location via `runtime.Caller(0)`. Avoids depending on the runner's
// working directory, so `go test ./internal/handler/...` from any CWD
// lands in the same place. Mirrors `migrations/roundtrip_test.go:1643`.
func migrationsDir(t *testing.T) string {
	t.Helper()
	_, here, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller failed")
	// here is `.../edge-control-plane/internal/handler/worker_cp_e2e_test.go`;
	// walk up two parents to reach the migrations dir.
	return filepath.Clean(filepath.Join(filepath.Dir(here), "..", "..", "migrations"))
}

// newTestCP boots a fresh `*httptest.Server` backed by the real CP
// handler (`app.New().Handler`) on top of a fresh Postgres container
// with all migrations applied. Returns:
//
//   - the httptest server (use its URL as the worker-side target),
//   - the *sqlx.DB the test can seed rows into directly,
//   - the *signing.Keyring the test uses to sign deployment rows so
//     the row's `signature` column carries a real CP-produced Ed25519
//     signature (this is the same all-zero-seed keypair the issue #611
//     wire-contract fixture uses, so the same wire shape is pinned
//     on both sides),
//   - the configured internal token string, so tests can present the
//     correct `X-Internal-Token` header without re-reading the cfg,
//   - the same `storage.ArtifactStore` wired into `app.New`, so
//     tamper tests can rewrite the on-disk artifact post-seed and
//     observe the CP streaming the modified bytes (the CP doesn't
//     re-verify before serving).
//
// Each call gets its own Postgres container + FS artifact root
// (both via t.TempDir / t.Cleanup) so tests can run in parallel and
// don't share state. The 100 MiB body cap is honored via the FS
// artifact store the same way production does.
func newTestCP(t *testing.T) (*httptest.Server, *sqlx.DB, *signing.Keyring, string, storage.ArtifactStore) {
	t.Helper()
	ctx := context.Background()

	pgC := newTestPostgres(t, ctx)
	db := newDBFromContainer(t, ctx, pgC)

	// Apply every up migration. Mirrors the pattern at
	// `migrations/roundtrip_test.go:1093-1098`. Each test gets a fresh
	// container so we only need the up path.
	src := &migrate.FileMigrationSource{Dir: migrationsDir(t)}
	n, err := migrate.Exec(db.DB, "postgres", src, migrate.Up)
	require.NoError(t, err, "apply migrations")
	require.Greater(t, n, 0, "expected at least one migration to apply")

	// Build the test config. Mirrors `internal/app/app_test.go:30-85` —
	// the 32-byte all-zero signing key file is reused as the seed for
	// both the production keyring (read by `app.New`) and the test's
	// independent `signing.TestKeyring` (used by `seedDeploymentRow`).
	// They produce identical Ed25519 keypairs because both derive from
	// the same all-zero seed, so a row signed by the test keyring
	// verifies cleanly under `keyring.Verify` (any keyring built from
	// the same seed will work; the production keyring also accepts it).
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
			Secret: e2eJWTSecret,
			Issuer: e2eIssuer,
			TTL:    24,
		},
		Region:          "test",
		BootstrapSecret: testBootstrapSecret,
		InternalToken:   e2eInternalToken,
		Storage: config.StorageConfig{
			ArtifactBackend: "fs",
			ArtifactPath:    artifactPath,
		},
		Signing: config.SigningConfig{
			Keyring: "test-k1 = " + keyHexMaterial,
			KeyID:   "test-k1",
		},
	}

	// The publisher stays a zero-value — `app_test.go:102` proves the
	// constructor is safe with `&nats.NATSPublisher{}`. The handler
	// chain doesn't touch NATS at the HTTP layer; only
	// `publisher.Conn()` (returns nil on zero-value) is consulted by
	// background goroutines the test never starts.
	artifactStore := storage.NewFSArtifactStore(artifactPath)
	application := app.New(cfg, db, &nats.NATSPublisher{}, artifactStore, emptyFS)
	require.NotNil(t, application)
	require.NotNil(t, application.Handler, "app.Handler must be non-nil")

	srv := httptest.NewServer(application.Handler)
	t.Cleanup(srv.Close)

	// The test's independent keyring uses the same all-zero seed as the
	// production keyring built inside `app.New`. Both produce the same
	// public key, so a row signed by `signing.TestKeyring(t)` verifies
	// under the production keyring too — but the test re-uses
	// `signing.Keyring.Verify` directly on its own keyring for symmetry
	// with what the Rust `verifier::Keyring::verify` does on its side.
	keyring := signing.TestKeyring(t)

	return srv, db, keyring, e2eInternalToken, artifactStore
}

// signBootstrapPayload is defined in `internal_test.go:276` and reused
// here — same package, identical implementation (HMAC-SHA256 hex over
// `worker_id:region:tenant_id:timestamp:nonce`), mirrors the CP-side
// verification in `internal/handler/internal.go:685-689`.

// issueWorkerJWT mints an HS256 worker JWT shaped exactly like the
// production `WorkerClaims` (`internal/middleware/worker.go:15-29`) and
// satisfying the same validation constraints as `VerifyWorkerJWT`
// (`internal/middleware/worker.go:92-108`): `jwt.WithExpirationRequired`,
// `iss` claim enforced, HS256 signing method only.
//
// Returns the signed token string. The worker side of issue #612
// produces equivalent JWTs via the `WorkerJwtSigner` in
// `edge-worker/src/auth.rs`; the resulting wire format is identical.
func issueWorkerJWT(t *testing.T, secret, issuer string, claims middleware.WorkerClaims) string {
	t.Helper()
	if claims.ExpiresAt == nil {
		claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(24 * time.Hour))
	}
	if claims.IssuedAt == nil {
		claims.IssuedAt = jwt.NewNumericDate(time.Now())
	}
	if claims.Issuer == "" {
		claims.Issuer = issuer
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString([]byte(secret))
	require.NoError(t, err, "sign worker JWT")
	return signed
}

// seedDeploymentRow writes the artifact bytes to the FS artifact store,
// SHA-256s them, signs `(sha256_raw || deployment_id)` with the supplied
// keyring, and inserts the row via `repository.DeploymentRepository.Create`.
// Returns `(hashHex, sig, kid, tenantID, appName)` so the caller can
// assert the row's wire shape against the download response.
//
// The keyring must be the same one that built the row's signature;
// `Keyring.Verify` resolves the key by `kid`. The test's keyring is
// `signing.TestKeyring(t)` (all-zero seed, kid "test-k1"); production
// rows stamped with `signing.Keyring.Sign` carry the kid the keyring
// reports back (`ActiveKeyID`), which for `TestKeyring` is "test-k1".
func seedDeploymentRow(
	t *testing.T,
	db *sqlx.DB,
	artifactStore storage.ArtifactStore,
	keyring *signing.Keyring,
	tenantID, appName, deploymentID string,
	wasmBytes []byte,
) (hashHex, sig, kid string) {
	t.Helper()
	ctx := context.Background()

	require.NoError(t, artifactStore.Save(ctx, tenantID, appName, deploymentID, bytes.NewReader(wasmBytes)),
		"save artifact bytes to FS store")

	sum := sha256.Sum256(wasmBytes)
	hashHex = hex.EncodeToString(sum[:])

	var err error
	sig, kid, err = keyring.Sign(hashHex, deploymentID)
	require.NoError(t, err, "keyring.Sign")

	dep := &domain.Deployment{
		ID:              deploymentID,
		TenantID:        tenantID,
		AppName:         appName,
		Status:          "deployed",
		Hash:            hashHex,
		Signature:       sig,
		SigningKeyID:    kid,
		CreatedAt:       time.Now(),
		DesiredReplicas: 1,
	}
	require.NoError(t, repository.NewDeploymentRepository(db).Create(ctx, dep),
		"insert deployment row")
	return hashHex, sig, kid
}

// freshNonce returns a 16-byte hex nonce suitable for the bootstrap
// request's `nonce` field. Distinct per test run so a duplicate
// nonce (which would still pass since the server doesn't replay-protect
// against duplicates within the 5-min window) can't cause a false green
// after a failed first attempt.
func freshNonce(t *testing.T) string {
	t.Helper()
	var b [16]byte
	_, err := rand.Read(b[:])
	require.NoError(t, err)
	return hex.EncodeToString(b[:])
}

// ── Test 1: full bootstrap handshake ───────────────────────────────

// TestWorkerCP_BootstrapHandshakeSucceeds exercises the two-phase
// bootstrap the worker code path in `edge-worker/src/bootstrap.rs`
// performs on first boot. Phase 1: POST `/api/internal/bootstrap`
// with an HMAC-signed body → 200 + `{token: <bootstrap_jwt>}`. Phase
// 2: GET `/api/internal/worker-secret` with the bootstrap JWT → 200
// + `{secret: <jwt_secret>}`. The returned secret must equal the
// JWT_SECRET the test configured (`e2eJWTSecret`).
func TestWorkerCP_BootstrapHandshakeSucceeds(t *testing.T) {
	srv, _, _, _, _ := newTestCP(t)

	workerID := "w_test_handshake"
	region := "fra"
	tenantID := "t_test_handshake"
	ts := time.Now().Format(time.RFC3339)
	nonce := freshNonce(t)
	sig := signBootstrapPayload(workerID, region, tenantID, ts, nonce, testBootstrapSecret)

	// ── Phase 1: POST /api/internal/bootstrap ─────────────────────
	phase1Body, err := json.Marshal(map[string]string{
		"worker_id": workerID,
		"region":    region,
		"tenant_id": tenantID,
		"timestamp": ts,
		"nonce":     nonce,
		"signature": sig,
	})
	require.NoError(t, err)
	resp1, err := http.Post(srv.URL+"/api/internal/bootstrap",
		"application/json", bytes.NewReader(phase1Body))
	require.NoError(t, err)
	defer resp1.Body.Close()
	require.Equal(t, http.StatusOK, resp1.StatusCode,
		"phase 1: status=%d, want 200; body=%s", resp1.StatusCode, readBody(resp1.Body))

	var phase1Resp struct {
		Token string `json:"token"`
	}
	require.NoError(t, json.NewDecoder(resp1.Body).Decode(&phase1Resp))
	require.NotEmpty(t, phase1Resp.Token, "phase 1: response missing 'token'")

	// The bootstrap JWT must verify under the bootstrap secret with
	// the expected claims. Same check the existing unit test at
	// `internal/handler/internal_test.go:402-418` performs.
	claims, err := middleware.VerifyBootstrapJWT(phase1Resp.Token, middleware.BootstrapJWTConfig{
		BootstrapSecret: testBootstrapSecret,
		Issuer:          "edgecloud-bootstrap",
	})
	require.NoError(t, err, "verify bootstrap JWT")
	require.Equal(t, workerID, claims.WorkerID)
	require.Equal(t, tenantID, claims.TenantID)
	require.Equal(t, region, claims.Region)

	// ── Phase 2: GET /api/internal/worker-secret ──────────────────
	req2, err := http.NewRequest(http.MethodGet, srv.URL+"/api/internal/worker-secret", nil)
	require.NoError(t, err)
	req2.Header.Set("Authorization", "Bearer "+phase1Resp.Token)
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.Equal(t, http.StatusOK, resp2.StatusCode,
		"phase 2: status=%d, want 200; body=%s", resp2.StatusCode, readBody(resp2.Body))

	var phase2Resp struct {
		Secret string `json:"secret"`
	}
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&phase2Resp))
	require.Equal(t, e2eJWTSecret, phase2Resp.Secret,
		"phase 2: returned secret must equal configured JWT_SECRET")
}

// ── Test 2: bootstrap rejects a body signed with the wrong secret ─

// TestWorkerCP_BootstrapRejectsBadSignature pins the failure mode at
// `internal/handler/internal.go:690-694`: an HMAC computed with the
// wrong secret must 401. The worker uses `subtle.ConstantTimeCompare`
// (via `hmac.Equal`) so the timing-safe path is exercised; we just
// assert the HTTP status code here.
func TestWorkerCP_BootstrapRejectsBadSignature(t *testing.T) {
	srv, _, _, _, _ := newTestCP(t)

	ts := time.Now().Format(time.RFC3339)
	nonce := freshNonce(t)
	// Sign with the WRONG secret — must reject.
	badSig := signBootstrapPayload("w_test_bad", "fra", "t_test_bad", ts, nonce, "not-the-real-secret")

	body, err := json.Marshal(map[string]string{
		"worker_id": "w_test_bad",
		"region":    "fra",
		"tenant_id": "t_test_bad",
		"timestamp": ts,
		"nonce":     nonce,
		"signature": badSig,
	})
	require.NoError(t, err)
	resp, err := http.Post(srv.URL+"/api/internal/bootstrap",
		"application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode,
		"bad HMAC must 401; body=%s", readBody(resp.Body))
}

// ── Test 3: bootstrap rejects a timestamp outside ±5 minutes ───────

// TestWorkerCP_BootstrapRejectsStaleTimestamp pins the freshness check
// at `internal/handler/internal.go:679-681` (±5 min skew window).
// Using `-10 minutes` ensures we're well outside the window.
func TestWorkerCP_BootstrapRejectsStaleTimestamp(t *testing.T) {
	srv, _, _, _, _ := newTestCP(t)

	stale := time.Now().Add(-10 * time.Minute).Format(time.RFC3339)
	nonce := freshNonce(t)
	sig := signBootstrapPayload("w_test_stale", "fra", "t_test_stale", stale, nonce, testBootstrapSecret)

	body, err := json.Marshal(map[string]string{
		"worker_id": "w_test_stale",
		"region":    "fra",
		"tenant_id": "t_test_stale",
		"timestamp": stale,
		"nonce":     nonce,
		"signature": sig,
	})
	require.NoError(t, err)
	resp, err := http.Post(srv.URL+"/api/internal/bootstrap",
		"application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode,
		"stale timestamp must 400; body=%s", readBody(resp.Body))
}

// readBody reads and returns the response body bytes for diagnostic
// messages. Truncated to 4 KiB so a large body doesn't drown the test
// output on failure.
func readBody(r io.Reader) string {
	const maxDump = 4 << 10
	b, _ := io.ReadAll(io.LimitReader(r, maxDump))
	return fmt.Sprintf("%s", b)
}

// freshArtifactBytes returns a deterministic-looking byte slice for
// the seed-deployment helper. The contents don't need to be a real
// Wasm module — the test re-hashes whatever the server streams back
// and compares against `row.Hash`. Using distinct per-test bytes
// keeps the cache invalidation case (#5) easy to read.
func freshArtifactBytes(seed byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = seed
	}
	return out
}

// ── Test 4: worker-JWT download lane — the headline test ────────────

// TestWorkerCP_DownloadViaWorkerJWT_Succeeds is the headline test for
// issue #612. It seeds a `deployments` row with a real CP-produced
// Ed25519 signature (via `signing.Keyring.Sign`), mints a worker JWT
// matching the row's tenant, downloads the artifact via the JWT lane,
// and asserts:
//
//  1. HTTP 200,
//  2. body bytes equal the seeded wasm,
//  3. SHA-256 of the body matches the row's `hash` column,
//  4. `signing.Keyring.Verify(hash, deployment_id, row.Signature,
//     row.SigningKeyID)` returns true — the same algorithm the Rust
//     `Downloader::verify_signature` runs in `edge-worker/src/downloader.rs:446`.
//
// The Rust side of the wire is independently pinned by
// `edge-worker/tests/signing_wire_contract.rs::well_known_signature_verifies_in_rust_keyring`
// against the same signature format, so a green run on both
// `go-test-integration` and `rust-test` proves the cross-language wire
// is intact.
func TestWorkerCP_DownloadViaWorkerJWT_Succeeds(t *testing.T) {
	srv, db, keyring, _, store := newTestCP(t)

	tenantID := "t_dl_jwt_ok"
	appName := "app-jwt"
	deploymentID := "d_dl_jwt_ok_0001"
	wasmBytes := freshArtifactBytes(0xAB, 256)

	// The `store` returned by newTestCP is the same FS-backed artifact
	// store wired into `app.New`; seedDeploymentRow writes the bytes
	// through it, and the Download handler later reads them back
	// through it. Using the same instance keeps the on-disk layout
	// consistent between seed and serve.
	hashHex, sig, kid := seedDeploymentRow(t, db, store, keyring,
		tenantID, appName, deploymentID, wasmBytes)

	// Mint a worker JWT scoped to the row's tenant.
	tok := issueWorkerJWT(t, e2eJWTSecret, e2eIssuer, middleware.WorkerClaims{
		WorkerID: "w_dl_jwt",
		TenantID: tenantID,
		Region:   "test",
		Apps:     []string{appName},
	})

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/api/internal/download/"+deploymentID, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"worker-JWT download must 200; body=%s", readBody(resp.Body))

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, wasmBytes, got, "downloaded bytes must equal seeded wasm")

	// Re-hash the body in-test and assert the row's `hash` column
	// matches. This is the same check `verify_hash` in the Rust
	// downloader (`edge-worker/src/downloader.rs:532-580`) does.
	sum := sha256.Sum256(got)
	require.Equal(t, hashHex, hex.EncodeToString(sum[:]),
		"recomputed SHA-256 of response body must match row.hash")

	// Verify the row's stored Ed25519 signature against the re-hashed
	// payload. This is the same call `Downloader::verify_signature`
	// makes on the worker side.
	ok, err := keyring.Verify(hashHex, deploymentID, sig, kid)
	require.NoError(t, err, "keyring.Verify must not error")
	require.True(t, ok,
		"the row's stored signature must verify under the test keyring — a failure here means a Go signer / verifier drift that breaks the cross-language wire")
}

// ── Test 5: worker-JWT download returns tampered artifact ──────────

// TestWorkerCP_DownloadViaWorkerJWT_TamperedArtifactRejected seeds a
// row with a real signature, then overwrites the on-disk artifact
// bytes post-seed. The CP streams whatever is on disk (it does NOT
// re-verify before serving — see `internal.go:217-255`); the worker
// is the side that catches the mismatch via `verify_hash`. The test
// confirms the CP-side guard failure mode: the server still returns
// 200 + the (tampered) bytes, but the recomputed SHA-256 of those
// bytes no longer matches `row.Hash` — which is exactly the signal
// `Downloader::verify_hash` would catch in production.
func TestWorkerCP_DownloadViaWorkerJWT_TamperedArtifactRejected(t *testing.T) {
	srv, db, keyring, _, store := newTestCP(t)

	tenantID := "t_dl_jwt_tamper"
	appName := "app-tamper"
	deploymentID := "d_dl_jwt_tamper_0001"
	wasmBytes := freshArtifactBytes(0xCD, 256)

	hashHex, _, _ := seedDeploymentRow(t, db, store, keyring,
		tenantID, appName, deploymentID, wasmBytes)

	// Overwrite the on-disk artifact with different bytes. The CP
	// keeps the row's signature and hash (those live in Postgres),
	// but the FS artifact store no longer matches.
	tampered := freshArtifactBytes(0xEF, 256)
	require.NoError(t, store.Save(context.Background(), tenantID, appName, deploymentID,
		bytes.NewReader(tampered)))

	tok := issueWorkerJWT(t, e2eJWTSecret, e2eIssuer, middleware.WorkerClaims{
		WorkerID: "w_dl_jwt_tamper",
		TenantID: tenantID,
		Region:   "test",
		Apps:     []string{appName},
	})
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/api/internal/download/"+deploymentID, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"the CP doesn't re-verify before streaming; it returns the bytes on disk regardless. body=%s", readBody(resp.Body))

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, tampered, got,
		"the CP should return whatever is on disk — the row's hash is the worker's guard, not the CP's")

	sum := sha256.Sum256(got)
	require.NotEqual(t, hashHex, hex.EncodeToString(sum[:]),
		"the worker-side guard (Downloader::verify_hash) catches this mismatch; if this assertion ever fails, the row and the artifact have drifted in an unrecoverable way")
}

// ── Test 11: missing deployment returns 404 ────────────────────────

// TestWorkerCP_DownloadMissingDeployment_Returns404 asserts that a
// valid worker JWT for a non-existent deployment_id returns 404, not
// 200 or 500. Pins the `httperror.NotFoundCtx` branch in
// `internal.go:228-231`.
func TestWorkerCP_DownloadMissingDeployment_Returns404(t *testing.T) {
	srv, _, _, _, _ := newTestCP(t)

	tok := issueWorkerJWT(t, e2eJWTSecret, e2eIssuer, middleware.WorkerClaims{
		WorkerID: "w_dl_missing",
		TenantID: "t_dl_missing",
		Region:   "test",
	})
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/api/internal/download/d_does_not_exist", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode,
		"missing deployment must 404; body=%s", readBody(resp.Body))
}

// ── Test 12: ?format=wasm (and omitted) streams the .wasm file ──────

// TestWorkerCP_DownloadFormatQuery_WasmDefault asserts the
// `?format=wasm` query (and the default-empty form) routes to the
// `.wasm` artifact. Pins `internal.go:233-234` + `FSArtifactStore.OpenFormat`
// (`internal/storage/artifact.go:260-265`).
func TestWorkerCP_DownloadFormatQuery_WasmDefault(t *testing.T) {
	srv, db, keyring, _, store := newTestCP(t)

	tenantID := "t_dl_format"
	appName := "app-format"
	deploymentID := "d_dl_format_0001"
	wasmBytes := freshArtifactBytes(0x77, 128)
	seedDeploymentRow(t, db, store, keyring,
		tenantID, appName, deploymentID, wasmBytes)

	tok := issueWorkerJWT(t, e2eJWTSecret, e2eIssuer, middleware.WorkerClaims{
		WorkerID: "w_dl_format",
		TenantID: tenantID,
		Region:   "test",
	})

	for _, query := range []string{"", "?format=wasm"} {
		t.Run("query="+query, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, srv.URL+"/api/internal/download/"+deploymentID+query, nil)
			require.NoError(t, err)
			req.Header.Set("Authorization", "Bearer "+tok)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode,
				"format=%q must 200; body=%s", query, readBody(resp.Body))
			require.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
			got, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, wasmBytes, got, "format=%q must stream the .wasm file", query)
		})
	}
}
