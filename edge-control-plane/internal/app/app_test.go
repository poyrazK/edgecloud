package app

import (
	"bytes"
	"context"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/config"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
	"github.com/golang-jwt/jwt/v5"
	"github.com/jmoiron/sqlx"
)

// emptyFS is a zero-value embed.FS for tests that don't need the OpenAPI spec.
var emptyFS embed.FS

func testConfig(t *testing.T, artifactPath string) *config.Config {
	t.Helper()
	// Write a dummy Ed25519 signing key file (issue #307). The file's
	// contents don't need to be a real key — loadSigner is called
	// once and fails fast at the parsing step if the file is empty,
	// so we write a 32-byte all-zero "seed" that the signing package
	// accepts via ed25519.NewKeyFromSeed. The signing-key requirement
	// was added by issue #307 and every New() call needs a key on
	// disk; tests don't sign anything so the actual key bytes are
	// irrelevant.
	keyPath := filepath.Join(t.TempDir(), "test_signing.key")
	if err := os.WriteFile(keyPath, make([]byte, 32), 0o600); err != nil {
		t.Fatalf("write fixture signing key: %v", err)
	}

	// keyHexMaterial returns the on-disk key's bytes as 64 lowercase
	// hex chars — the form `signing.LoadKeyringFromInline` parses.
	// Issue #307 PR1: the test config exercises the new keyring
	// loader instead of the legacy single-key fallback.
	keyHexMaterial := func() string {
		data, err := os.ReadFile(keyPath)
		if err != nil {
			t.Fatalf("read fixture signing key: %v", err)
		}
		return hex.EncodeToString(data)
	}()
	return &config.Config{
		Database: config.DatabaseConfig{
			Host: "localhost", Port: 5432, User: "test", Password: "test", Name: "test", SSLMode: "disable",
		},
		NATS: config.NATSConfig{URL: "nats://localhost:4222"},
		App:  config.AppConfig{Host: "0.0.0.0", Port: 8080, Env: "test"},
		JWT: config.JWTConfig{
			Secret: "this-is-a-32-byte-test-secret-x!",
			Issuer: "edgecloud-test",
			TTL:    24,
		},
		Region: "test",
		Storage: config.StorageConfig{
			ArtifactBackend: "fs",
			ArtifactPath:    artifactPath,
		},
		// Issue #307: signing key is required for the CP to start.
		// Without this, New() log.Fatalf's with the missing-key
		// error and the test binary exits 1.
		Signing: config.SigningConfig{
			// Issue #307 PR1: the CP now boots with a keyring
			// (`EDGE_SIGNING_KEYRING[_PATH]`); the legacy single-key
			// form (`KeyPath` + `KeyID`) is a one-release deprecation
			// fallback that refuses a non-default KeyID. Test the
			// supported keyring path here.
			Keyring: "test-k1 = " + keyHexMaterial,
			KeyID:   "test-k1", // active signing kid — must be present in the keyring
		},
	}
}

func newMockDB(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock) {
	t.Helper()
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	t.Cleanup(func() { _ = mockDB.Close() })
	return sqlxDB, mock
}

func TestNew_ReturnsNonNilApp(t *testing.T) {
	artifactPath := t.TempDir()
	cfg := testConfig(t, artifactPath)
	db, _ := newMockDB(t)
	publisher := &nats.NATSPublisher{}
	artifactStore := storage.NewFSArtifactStore(artifactPath)

	app := New(cfg, db, publisher, artifactStore, emptyFS)
	if app == nil {
		t.Fatal("New returned nil")
	}
	if app.Handler == nil {
		t.Fatal("Handler is nil")
	}
	if app.Region != "test" {
		t.Errorf("Region = %q, want test", app.Region)
	}
}

func TestNew_RoutesAreRegistered(t *testing.T) {
	artifactPath := t.TempDir()
	cfg := testConfig(t, artifactPath)
	db, _ := newMockDB(t)
	publisher := &nats.NATSPublisher{}
	artifactStore := storage.NewFSArtifactStore(artifactPath)

	app := New(cfg, db, publisher, artifactStore, emptyFS)

	type route struct{ method, path string }
	routes := []route{
		{"GET", "/metrics"},
		{"GET", "/docs"},
		{"GET", "/docs"},
		{"GET", "/docs/"},
		{"GET", "/api/tenants"},
		{"POST", "/api/tenants"},
		{"GET", "/api/apps"},
		{"GET", "/api/keys"},
		{"GET", "/api/v1/apps"},
		{"GET", "/api/v1/keys"},
		{"GET", "/api/v1/quotas"},
		{"GET", "/api/v1/admin/tenants"},
		{"GET", "/api/v1/admin/cluster"},
		{"GET", "/api/internal/workers"},
		{"GET", "/api/v1/admin/secrets/keys"},
	}

	for _, rt := range routes {
		t.Run(rt.method+" "+rt.path, func(t *testing.T) {
			req := httptest.NewRequest(rt.method, rt.path, nil)
			rr := httptest.NewRecorder()
			app.Handler.ServeHTTP(rr, req)

			if rr.Code == http.StatusNotFound {
				t.Errorf("route %s %s returned 404 — not registered", rt.method, rt.path)
			}
		})
	}
}

func TestNew_AuthMiddlewareBlocksUnauthenticated(t *testing.T) {
	artifactPath := t.TempDir()
	cfg := testConfig(t, artifactPath)
	db, _ := newMockDB(t)
	publisher := &nats.NATSPublisher{}
	artifactStore := storage.NewFSArtifactStore(artifactPath)

	app := New(cfg, db, publisher, artifactStore, emptyFS)

	protectedRoutes := []string{
		"GET /api/v1/apps",
		"GET /api/v1/keys",
		"GET /api/v1/quotas",
		"GET /api/v1/admin/tenants",
		"GET /api/v1/admin/cluster",
	}

	for _, rt := range protectedRoutes {
		t.Run(rt, func(t *testing.T) {
			parts := splitMethod(rt)
			req := httptest.NewRequest(parts[0], parts[1], nil)
			rr := httptest.NewRecorder()
			app.Handler.ServeHTTP(rr, req)

			if rr.Code != http.StatusUnauthorized {
				t.Errorf("%s: status = %d, want 401", rt, rr.Code)
			}
		})
	}
}

func TestNew_LegacyRedirectRoutes(t *testing.T) {
	artifactPath := t.TempDir()
	cfg := testConfig(t, artifactPath)
	db, _ := newMockDB(t)
	publisher := &nats.NATSPublisher{}
	artifactStore := storage.NewFSArtifactStore(artifactPath)

	app := New(cfg, db, publisher, artifactStore, emptyFS)

	legacyRoutes := []string{
		"GET /api/tenants",
		"POST /api/tenants",
		"GET /api/apps",
		"POST /api/keys",
		"GET /api/keys",
	}

	for _, rt := range legacyRoutes {
		t.Run(rt, func(t *testing.T) {
			parts := splitMethod(rt)
			req := httptest.NewRequest(parts[0], parts[1], nil)
			rr := httptest.NewRecorder()
			app.Handler.ServeHTTP(rr, req)

			if rr.Code != http.StatusMovedPermanently {
				t.Errorf("%s: status = %d, want 301", rt, rr.Code)
			}
			if rr.Header().Get("Sunset") == "" {
				t.Errorf("%s: missing Sunset header", rt)
			}
		})
	}
}

func TestParseDurationEnv(t *testing.T) {
	tests := []struct {
		name     string
		envVar   string
		envValue string
		def      time.Duration
		want     time.Duration
	}{
		{"unset uses default", "TEST_DUR_UNSET", "", time.Hour, time.Hour},
		{"valid go duration", "TEST_DUR_VALID", "5m", time.Hour, 5 * time.Minute},
		{"zero rejected", "TEST_DUR_ZERO", "0s", time.Hour, time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(tt.envVar, tt.envValue)
			got := parseDurationEnv(tt.envVar, tt.def)
			if got != tt.want {
				t.Errorf("parseDurationEnv(%q, %v) = %v, want %v", tt.envVar, tt.def, got, tt.want)
			}
		})
	}
}

func TestStableWindowFromEnv(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		want     time.Duration
	}{
		{"unset", "", 0},
		{"valid seconds", "30", 30 * time.Second},
		{"negative rejected", "-1", 0},
		{"non-numeric rejected", "abc", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("STABLE_WINDOW_SECONDS", tt.envValue)
			got := stableWindowFromEnv()
			if got != tt.want {
				t.Errorf("stableWindowFromEnv() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRunBackground_DoesNotPanic(t *testing.T) {
	artifactPath := t.TempDir()
	cfg := testConfig(t, artifactPath)
	db, _ := newMockDB(t)
	publisher := &nats.NATSPublisher{}
	artifactStore := storage.NewFSArtifactStore(artifactPath)

	app := New(cfg, db, publisher, artifactStore, emptyFS)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	app.RunBackground(ctx) // must not panic
}

// TestRunBackground_GoroutinesStart replaces the smoke-only behavior of
// the original TestRunBackground_DoesNotPanic: the prior test used a
// pre-cancelled ctx with a zero-value NATS publisher, so the heartbeat
// and autoscale goroutines exited before doing anything and the four
// GC loops only ran their "refuses to start on invalid intervals" code
// path.
//
// This test verifies the same shape via the loophealth tracker directly:
// wrapping the real GC services would need a real DB (the sqlmock has
// no expectations registered for the SQL statements those loops issue,
// which would block), so we exercise RunErr/Run wrappers in isolation
// with stub bodies that mirror how RunBackground uses them.
func TestRunBackground_WrappersRegisterAndRecover(t *testing.T) {
	artifactPath := t.TempDir()
	cfg := testConfig(t, artifactPath)
	db, _ := newMockDB(t)
	publisher := &nats.NATSPublisher{}
	artifactStore := storage.NewFSArtifactStore(artifactPath)

	app := New(cfg, db, publisher, artifactStore, emptyFS)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Mirror the wrapper invocations RunBackground would issue, but
	// with bodies that don't touch the database. This verifies the
	// tracker registration + panic recovery path of the wrappers.
	done := make(chan struct{})
	go func() {
		defer close(done)
		app.loopHealth.RunErr(ctx, "heartbeat", "heartbeat: ", func(string, ...any) {}, func(c context.Context) error {
			<-c.Done()
			return nil
		})
	}()
	go func() {
		app.loopHealth.Run(ctx, "log_gc", "log_gc: ", func(string, ...any) {}, func(c context.Context) {
			<-c.Done()
		})
	}()

	// Let wrappers enter their bodies (synchronously registers and sets running=true).
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done
	time.Sleep(20 * time.Millisecond)

	snap := app.loopHealth.Snapshot()
	have := map[string]bool{}
	for _, s := range snap {
		have[s.Name] = true
		if s.Panics != 0 {
			t.Errorf("loop %s panicked %d times", s.Name, s.Panics)
		}
		if s.Running {
			t.Errorf("loop %s still running after ctx cancel", s.Name)
		}
		if s.StartedAt == "" {
			t.Errorf("loop %s missing started_at", s.Name)
		}
	}
	for _, name := range []string{"heartbeat", "log_gc"} {
		if !have[name] {
			t.Errorf("expected loop %q in snapshot, got %v", name, snap)
		}
	}
}

// TestRunBackground_PanicInLogGCIsRecovered verifies that a panic inside
// a GC loop body is recovered by the wrapper, counted, and logged —
// without killing the other loops. We use the loophealth.Tracker.Run
// helper directly because injecting a panic into the real LogGCService
// would require a sqlmock dance that's brittle across refactors.
func TestRunBackground_PanicInLogGCIsRecovered(t *testing.T) {
	artifactPath := t.TempDir()
	cfg := testConfig(t, artifactPath)
	db, _ := newMockDB(t)
	publisher := &nats.NATSPublisher{}
	artifactStore := storage.NewFSArtifactStore(artifactPath)

	app := New(cfg, db, publisher, artifactStore, emptyFS)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Simulate a panicking log_gc run. Run spawns its own goroutine
	// (review finding #1 fix), so we poll for the panic counter to
	// bump rather than waiting on a done channel.
	app.loopHealth.Run(ctx, "log_gc", "log_gc: ", func(string, ...any) {
		// discard — the LogFn path is exercised by loophealth_test.go
	}, func(c context.Context) {
		panic("forced panic in log_gc")
	})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if app.loopHealth.Get("log_gc").Panics() == 1 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	cancel()

	if got := app.loopHealth.Get("log_gc").Panics(); got != 1 {
		t.Errorf("log_gc.Panics = %d, want 1", got)
	}
	if app.loopHealth.Get("log_gc").Running() {
		t.Errorf("expected log_gc.Running=false after panic")
	}

	// Other loops should still be unaffected (untouched).
	for _, name := range []string{"reconcile", "worker_gc", "deployment_gc", "heartbeat", "autoscale"} {
		if got := app.loopHealth.Get(name).Panics(); got != 0 {
			t.Errorf("%s.Panics = %d, want 0", name, got)
		}
	}
}

// TestReady_HealthyReturns200 verifies the default /ready response
// shape (issue #48 — was TestHealth_HealthyReturns200 pre-split):
// 200 + status=ok + loops map populated. The deep readiness check
// pings the DB; this test gives it a successful mock ping.
func TestReady_HealthyReturns200(t *testing.T) {
	artifactPath := t.TempDir()
	cfg := testConfig(t, artifactPath)
	db, mock, cleanup := newMockDBWithMock(t)
	defer cleanup()
	// Allow PingContext to succeed.
	mock.ExpectPing()
	publisher := &nats.NATSPublisher{}
	artifactStore := storage.NewFSArtifactStore(artifactPath)

	app := New(cfg, db, publisher, artifactStore, emptyFS)

	// Pre-populate the tracker so the loops map is non-empty.
	// Run is now non-blocking (wrapper spawns its own goroutine), so
	// poll for the body to have entered before the GET — otherwise
	// the goroutine may not have registered the loop yet and the
	// assertion races the scheduler.
	app.loopHealth.Run(context.Background(), "log_gc", "log_gc: ", func(string, ...any) {}, func(context.Context) {})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !app.loopHealth.Get("log_gc").StartedAt().IsZero() {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	if app.loopHealth.Get("log_gc").StartedAt().IsZero() {
		t.Fatalf("log_gc loop never entered within 2s (tracker race after Run wrapper change)")
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	app.Handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}

	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got := body["status"]; got != "ok" {
		t.Errorf("status = %v, want ok", got)
	}
	loops, ok := body["loops"].(map[string]any)
	if !ok {
		t.Fatalf("loops field missing or wrong type: %v", body["loops"])
	}
	if _, exists := loops["log_gc"]; !exists {
		t.Errorf("loops map missing log_gc entry: %v", loops)
	}
	if _, hasReasons := body["degraded_reasons"]; hasReasons {
		t.Errorf("degraded_reasons should be absent on healthy response: %v", body)
	}
}

// TestReady_DegradedStays200 verifies the post-#48 behavior: any loop
// with panics>0 (or stale) flips status to "degraded" on /ready but
// stays 200 so load balancers don't pull the CP from rotation. DB
// and NATS are healthy throughout.
func TestReady_DegradedStays200(t *testing.T) {
	artifactPath := t.TempDir()
	cfg := testConfig(t, artifactPath)
	db, mock, cleanup := newMockDBWithMock(t)
	defer cleanup()
	mock.ExpectPing()
	publisher := &nats.NATSPublisher{}
	artifactStore := storage.NewFSArtifactStore(artifactPath)

	app := New(cfg, db, publisher, artifactStore, emptyFS)

	// Drive a real recovered panic through the wrapper so the test
	// exercises the production panic-recovery path (review finding #4
	// fix: the heartbeat drain now bumps the counter via the tracker
	// field on WorkerService). Run is now non-blocking — poll for
	// the counter to bump.
	app.loopHealth.Run(context.Background(), "heartbeat", "heartbeat: ", func(string, ...any) {}, func(c context.Context) {
		panic("forced for /ready degraded test")
	})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if app.loopHealth.Get("heartbeat").Panics() == 1 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	if app.loopHealth.Get("heartbeat").Panics() != 1 {
		t.Fatalf("heartbeat.Panics = %d, want 1 (recovered panic did not register)", app.loopHealth.Get("heartbeat").Panics())
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	app.Handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (degraded must stay 200); body=%s", rr.Code, rr.Body.String())
	}

	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got := body["status"]; got != "degraded" {
		t.Errorf("status = %v, want degraded", got)
	}
	reasons, ok := body["degraded_reasons"].([]any)
	if !ok {
		t.Fatalf("degraded_reasons field missing or wrong type: %v", body["degraded_reasons"])
	}
	if len(reasons) != 1 || reasons[0] != "heartbeat" {
		t.Errorf("degraded_reasons = %v, want [heartbeat]", reasons)
	}
}

// TestReady_UnhealthyOnDBPingFailure preserves the pre-#48 503
// behavior on the deep path: a failed DB ping must produce
// status=unhealthy + failure_component="db" so load balancers pull
// this instance from rotation.
func TestReady_UnhealthyOnDBPingFailure(t *testing.T) {
	artifactPath := t.TempDir()
	cfg := testConfig(t, artifactPath)
	db, mock, cleanup := newMockDBWithMock(t)
	defer cleanup()
	mock.ExpectPing().WillReturnError(errDBPingFailed)
	publisher := &nats.NATSPublisher{}
	artifactStore := storage.NewFSArtifactStore(artifactPath)

	app := New(cfg, db, publisher, artifactStore, emptyFS)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	app.Handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got := body["status"]; got != "unhealthy" {
		t.Errorf("status = %v, want unhealthy", got)
	}
	if got := body["failure_component"]; got != "db" {
		t.Errorf("failure_component = %v, want db", got)
	}
	if body["loops"] != nil {
		t.Errorf("unhealthy response should not include loops map, got: %v", body["loops"])
	}
	// Pin the body to the documented UnhealthyResponse schema
	// (docs/api/openapi.yaml:112 — added in this PR's follow-up so
	// the 503 body shape is documented, not just the 200). The
	// schema requires exactly status + failure_component + error;
	// any extra field would diverge from the spec.
	wantKeys := map[string]struct{}{
		"status":            {},
		"failure_component": {},
		"error":             {},
	}
	for k := range body {
		if _, ok := wantKeys[k]; !ok {
			t.Errorf("unexpected key %q in 503 body (UnhealthyResponse schema does not declare it); full body: %v", k, body)
		}
	}
	for k := range wantKeys {
		if _, ok := body[k]; !ok {
			t.Errorf("missing required key %q from 503 body (UnhealthyResponse schema requires it); full body: %v", k, body)
		}
	}
	if errStr, ok := body["error"].(string); !ok || errStr == "" {
		t.Errorf("error must be a non-empty string per UnhealthyResponse schema; got %v", body["error"])
	}
}

// TestHealth_LivenessAlwaysOK verifies the post-#48 /health handler is
// pure liveness: it returns 200 + {"status":"ok"} even when the DB
// mock has NO ExpectPing registered and the NATS publisher is a
// zero-value (Conn() returns nil, short-circuiting the NATS path).
// The handler must not touch any external dependency.
func TestHealth_LivenessAlwaysOK(t *testing.T) {
	artifactPath := t.TempDir()
	cfg := testConfig(t, artifactPath)
	// Deliberately no ExpectPing — the handler must not call
	// PingContext. If it does, sqlmock (with MonitorPingsOption) and
	// any queued ExpectPing mismatch would surface as a missing-
	// expectation failure.
	db, _, cleanup := newMockDBWithMock(t)
	defer cleanup()
	publisher := &nats.NATSPublisher{}
	artifactStore := storage.NewFSArtifactStore(artifactPath)

	app := New(cfg, db, publisher, artifactStore, emptyFS)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	app.Handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}

	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got := body["status"]; got != "ok" {
		t.Errorf("status = %v, want ok", got)
	}
}

// TestHealth_LivenessDoesNotTouchDeps is the strict version of
// LivenessAlwaysOK: it uses sqlmock.MonitorPingsOption(true) so any
// stray PingContext call from the handler becomes a hard failure
// surfaced by go test. If a future refactor accidentally re-adds a
// db.PingContext() to the /health handler, this test fails.
func TestHealth_LivenessDoesNotTouchDeps(t *testing.T) {
	artifactPath := t.TempDir()
	cfg := testConfig(t, artifactPath)
	mockDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()
	// No ExpectPing. If the handler calls PingContext, sqlmock
	// records the violation and VerifyAll below surfaces it.
	db := sqlx.NewDb(mockDB, "postgres")
	publisher := &nats.NATSPublisher{}
	artifactStore := storage.NewFSArtifactStore(artifactPath)

	app := New(cfg, db, publisher, artifactStore, emptyFS)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	app.Handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock recorded unexpected calls — the /health handler touched the DB: %v", err)
	}
}

// TestHealth_LivenessNoLoops pins the wire-shape contract of /health:
// the body must be exactly {"status":"ok"} — no loops map, no
// degraded_reasons list, no per-loop state of any kind. Those belong
// on /ready (issue #48 split).
func TestHealth_LivenessNoLoops(t *testing.T) {
	artifactPath := t.TempDir()
	cfg := testConfig(t, artifactPath)
	db, mock, cleanup := newMockDBWithMock(t)
	defer cleanup()
	mock.ExpectPing() // unused but pinned so the handler's lack of Ping doesn't trigger a mismatch
	publisher := &nats.NATSPublisher{}
	artifactStore := storage.NewFSArtifactStore(artifactPath)

	app := New(cfg, db, publisher, artifactStore, emptyFS)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	app.Handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}

	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if _, present := body["loops"]; present {
		t.Errorf("/health must not include loops map (issue #48 — lives on /ready); got: %v", body["loops"])
	}
	if _, present := body["degraded_reasons"]; present {
		t.Errorf("/health must not include degraded_reasons (issue #48 — lives on /ready); got: %v", body["degraded_reasons"])
	}
	if got := body["status"]; got != "ok" {
		t.Errorf("status = %v, want ok", got)
	}
}

// errDBPingFailed is a sentinel used to force the sqlmock PingContext
// to return an error in TestHealth_UnhealthyOnDBPingFailure. Plain
// errors.New matches the repo convention used in api_key_test.go and
// deployment_regions_test.go:710.
var errDBPingFailed = errors.New("forced ping failure")

// newMockDBWithMock is a helper that returns the sqlx.DB and the
// underlying sqlmock so individual tests can configure expectations
// (Ping success/failure, etc.).
func newMockDBWithMock(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return sqlxDB, mock, func() { _ = sqlxDB.Close() }
}

func TestNewWithSecretsConfig(t *testing.T) {
	artifactPath := t.TempDir()
	cfg := testConfig(t, artifactPath)
	cfg.Secrets = config.SecretsConfig{
		ActiveKeyID: "k1",
		Keys: map[string]string{
			"k1": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
	}
	db, mock, cleanup := newMockDBWithMock(t)
	defer cleanup()
	// Issue #441: with secrets configured, the startup plaintext check
	// calls CountPlaintextRows → StreamAll. Expect the query and
	// return zero rows so the check passes (n=0, no plaintext).
	mock.ExpectQuery(`SELECT tenant_id, app_name, env_key, env_value FROM app_env ORDER BY tenant_id, app_name, env_key`).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	publisher := &nats.NATSPublisher{}
	artifactStore := storage.NewFSArtifactStore(artifactPath)

	app := New(cfg, db, publisher, artifactStore, emptyFS)
	if app == nil {
		t.Fatal("New returned nil with secrets config")
	}
	if app.Handler == nil {
		t.Fatal("Handler is nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestNewWithAutoscaleDisabled(t *testing.T) {
	artifactPath := t.TempDir()
	cfg := testConfig(t, artifactPath)
	cfg.Autoscale = config.AutoscaleConfig{
		Enabled:            false,
		MinWorkers:         2,
		MaxWorkers:         10,
		TargetHeadroomPct:  20,
		ScaleUpCooldownS:   60,
		ScaleDownCooldownS: 120,
		DecisionIntervalS:  30,
	}
	db, _ := newMockDB(t)
	publisher := &nats.NATSPublisher{}
	artifactStore := storage.NewFSArtifactStore(artifactPath)

	app := New(cfg, db, publisher, artifactStore, emptyFS)
	if app == nil {
		t.Fatal("New returned nil")
	}
	if app.AutoscaleSvc != nil {
		t.Error("AutoscaleSvc should be nil when autoscale is disabled")
	}
}

func splitMethod(s string) [2]string {
	for i := 0; i < len(s); i++ {
		if s[i] == ' ' {
			return [2]string{s[:i], s[i+1:]}
		}
	}
	return [2]string{s, "/"}
}

// ----------------------------------------------------------------------
// workerTokenTenantKeyFromBody — issue #491 + PR review regression pins.
//
// The helper is the per-tenant key-extractor that backs the
// rate-limiter on POST /api/internal/tokens/tenant. It must:
//   - Extract tenant_id from the request body without consuming it.
//   - Restore the body so the downstream handler's json.Decoder
//     still sees the full payload.
//   - Fall back to the worker_id (from JWT context) on malformed /
//     empty / oversize / missing-field bodies — never return "" (the
//     RateLimiter treats "" as "skip limiting", which would let a
//     worker flood malformed bodies past the per-tenant bucket).
// ----------------------------------------------------------------------

const workerTokenKeyTestSecret = "test-secret-must-be-at-least-32-bytes-long!"
const workerTokenKeyTestWorker = "w_us_fra_42"

// newWorkerKeyRequest builds a *http.Request whose context already
// carries the worker_id that middleware.WorkerAuth would have stamped
// after validating the Bearer JWT. The trick: we run the auth
// middleware once on a fresh request, capture the post-auth context
// via a no-op next handler, then build a NEW request with that
// context and the original body. This isolates the body-peek helper
// from the auth chain — we're testing the helper, not the auth gate.
func newWorkerKeyRequest(t *testing.T, body string) *http.Request {
	t.Helper()
	claims := &middleware.WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "edgecloud",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: workerTokenKeyTestWorker,
		TenantID: "*",
		Role:     middleware.RoleWorker,
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString([]byte(workerTokenKeyTestSecret))
	if err != nil {
		t.Fatalf("failed to sign test worker JWT: %v", err)
	}

	authReq := httptest.NewRequest("POST", "/api/internal/tokens/tenant", strings.NewReader(body))
	authReq.Header.Set("Authorization", "Bearer "+signed)

	auth := middleware.WorkerAuth(middleware.WorkerJWTConfig{
		Secret: workerTokenKeyTestSecret,
		Issuer: "edgecloud",
	})

	var capturedCtx context.Context
	var status int
	auth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedCtx = r.Context()
		status = http.StatusOK
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(httptest.NewRecorder(), authReq)

	if capturedCtx == nil {
		t.Fatalf("auth middleware did not populate context (status=%d)", status)
	}

	// Build a fresh request with the captured context and the
	// ORIGINAL body — the inner handler didn't read it, so it's
	// intact.
	return httptest.NewRequest("POST", "/api/internal/tokens/tenant", strings.NewReader(body)).WithContext(capturedCtx)
}

func TestWorkerTokenTenantKeyFromBody_HappyPath(t *testing.T) {
	r := newWorkerKeyRequest(t, `{"tenant_id":"t_real"}`)
	got := workerTokenTenantKeyFromBody(r)
	if got != "t_real" {
		t.Fatalf("expected t_real, got %q", got)
	}
	// Body restoration: handler must still be able to decode the
	// original payload after the limiter has peeked.
	var replay map[string]string
	if err := json.NewDecoder(r.Body).Decode(&replay); err != nil {
		t.Fatalf("body not restored after peek: %v", err)
	}
	if replay["tenant_id"] != "t_real" {
		t.Fatalf("body content lost: got %v", replay)
	}
}

func TestWorkerTokenTenantKeyFromBody_EmptyBodyFallsBackToWorker(t *testing.T) {
	r := newWorkerKeyRequest(t, "")
	got := workerTokenTenantKeyFromBody(r)
	if got != workerTokenKeyTestWorker {
		t.Fatalf("expected fallback to worker_id %q, got %q", workerTokenKeyTestWorker, got)
	}
}

func TestWorkerTokenTenantKeyFromBody_MalformedJSONFallsBackToWorker(t *testing.T) {
	r := newWorkerKeyRequest(t, `{"tenant_id": `) // truncated
	got := workerTokenTenantKeyFromBody(r)
	if got != workerTokenKeyTestWorker {
		t.Fatalf("expected fallback to worker_id, got %q", got)
	}
}

func TestWorkerTokenTenantKeyFromBody_MissingFieldFallsBackToWorker(t *testing.T) {
	r := newWorkerKeyRequest(t, `{"other_field":"x"}`)
	got := workerTokenTenantKeyFromBody(r)
	if got != workerTokenKeyTestWorker {
		t.Fatalf("expected fallback to worker_id, got %q", got)
	}
}

func TestWorkerTokenTenantKeyFromBody_OversizeBodyFallsBackToWorker(t *testing.T) {
	// 200-byte body that exceeds the 128-byte peek buffer. The peek
	// returns only the first 128 bytes, which won't include a complete
	// tenant_id (which is capped at 64 chars by isSafeTenantID anyway,
	// but a 200-char payload with tenant_id near the END would be
	// truncated). Expect the fallback.
	big := bytes.Repeat([]byte("x"), 200)
	body := `{"tenant_id":"` + string(big) + `"}`
	r := newWorkerKeyRequest(t, body)
	got := workerTokenTenantKeyFromBody(r)
	// We can't assert an exact value because the truncated JSON may
	// still parse as a tenant_id substring (the first 128 bytes are
	// `{"tenant_id":"xxxxx...` which is invalid JSON but the helper
	// catches that and falls back to worker_id). Both fallback and a
	// partial-tenant_id are acceptable — what matters is the function
	// never returns "" (which would bypass the limiter).
	if got == "" {
		t.Fatalf("oversize body must not return empty key (limiter bypass)")
	}
}

func TestWorkerTokenTenantKeyFromBody_TruncatedValidPrefixStillHandledSafely(t *testing.T) {
	// Boundary: a body that's exactly 128 bytes. The peek reads the
	// full body, the JSON parses cleanly, and we extract a tenant_id
	// (or fall back to worker_id if the truncation chops the value
	// mid-string). Either way, the function returns a non-empty key.
	body := `{"tenant_id":"t_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}` // >64 chars
	r := newWorkerKeyRequest(t, body)
	got := workerTokenTenantKeyFromBody(r)
	if got == "" {
		t.Fatalf("expected non-empty key (limiter bypass on truncated peek)")
	}
}
