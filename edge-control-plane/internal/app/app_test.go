package app

import (
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
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
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

// TestHealth_HealthyReturns200 verifies the default /health response
// shape: 200 + status=ok + loops map populated.
func TestHealth_HealthyReturns200(t *testing.T) {
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

// TestHealth_DegradedAfterLoopPanic verifies the new behavior: any loop
// with panics>0 (or stale) flips status to "degraded" but stays 200 so
// load balancers don't pull the CP from rotation.
func TestHealth_DegradedAfterLoopPanic(t *testing.T) {
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
		panic("forced for /health degraded test")
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
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
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

// TestHealth_UnhealthyOnDBPingFailure preserves the existing 503
// behavior: a failed DB ping must still produce status=unhealthy so
// load balancers pull this instance from rotation.
func TestHealth_UnhealthyOnDBPingFailure(t *testing.T) {
	artifactPath := t.TempDir()
	cfg := testConfig(t, artifactPath)
	db, mock, cleanup := newMockDBWithMock(t)
	defer cleanup()
	mock.ExpectPing().WillReturnError(errDBPingFailed)
	publisher := &nats.NATSPublisher{}
	artifactStore := storage.NewFSArtifactStore(artifactPath)

	app := New(cfg, db, publisher, artifactStore, emptyFS)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
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
	if body["loops"] != nil {
		t.Errorf("unhealthy response should not include loops map, got: %v", body["loops"])
	}
}

// TestHealth_BackwardCompatibleStatusCode verifies that the response
// shape on the healthy path still includes status="ok" as a string
// field (so any operator script that grep'd for "ok" still works).
func TestHealth_BackwardCompatibleStatusCode(t *testing.T) {
	artifactPath := t.TempDir()
	cfg := testConfig(t, artifactPath)
	db, mock, cleanup := newMockDBWithMock(t)
	defer cleanup()
	mock.ExpectPing()
	publisher := &nats.NATSPublisher{}
	artifactStore := storage.NewFSArtifactStore(artifactPath)

	app := New(cfg, db, publisher, artifactStore, emptyFS)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	app.Handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"status":"ok"`) {
		t.Errorf("body missing status=ok: %s", rr.Body.String())
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
	db, _ := newMockDB(t)
	publisher := &nats.NATSPublisher{}
	artifactStore := storage.NewFSArtifactStore(artifactPath)

	app := New(cfg, db, publisher, artifactStore, emptyFS)
	if app == nil {
		t.Fatal("New returned nil with secrets config")
	}
	if app.Handler == nil {
		t.Fatal("Handler is nil")
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
