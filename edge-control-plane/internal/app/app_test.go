package app

import (
	"context"
	"embed"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
