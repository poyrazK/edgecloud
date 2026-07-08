package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// validSecret is a 32-byte secret we use in tests that need a non-placeholder value.
const validSecret = "this-is-a-32-byte-test-secret-x!"

// testSigningKeyPath is the absolute path to a fixture signing key
// file (issue #307). Every test that calls Load must set
// EDGE_SIGNING_KEY_PATH to this path so the new `signing` validator
// doesn't reject the config; the validator requires a key, but the
// test's only goal is exercising a different config field. The key
// contents themselves are never decoded by these tests — they just
// need the path to point at a parseable file.
//
// The fixture file is generated on first run by [ensureTestSigningKey]
// and is .gitignored (testdata/*.key) so every checkout gets a
// fresh random key. The key is meant to exercise the file-loading
// path; cryptographic content doesn't matter. Compute the absolute
// path once so tests work regardless of which directory `go test`
// runs from.
var testSigningKeyPath = func() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("runtime.Caller failed in config_test.go")
	}
	return filepath.Join(filepath.Dir(file), "testdata", "test_signing.key")
}()

// ensureTestSigningKey writes a deterministic 32-byte key to
// testdata/test_signing.key if the file is missing. Deterministic
// (all-zeros) because the test fixture only validates the file
// *loading* path — there is no cryptographic material in play; the
// signing tests use `signing.TestKey(t)` (also all-zero seed) for
// their own assertions.
func ensureTestSigningKey(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(testSigningKeyPath); err == nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(testSigningKeyPath), 0o700); err != nil {
		t.Fatalf("create testdata dir: %v", err)
	}
	if err := os.WriteFile(testSigningKeyPath, make([]byte, 32), 0o600); err != nil {
		t.Fatalf("write fixture signing key: %v", err)
	}
}

// minimalConfigYAML is a small but valid config.yaml fixture. Tests override
// the jwt.secret as needed.
const minimalConfigYAML = `
jwt:
  secret: "` + validSecret + `"
  ttl_hours: 24
  issuer: edgecloud
`

// withSigningKey sets EDGE_SIGNING_KEY_PATH for the lifetime of a
// test. The config Load() validator requires a signing key (issue
// #307); every test that calls Load must call this so the validator
// passes and the test reaches the field it actually cares about.
// Done via t.Setenv so the env is restored when the test ends.
func withSigningKey(t *testing.T) {
	t.Helper()
	ensureTestSigningKey(t)
	t.Setenv("EDGE_SIGNING_KEY_PATH", testSigningKeyPath)
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	withSigningKey(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoad_RejectsPlaceholderSecrets(t *testing.T) {
	// Snapshot the map so we exercise every entry. The map is package-level
	// and must not be mutated by tests.
	for placeholder := range insecureJWTSecretValues {
		t.Run("placeholder="+placeholder, func(t *testing.T) {
			// Clear any JWT_SECRET from the surrounding environment so the
			// YAML value is what Load sees.
			t.Setenv("JWT_SECRET", "")

			body := "jwt:\n  secret: \"" + placeholder + "\"\n"
			path := writeConfig(t, body)

			_, err := Load(path)
			if err == nil {
				t.Fatalf("expected error for placeholder secret %q, got nil", placeholder)
			}
			if !strings.Contains(err.Error(), "placeholder") {
				t.Errorf("error %q should mention 'placeholder'", err.Error())
			}
		})
	}
}

func TestLoad_RejectsEmptySecret(t *testing.T) {
	t.Setenv("JWT_SECRET", "")

	// A YAML block with jwt: but no secret at all (or an explicit empty
	// string) must fail with a clear "not set" message — not be lumped
	// in with the "known placeholder" category.
	body := "jwt:\n  ttl_hours: 24\n  issuer: edgecloud\n"
	path := writeConfig(t, body)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing jwt.secret, got nil")
	}
	if !strings.Contains(err.Error(), "not set") {
		t.Errorf("error %q should mention 'not set'", err.Error())
	}
	if strings.Contains(err.Error(), "placeholder") {
		t.Errorf("error %q should NOT mention 'placeholder' for an empty secret", err.Error())
	}
}

func TestLoad_RejectsShortSecret(t *testing.T) {
	t.Setenv("JWT_SECRET", "")

	// 31 ASCII bytes — one short of the minimum.
	short := strings.Repeat("a", 31)
	body := "jwt:\n  secret: \"" + short + "\"\n"
	path := writeConfig(t, body)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for 31-byte secret, got nil")
	}
	if !strings.Contains(err.Error(), "32 bytes") {
		t.Errorf("error %q should mention the 32-byte minimum", err.Error())
	}
}

func TestLoad_AcceptsValidSecret(t *testing.T) {
	t.Setenv("JWT_SECRET", "")

	path := writeConfig(t, minimalConfigYAML)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.JWT.Secret != validSecret {
		t.Errorf("JWT.Secret = %q, want %q", cfg.JWT.Secret, validSecret)
	}
}

func TestLoad_EnvVarOverridesYAML(t *testing.T) {
	// YAML contains a different (but also valid) secret; the env var must win.
	yamlSecret := strings.Repeat("y", 32)
	envSecret := strings.Repeat("e", 32)

	body := "jwt:\n  secret: \"" + yamlSecret + "\"\n"
	path := writeConfig(t, body)

	t.Setenv("JWT_SECRET", envSecret)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.JWT.Secret != envSecret {
		t.Errorf("JWT.Secret = %q, want env value %q", cfg.JWT.Secret, envSecret)
	}
}

// TestBundledConfig_FailsStartup is a regression guard: the config.yaml
// shipped at the repo root intentionally contains the `change-me-in-production`
// placeholder. After the JWT-secret validation landed (Finding 5), a fresh
// `cp config.yaml . && ./edge-control-plane` must refuse to boot rather than
// silently use the placeholder. If this test starts passing (or stops finding
// config.yaml), the validation or the file has drifted from intent.
func TestBundledConfig_FailsStartup(t *testing.T) {
	// Resolve the repo root from this test file's location: ../../
	// relative to internal/config/config_test.go. Using runtime.Caller is
	// robust against `go test` being invoked from any working directory.
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Skip("runtime.Caller unavailable; cannot locate bundled config")
	}
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	configPath := filepath.Join(repoRoot, "config.yaml")

	if _, err := os.Stat(configPath); err != nil {
		t.Skipf("bundled config.yaml not found at %s (run from repo root): %v", configPath, err)
	}

	t.Setenv("JWT_SECRET", "")

	_, err := Load(configPath)
	if err == nil {
		t.Fatalf("Load(%s) succeeded; bundled config should be rejected due to placeholder jwt.secret", configPath)
	}
	if !strings.Contains(err.Error(), "placeholder") {
		t.Errorf("error %q should mention 'placeholder'", err.Error())
	}
}

// ---------------------------------------------------------------------------
// Storage backend selection — issue #127 (cross-region artifact replication)
//
// Load() runs validateStorageConfig after env-var overrides. These tests
// pin the per-backend required-field matrix so a future relaxation
// (e.g. dropping the remote token check) is caught by CI rather than
// reaching production.
// ---------------------------------------------------------------------------

// TestLoad_StorageBackend_DefaultsToFS pins the backwards-compat
// behavior: a config block with only `artifact_path` set (the existing
// shipped config) is interpreted as the FS backend with no error.
// Removing this contract would silently break every existing deployment.
func TestLoad_StorageBackend_DefaultsToFS(t *testing.T) {
	t.Setenv("JWT_SECRET", "")
	t.Setenv("STORAGE_ARTIFACT_BACKEND", "")

	body := `
jwt:
  secret: "` + validSecret + `"
storage:
  artifact_path: "/tmp/artifacts"
`
	cfg, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("Load with default FS backend: %v", err)
	}
	if cfg.Storage.ArtifactBackend != "" {
		t.Errorf("ArtifactBackend = %q, want empty (defaults to fs)", cfg.Storage.ArtifactBackend)
	}
	if cfg.Storage.ArtifactPath != "/tmp/artifacts" {
		t.Errorf("ArtifactPath = %q, want %q", cfg.Storage.ArtifactPath, "/tmp/artifacts")
	}
}

// TestLoad_StorageBackend_AcceptsExplicitFS pins that selecting "fs"
// explicitly is equivalent to leaving the field empty. The factory
// treats both as FSArtifactStore.
func TestLoad_StorageBackend_AcceptsExplicitFS(t *testing.T) {
	t.Setenv("JWT_SECRET", "")
	t.Setenv("STORAGE_ARTIFACT_BACKEND", "")

	body := `
jwt:
  secret: "` + validSecret + `"
storage:
  artifact_backend: "fs"
  artifact_path: "/tmp/artifacts"
`
	_, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("Load with explicit fs backend: %v", err)
	}
}

// TestLoad_StorageBackend_RejectsUnknown pins the typo-guard: an
// unrecognized backend name fails startup rather than silently
// falling through to fs. A typo like "s33" or "remote-storage" would
// otherwise land the operator on the filesystem backend without
// warning.
func TestLoad_StorageBackend_RejectsUnknown(t *testing.T) {
	t.Setenv("JWT_SECRET", "")

	body := `
jwt:
  secret: "` + validSecret + `"
storage:
  artifact_backend: "s33"
`
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("expected error for unknown backend, got nil")
	}
	if !strings.Contains(err.Error(), "s33") {
		t.Errorf("error %q should mention the offending backend name 's33'", err.Error())
	}
	if !strings.Contains(err.Error(), "not a recognized backend") {
		t.Errorf("error %q should mention 'not a recognized backend'", err.Error())
	}
}

// TestLoad_StorageBackend_S3 covers the S3 happy path: bucket + region
// in YAML, no env-var overrides. This is the contract the S3ArtifactStore
// constructor relies on at startup.
func TestLoad_StorageBackend_S3(t *testing.T) {
	t.Setenv("JWT_SECRET", "")
	t.Setenv("STORAGE_ARTIFACT_BACKEND", "")

	body := `
jwt:
  secret: "` + validSecret + `"
storage:
  artifact_backend: "s3"
  s3_bucket: "my-wasm-bucket"
  s3_region: "us-east-1"
  s3_endpoint: "http://localhost:9000"
  s3_path_style: true
  s3_key_prefix: "tenants/"
`
	cfg, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("Load with S3 backend: %v", err)
	}
	if cfg.Storage.ArtifactBackend != "s3" {
		t.Errorf("ArtifactBackend = %q, want %q", cfg.Storage.ArtifactBackend, "s3")
	}
	if cfg.Storage.S3Bucket != "my-wasm-bucket" {
		t.Errorf("S3Bucket = %q, want %q", cfg.Storage.S3Bucket, "my-wasm-bucket")
	}
	if cfg.Storage.S3PathStyle != true {
		t.Error("S3PathStyle = false, want true (minio)")
	}
	if cfg.Storage.S3KeyPrefix != "tenants/" {
		t.Errorf("S3KeyPrefix = %q, want %q", cfg.Storage.S3KeyPrefix, "tenants/")
	}
}

// TestLoad_StorageBackend_S3_RequiresBucket pins the missing-bucket
// error. The S3 constructor would also reject this, but Load() catches
// it earlier so the operator gets a config-validation error at startup
// rather than a runtime error on first deploy.
func TestLoad_StorageBackend_S3_RequiresBucket(t *testing.T) {
	t.Setenv("JWT_SECRET", "")

	body := `
jwt:
  secret: "` + validSecret + `"
storage:
  artifact_backend: "s3"
  s3_region: "us-east-1"
`
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("expected error for s3 backend without bucket, got nil")
	}
	if !strings.Contains(err.Error(), "s3_bucket") {
		t.Errorf("error %q should mention 's3_bucket'", err.Error())
	}
}

// TestLoad_StorageBackend_S3_RequiresRegion pins the missing-region
// error. S3 SDK requires a region even for custom endpoints (minio).
func TestLoad_StorageBackend_S3_RequiresRegion(t *testing.T) {
	t.Setenv("JWT_SECRET", "")

	body := `
jwt:
  secret: "` + validSecret + `"
storage:
  artifact_backend: "s3"
  s3_bucket: "my-wasm-bucket"
`
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("expected error for s3 backend without region, got nil")
	}
	if !strings.Contains(err.Error(), "s3_region") {
		t.Errorf("error %q should mention 's3_region'", err.Error())
	}
}

// TestLoad_StorageBackend_Remote covers the Remote happy path: all
// three required fields (peer URL, peer token, local cache dir) set
// in YAML.
func TestLoad_StorageBackend_Remote(t *testing.T) {
	t.Setenv("JWT_SECRET", "")
	t.Setenv("STORAGE_ARTIFACT_BACKEND", "")

	body := `
jwt:
  secret: "` + validSecret + `"
storage:
  artifact_backend: "remote"
  artifact_path: "/var/cache/edgecloud"
  peer_control_plane_url: "https://cp-us-east.edgecloud.example"
  peer_control_plane_internal_token: "shared-secret-at-least-thirty-two-bytes-long"
`
	cfg, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("Load with Remote backend: %v", err)
	}
	if cfg.Storage.PeerControlPlaneURL != "https://cp-us-east.edgecloud.example" {
		t.Errorf("PeerControlPlaneURL = %q, want cp-us-east URL", cfg.Storage.PeerControlPlaneURL)
	}
	if cfg.Storage.PeerControlPlaneInternalToken == "" {
		t.Error("PeerControlPlaneInternalToken is empty after Load")
	}
}

// TestLoad_StorageBackend_Remote_RequiresURL pins the missing-URL error.
func TestLoad_StorageBackend_Remote_RequiresURL(t *testing.T) {
	t.Setenv("JWT_SECRET", "")

	body := `
jwt:
  secret: "` + validSecret + `"
storage:
  artifact_backend: "remote"
  artifact_path: "/var/cache/edgecloud"
  peer_control_plane_internal_token: "shared-secret-at-least-thirty-two-bytes-long"
`
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("expected error for remote backend without URL, got nil")
	}
	if !strings.Contains(err.Error(), "peer_control_plane_url") {
		t.Errorf("error %q should mention 'peer_control_plane_url'", err.Error())
	}
}

// TestLoad_StorageBackend_Remote_RequiresToken pins the fail-closed
// token rule. An empty PeerControlPlaneInternalToken would let the
// RemoteArtifactStore present an empty X-Internal-Token to the peer,
// and the peer's middleware (which rejects empty tokens) would 401 —
// but better to reject at this CP's startup so the operator gets a
// clear config error rather than a per-deploy 502.
func TestLoad_StorageBackend_Remote_RequiresToken(t *testing.T) {
	t.Setenv("JWT_SECRET", "")

	body := `
jwt:
  secret: "` + validSecret + `"
storage:
  artifact_backend: "remote"
  artifact_path: "/var/cache/edgecloud"
  peer_control_plane_url: "https://cp-us-east.edgecloud.example"
`
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("expected error for remote backend without token, got nil")
	}
	if !strings.Contains(err.Error(), "peer_control_plane_internal_token") {
		t.Errorf("error %q should mention 'peer_control_plane_internal_token'", err.Error())
	}
	if !strings.Contains(err.Error(), "fail-closed") {
		t.Errorf("error %q should mention 'fail-closed'", err.Error())
	}
}

// TestLoad_StorageBackend_Remote_RequiresArtifactPath pins the
// missing-cache-dir rule. The remote backend wraps an FSArtifactStore
// for the local cache; without a path it can't construct one.
func TestLoad_StorageBackend_Remote_RequiresArtifactPath(t *testing.T) {
	t.Setenv("JWT_SECRET", "")

	body := `
jwt:
  secret: "` + validSecret + `"
storage:
  artifact_backend: "remote"
  peer_control_plane_url: "https://cp-us-east.edgecloud.example"
  peer_control_plane_internal_token: "shared-secret-at-least-thirty-two-bytes-long"
`
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("expected error for remote backend without artifact_path, got nil")
	}
	if !strings.Contains(err.Error(), "artifact_path") {
		t.Errorf("error %q should mention 'artifact_path'", err.Error())
	}
}

// TestLoad_StorageBackend_EnvVarOverrides pins that STORAGE_*
// env vars override YAML values. Mirrors the JWT_SECRET override test
// at the top of this file; kept separate so a regression in the new
// env-var handlers is caught even if the JWT ones pass.
func TestLoad_StorageBackend_EnvVarOverrides(t *testing.T) {
	// Reset every env var this test exercises so neither the host
	// environment nor a prior sub-test leaks into it.
	for _, k := range []string{
		"JWT_SECRET", "STORAGE_ARTIFACT_BACKEND",
		"STORAGE_S3_BUCKET", "STORAGE_S3_REGION",
		"STORAGE_S3_ENDPOINT", "STORAGE_S3_PATH_STYLE",
		"STORAGE_PEER_CONTROL_PLANE_URL",
	} {
		t.Setenv(k, "")
	}

	body := `
jwt:
  secret: "` + validSecret + `"
storage:
  artifact_backend: "fs"
  artifact_path: "/yaml/path"
`
	t.Setenv("STORAGE_ARTIFACT_BACKEND", "s3")
	t.Setenv("STORAGE_S3_BUCKET", "env-bucket")
	t.Setenv("STORAGE_S3_REGION", "eu-west-1")
	t.Setenv("STORAGE_S3_ENDPOINT", "https://env.example")
	t.Setenv("STORAGE_S3_PATH_STYLE", "true")

	cfg, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("Load with env-var overrides: %v", err)
	}
	if cfg.Storage.ArtifactBackend != "s3" {
		t.Errorf("ArtifactBackend = %q, want %q (env override)", cfg.Storage.ArtifactBackend, "s3")
	}
	if cfg.Storage.S3Bucket != "env-bucket" {
		t.Errorf("S3Bucket = %q, want %q (env override)", cfg.Storage.S3Bucket, "env-bucket")
	}
	if cfg.Storage.S3Region != "eu-west-1" {
		t.Errorf("S3Region = %q, want %q (env override)", cfg.Storage.S3Region, "eu-west-1")
	}
	if cfg.Storage.S3Endpoint != "https://env.example" {
		t.Errorf("S3Endpoint = %q, want env override", cfg.Storage.S3Endpoint)
	}
	if cfg.Storage.S3PathStyle != true {
		t.Error("S3PathStyle = false, want true (env override)")
	}
	if cfg.Storage.ArtifactPath != "/yaml/path" {
		t.Errorf("ArtifactPath = %q, want %q (not env-overridden)", cfg.Storage.ArtifactPath, "/yaml/path")
	}
}

// TestLoad_StorageBackend_S3PathStyle_RejectsInvalid pins the bool
// parse guard. A typo in STORAGE_S3_PATH_STYLE (e.g. "yes") should
// fail at startup rather than silently default to false (which would
// break a minio deployment that depends on path-style URLs).
func TestLoad_StorageBackend_S3PathStyle_RejectsInvalid(t *testing.T) {
	t.Setenv("JWT_SECRET", "")
	t.Setenv("STORAGE_S3_PATH_STYLE", "yes")

	body := `
jwt:
  secret: "` + validSecret + `"
storage:
  artifact_backend: "s3"
  s3_bucket: "b"
  s3_region: "r"
`
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("expected error for invalid STORAGE_S3_PATH_STYLE, got nil")
	}
	if !strings.Contains(err.Error(), "STORAGE_S3_PATH_STYLE") {
		t.Errorf("error %q should mention 'STORAGE_S3_PATH_STYLE'", err.Error())
	}
}
