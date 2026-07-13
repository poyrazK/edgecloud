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
// signing tests use `signing.TestKeyring(t)` (also all-zero seed) for
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

// withValidDBPassword sets DATABASE_PASSWORD to a known-valid value
// for the lifetime of a test. The config Load() validator added in
// issue #626 rejects empty / known-placeholder passwords; every
// existing test that builds a minimal config (without a `database:`
// block in the YAML body) needs this so the validator passes and the
// test reaches the field it actually cares about. The
// placeholder-rejection tests in this file explicitly clear the env
// var with their own t.Setenv("DATABASE_PASSWORD", "") to exercise the
// validator path.
func withValidDBPassword(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_PASSWORD", validSecret)
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	withSigningKey(t)
	withValidDBPassword(t)
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

// TestLoad_RejectsPlaceholderDBPasswords is the issue #626 regression
// for the database-password guard. Mirrors TestLoad_RejectsPlaceholderSecrets:
// iterate every entry in insecureDBPasswordValues and confirm each one
// produces a fail-closed error. The set must stay small and curated;
// adding an entry requires a code review so a typo doesn't accidentally
// invalidate a legitimate operator password.
func TestLoad_RejectsPlaceholderDBPasswords(t *testing.T) {
	for placeholder := range insecureDBPasswordValues {
		t.Run("placeholder="+placeholder, func(t *testing.T) {
			// Clear DATABASE_PASSWORD AFTER writeConfig runs so the helper's
			// default (validSecret) does not mask the YAML placeholder.
			// writeConfig sets EDGE_SIGNING_KEY_PATH and DATABASE_PASSWORD;
			// t.Setenv is LIFO-restored on test exit.
			body := "jwt:\n  secret: \"" + validSecret + "\"\n" +
				"database:\n  password: \"" + placeholder + "\"\n"
			path := writeConfig(t, body)
			t.Setenv("DATABASE_PASSWORD", "")

			_, err := Load(path)
			if err == nil {
				t.Fatalf("expected error for placeholder db password %q, got nil", placeholder)
			}
			if !strings.Contains(err.Error(), "placeholder") {
				t.Errorf("error %q should mention 'placeholder'", err.Error())
			}
		})
	}
}

// TestLoad_RejectsEmptyDBPassword pins the empty-string case for the
// database-password guard. Mirrors TestLoad_RejectsEmptySecret: the
// error message must mention "not set" and must NOT mention
// "placeholder", so operators can tell the two failure modes apart.
func TestLoad_RejectsEmptyDBPassword(t *testing.T) {
	body := "jwt:\n  secret: \"" + validSecret + "\"\n" +
		"database:\n  user: edgecloud\n  name: edgecloud\n"
	path := writeConfig(t, body)
	t.Setenv("DATABASE_PASSWORD", "")

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing database.password, got nil")
	}
	if !strings.Contains(err.Error(), "not set") {
		t.Errorf("error %q should mention 'not set'", err.Error())
	}
	if strings.Contains(err.Error(), "placeholder") {
		t.Errorf("error %q should NOT mention 'placeholder' for an empty db password", err.Error())
	}
}

// TestLoad_AcceptsValidDBPassword pins the happy path: a 32+ byte,
// non-placeholder password boots cleanly. Sized like the JWT-secret
// happy-path test for symmetry; the validator does not currently
// enforce a minimum length, but a strong default protects against
// single-character accidental-typo regressions.
func TestLoad_AcceptsValidDBPassword(t *testing.T) {
	want := strings.Repeat("p", 32)
	body := "jwt:\n  secret: \"" + validSecret + "\"\n" +
		"database:\n  password: \"" + want + "\"\n"
	path := writeConfig(t, body)
	// Clear DATABASE_PASSWORD so the YAML value (32 p's) is what Load sees;
	// writeConfig's helper sets it to validSecret to keep other tests
	// passing without a database block.
	t.Setenv("DATABASE_PASSWORD", "")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Database.Password != want {
		t.Errorf("Database.Password = %q, want %q", cfg.Database.Password, want)
	}
}

// TestLoad_DBPasswordEnvVarOverridesYAML pins the env-over-YAML
// precedence: a placeholder in YAML must be overridable via
// DATABASE_PASSWORD=… to a unique value, matching how operators
// transition off the dev default in `.env.example`.
func TestLoad_DBPasswordEnvVarOverridesYAML(t *testing.T) {
	envPassword := strings.Repeat("e", 32)

	body := "jwt:\n  secret: \"" + validSecret + "\"\n" +
		"database:\n  password: \"edgecloud\"\n"
	path := writeConfig(t, body)
	t.Setenv("DATABASE_PASSWORD", envPassword)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Database.Password != envPassword {
		t.Errorf("Database.Password = %q, want env value %q", cfg.Database.Password, envPassword)
	}
}

// TestLoad_RejectsShortDBPassword pins the 16-byte length floor added
// in the issue #626 review follow-up. Sibling to TestLoad_RejectsShortSecret:
// a non-placeholder password shorter than the minimum must fail with a
// message that mentions the byte count so operators can fix it.
func TestLoad_RejectsShortDBPassword(t *testing.T) {
	short := strings.Repeat("p", 15) // one short of the 16-byte minimum
	body := "jwt:\n  secret: \"" + validSecret + "\"\n" +
		"database:\n  password: \"" + short + "\"\n"
	path := writeConfig(t, body)
	t.Setenv("DATABASE_PASSWORD", "") // writeConfig's helper sets validSecret; clear so the YAML value reaches Load

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for 15-byte db password, got nil")
	}
	if !strings.Contains(err.Error(), "16 bytes") {
		t.Errorf("error %q should mention the 16-byte minimum", err.Error())
	}
}

// TestBundledConfig_FailsStartup_DBPassword is the issue #626
// regression guard for the bundled config.yaml. The file used to ship
// with `password: "edgecloud"` hardcoded; we now ship an empty string
// + comment, and the validator refuses the dev default. The bundled
// config also has the JWT placeholder (`change-me-in-production`),
// which trips the JWT check first — we accept either error message
// here. The NEW TestLoad_RejectsDBPasswordLiteral_Edgecloud below is
// the specific guard against re-introducing the literal `edgecloud`:
// it builds a synthetic config with a valid JWT secret and the literal
// `edgecloud` password, and pins the "placeholder" error.
func TestBundledConfig_FailsStartup_DBPassword(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Skip("runtime.Caller unavailable; cannot locate bundled config")
	}
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	configPath := filepath.Join(repoRoot, "config.yaml")

	if _, err := os.Stat(configPath); err != nil {
		t.Skipf("bundled config.yaml not found at %s (run from repo root): %v", configPath, err)
	}

	t.Setenv("DATABASE_PASSWORD", "")
	t.Setenv("JWT_SECRET", "")

	_, err := Load(configPath)
	if err == nil {
		t.Fatalf("Load(%s) succeeded; bundled config should be rejected", configPath)
	}
	// The bundled config has BOTH the empty DB password (now) and the
	// JWT placeholder; the JWT check fires first. Accept either signal
	// here — TestBundledConfig_FailsStartup pins the JWT "placeholder"
	// assertion, and TestLoad_RejectsEmptyDBPassword pins the
	// database-password "not set" assertion.
	if !strings.Contains(err.Error(), "placeholder") && !strings.Contains(err.Error(), "not set") {
		t.Errorf("error %q should mention either 'placeholder' (JWT) or 'not set' (database)", err.Error())
	}
}

// TestLoad_RejectsDBPasswordLiteral_Edgecloud is the SPECIFIC issue #626
// regression guard against re-introducing `password: "edgecloud"` into
// the bundled config.yaml. It builds a synthetic config with a valid
// JWT secret and the literal `edgecloud` DB password, and pins the
// validator's "placeholder" error. If this test starts passing, the
// validator's deny-list has lost the literal (or someone changed the
// bundled config back to the hardcoded dev default).
func TestLoad_RejectsDBPasswordLiteral_Edgecloud(t *testing.T) {
	// Synthetic config: valid JWT secret + literal edgecloud DB password.
	// Same env-var clear pattern as TestLoad_RejectsPlaceholderDBPasswords:
	// writeConfig's helper sets DATABASE_PASSWORD=validSecret; clear so
	// the YAML placeholder reaches Load.
	body := "jwt:\n  secret: \"" + validSecret + "\"\n" +
		"database:\n  password: \"edgecloud\"\n"
	path := writeConfig(t, body)
	t.Setenv("DATABASE_PASSWORD", "")

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load succeeded; literal 'edgecloud' DB password must be rejected")
	}
	if !strings.Contains(err.Error(), "placeholder") {
		t.Errorf("error %q should mention 'placeholder' (literal edgecloud regression)", err.Error())
	}
	// Specifically NOT the length error — the literal 'edgecloud' is 9
	// bytes, which is below the 16-byte minimum; without the deny-list
	// check firing first, the validator would still reject on length.
	// Either error is a valid fail, but the test's primary intent is
	// the deny-list check.
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

// TestLoad_AllowLegacyPlaintextEnv_DefaultsFalse pins the fail-closed
// default for issue #441: when EDGE_ALLOW_LEGACY_PLAINTEXT_ENV is unset
// (or any non-bool value), the field is false. Operators must opt in
// explicitly to the migration window.
func TestLoad_AllowLegacyPlaintextEnv_DefaultsFalse(t *testing.T) {
	t.Setenv("EDGE_ALLOW_LEGACY_PLAINTEXT_ENV", "")
	path := writeConfig(t, "jwt:\n  secret: \""+strings.Repeat("a", 32)+"\"\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Secrets.AllowLegacyPlaintextEnv {
		t.Errorf("AllowLegacyPlaintextEnv = true, want false (default)")
	}
}

// TestLoad_AllowLegacyPlaintextEnv_BindsTrue pins the env-var binding
// for EDGE_ALLOW_LEGACY_PLAINTEXT_ENV=true. Used at startup to relax
// the plaintext-row fail-fast during the migration window.
func TestLoad_AllowLegacyPlaintextEnv_BindsTrue(t *testing.T) {
	t.Setenv("EDGE_ALLOW_LEGACY_PLAINTEXT_ENV", "true")
	path := writeConfig(t, "jwt:\n  secret: \""+strings.Repeat("a", 32)+"\"\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Secrets.AllowLegacyPlaintextEnv {
		t.Errorf("AllowLegacyPlaintextEnv = false, want true (env opt-in)")
	}
}

// TestLoad_AllowLegacyPlaintextEnv_RejectsInvalid pins the parse guard:
// a non-bool value (e.g. "yes") must surface as a Load error so a
// typo doesn't silently turn into "false" or "true".
func TestLoad_AllowLegacyPlaintextEnv_RejectsInvalid(t *testing.T) {
	t.Setenv("EDGE_ALLOW_LEGACY_PLAINTEXT_ENV", "yes")
	path := writeConfig(t, "jwt:\n  secret: \""+strings.Repeat("a", 32)+"\"\n")
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load with EDGE_ALLOW_LEGACY_PLAINTEXT_ENV=yes should error")
	}
	if !strings.Contains(err.Error(), "EDGE_ALLOW_LEGACY_PLAINTEXT_ENV") {
		t.Errorf("error %q should mention the env var name", err.Error())
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

// TestLoad_Billing_NoopInDev_Accepted: the default-empty provider
// defaults to "noop" and is allowed when app.env is dev.
func TestLoad_Billing_NoopInDev_Accepted(t *testing.T) {
	t.Setenv("BILLING_PROVIDER", "")
	t.Setenv("STRIPE_SECRET_KEY", "")
	t.Setenv("STRIPE_WEBHOOK_SECRET", "")

	body := `
app:
  env: dev
` + validBaseYAML(t)
	path := writeConfig(t, body)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Billing.Provider != "noop" {
		t.Errorf("Billing.Provider = %q, want noop (defaulted)", cfg.Billing.Provider)
	}
}

// TestLoad_Billing_NoopInTest_Accepted: noop is also accepted in
// test environment (CI runs the binary with app.env=test).
func TestLoad_Billing_NoopInTest_Accepted(t *testing.T) {
	t.Setenv("BILLING_PROVIDER", "")
	body := `
app:
  env: test
` + validBaseYAML(t)
	path := writeConfig(t, body)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Billing.Provider != "noop" {
		t.Errorf("Billing.Provider = %q, want noop (defaulted)", cfg.Billing.Provider)
	}
}

// TestLoad_Billing_NoopInProd_Rejected: a noop provider in
// production would silently accept every checkout as "succeeded"
// without ever taking payment. Fail-closed.
func TestLoad_Billing_NoopInProd_Rejected(t *testing.T) {
	t.Setenv("BILLING_PROVIDER", "")
	body := `
app:
  env: production
` + validBaseYAML(t)
	path := writeConfig(t, body)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for noop in production, got nil")
	}
	if !strings.Contains(err.Error(), "noop") {
		t.Errorf("error %q should mention 'noop'", err.Error())
	}
}

// TestLoad_Billing_Stripe_MissingSecretKey: stripe without a
// secret_key is a hard fail — Deploy would issue unsigned artifacts
// and the webhook verifier would reject every delivery.
func TestLoad_Billing_Stripe_MissingSecretKey(t *testing.T) {
	t.Setenv("BILLING_PROVIDER", "stripe")
	t.Setenv("STRIPE_SECRET_KEY", "")
	t.Setenv("STRIPE_WEBHOOK_SECRET", "whsec_test")
	t.Setenv("STRIPE_PRICE_ID_PRO", "price_pro")
	t.Setenv("STRIPE_PRICE_ID_BUSINESS", "price_biz")
	t.Setenv("STRIPE_PRICE_ID_ENTERPRISE", "price_ent")
	body := `
app:
  env: production
` + validBaseYAML(t)
	path := writeConfig(t, body)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for stripe without secret_key, got nil")
	}
	if !strings.Contains(err.Error(), "secret_key") {
		t.Errorf("error %q should mention 'secret_key'", err.Error())
	}
}

// TestLoad_Billing_Stripe_MissingWebhookSecret: stripe without a
// webhook_secret means the verifier can't check signatures and
// every delivery would 400. Hard fail.
func TestLoad_Billing_Stripe_MissingWebhookSecret(t *testing.T) {
	t.Setenv("BILLING_PROVIDER", "stripe")
	t.Setenv("STRIPE_SECRET_KEY", "sk_test_abc")
	t.Setenv("STRIPE_WEBHOOK_SECRET", "")
	t.Setenv("STRIPE_PRICE_ID_PRO", "price_pro")
	t.Setenv("STRIPE_PRICE_ID_BUSINESS", "price_biz")
	t.Setenv("STRIPE_PRICE_ID_ENTERPRISE", "price_ent")
	body := `
app:
  env: production
` + validBaseYAML(t)
	path := writeConfig(t, body)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for stripe without webhook_secret, got nil")
	}
	if !strings.Contains(err.Error(), "webhook_secret") {
		t.Errorf("error %q should mention 'webhook_secret'", err.Error())
	}
}

// TestLoad_Billing_Stripe_MissingPriceIDs: every paid plan must
// have a price_id. "free" is exempt because the checkout handler
// rejects it before any merchant call.
func TestLoad_Billing_Stripe_MissingPriceIDs(t *testing.T) {
	t.Setenv("BILLING_PROVIDER", "stripe")
	t.Setenv("STRIPE_SECRET_KEY", "sk_test_abc")
	t.Setenv("STRIPE_WEBHOOK_SECRET", "whsec_test")
	t.Setenv("STRIPE_PRICE_ID_PRO", "price_pro")
	// business and enterprise deliberately missing.
	t.Setenv("STRIPE_PRICE_ID_BUSINESS", "")
	t.Setenv("STRIPE_PRICE_ID_ENTERPRISE", "")
	body := `
app:
  env: production
` + validBaseYAML(t)
	path := writeConfig(t, body)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing price_ids, got nil")
	}
	if !strings.Contains(err.Error(), "price_ids") {
		t.Errorf("error %q should mention 'price_ids'", err.Error())
	}
}

// TestLoad_Billing_Stripe_FullyConfigured_Accepted: the happy
// path — every required field set, validator passes.
func TestLoad_Billing_Stripe_FullyConfigured_Accepted(t *testing.T) {
	t.Setenv("BILLING_PROVIDER", "stripe")
	t.Setenv("STRIPE_SECRET_KEY", "sk_test_abc")
	t.Setenv("STRIPE_WEBHOOK_SECRET", "whsec_test")
	t.Setenv("STRIPE_PUBLISHABLE_KEY", "pk_test_abc")
	t.Setenv("STRIPE_PRICE_ID_PRO", "price_pro")
	t.Setenv("STRIPE_PRICE_ID_BUSINESS", "price_biz")
	t.Setenv("STRIPE_PRICE_ID_ENTERPRISE", "price_ent")
	body := `
app:
  env: production
billing:
  provider: stripe
  success_url: https://app.example.com/billing/success
  cancel_url: https://app.example.com/billing/cancel
` + validBaseYAML(t)
	path := writeConfig(t, body)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Billing.Provider != "stripe" {
		t.Errorf("Billing.Provider = %q, want stripe", cfg.Billing.Provider)
	}
	if cfg.Billing.Stripe.SecretKey != "sk_test_abc" {
		t.Errorf("Billing.Stripe.SecretKey = %q, want sk_test_abc", cfg.Billing.Stripe.SecretKey)
	}
	if got := cfg.Billing.Stripe.PriceIDs["pro"]; got != "price_pro" {
		t.Errorf("PriceIDs[pro] = %q, want price_pro", got)
	}
	if cfg.Billing.SuccessURL != "https://app.example.com/billing/success" {
		t.Errorf("SuccessURL = %q", cfg.Billing.SuccessURL)
	}
}

// TestLoad_Billing_UnknownProvider_Rejected: a typo in the
// provider name should not silently fall through to noop.
func TestLoad_Billing_UnknownProvider_Rejected(t *testing.T) {
	t.Setenv("BILLING_PROVIDER", "stripes") // typo
	body := `
app:
  env: dev
` + validBaseYAML(t)
	path := writeConfig(t, body)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for unknown provider, got nil")
	}
	if !strings.Contains(err.Error(), "not recognized") {
		t.Errorf("error %q should mention 'not recognized'", err.Error())
	}
}

// validBaseYAML returns a YAML string with the minimum fields every
// Load() call needs: a non-empty JWT secret and a signing key
// (the latter is wired via t.Setenv, not YAML). Used by the
// billing tests to keep the per-test YAML focused on the field
// under test.
func validBaseYAML(t *testing.T) string {
	t.Helper()
	return `
jwt:
  secret: "` + validSecret + `"
  ttl_hours: 24
  issuer: edgecloud
`
}
