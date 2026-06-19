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

// minimalConfigYAML is a small but valid config.yaml fixture. Tests override
// the jwt.secret as needed.
const minimalConfigYAML = `
jwt:
  secret: "` + validSecret + `"
  ttl_hours: 24
  issuer: edgecloud
`

func writeConfig(t *testing.T, body string) string {
	t.Helper()
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
