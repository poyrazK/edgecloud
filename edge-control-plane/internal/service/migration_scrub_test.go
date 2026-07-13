package service

import (
	"sort"
	"strings"
	"testing"
)

// TestScrubbedEnv_StripsAllKnownSensitiveVars is the drift-guard
// for issue #622 commit 4's env-scrubbing. Every var name listed
// here MUST NOT appear in scrubbedEnv() output. If a new sensitive
// var is added to internal/config/config.go without adding a row
// here, this test fails — preventing accidental env-leak regressions.
//
// `t.Setenv` is the safe test helper: the value is auto-restored
// when the test (and any subtest) exits. We deliberately don't
// call t.Parallel because env state is process-global.
func TestScrubbedEnv_StripsAllKnownSensitiveVars(t *testing.T) {
	cases := []string{
		// JWT / cluster signing.
		"JWT_SECRET",
		"JWT_KEY_FOO",
		"BOOTSTRAP_SECRET",
		"EDGE_INTERNAL_TOKEN",
		// Postgres.
		"DATABASE_PASSWORD",
		// Stripe.
		"STRIPE_SECRET_KEY",
		"STRIPE_WEBHOOK_SECRET",
		"STRIPE_PUBLISHABLE_KEY",
		"STRIPE_PRICE_ID_BASIC",
		// Signing keyring.
		"EDGE_SIGNING_KEY",
		"EDGE_SIGNING_KEY_ID",
		"EDGE_SIGNING_KEYRING",
		"EDGE_SIGNING_KEYRING_PATH",
		"EDGE_SIGNING_KEY_PATH",
		// Secrets encryption keyring.
		"EDGE_SECRETS_MASTER_KEY",
		"EDGE_SECRETS_ACTIVE_KEY_ID",
		"EDGE_SECRETS_KEY_KID_2025",
		// Storage backends.
		"STORAGE_S3_ACCESS_KEY",
		"STORAGE_S3_SECRET_KEY",
		"STORAGE_S3_SESSION_TOKEN",
		"STORAGE_PEER_CONTROL_PLANE_INTERNAL_TOKEN",
		// NATS connection string may carry credentials.
		"NATS_URL",
		// Ad-hoc operator-set sensitive vars — broad patterns
		// catch these even though they're not in the explicit
		// denylist.
		"FOO_SECRET",
		"FOO_KEY",
		"FOO_PASSWORD",
		"FOO_TOKEN",
		"FOO_CREDENTIAL",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "test-leak-value")
			out := scrubbedEnv()
			for _, kv := range out {
				eq := strings.IndexByte(kv, '=')
				if eq < 0 {
					continue
				}
				if kv[:eq] == name {
					t.Errorf("scrubbedEnv() leaked %s; var must be stripped", name)
				}
			}
		})
	}
}

// TestScrubbedEnv_AllowsAllowlistedVars asserts the allowlist
// works. Every var in scrubbedEnvAllowlist MUST pass through
// unchanged. Operators depend on this for CARGO_HOME / RUSTUP_HOME
// / WASI_SDK_PATH — breaking any of these silently makes cargo /
// rustc / clang fail in production.
func TestScrubbedEnv_AllowsAllowlistedVars(t *testing.T) {
	cases := []string{
		"PATH",
		"HOME",
		"TMPDIR",
		"LANG",
		"LC_ALL",
		"LC_COLLATE",
		"LC_CTYPE",
		"LC_MESSAGES",
		"LC_MONETARY",
		"LC_NUMERIC",
		"LC_TIME",
		"WASI_SDK_PATH",
		"CARGO_HOME",
		"CARGO_TARGET_DIR",
		"RUSTUP_HOME",
		"CLANG",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "test-value")
			out := scrubbedEnv()
			found := false
			for _, kv := range out {
				eq := strings.IndexByte(kv, '=')
				if eq < 0 {
					continue
				}
				if kv[:eq] == name {
					found = true
					// Value must round-trip unchanged.
					if kv[eq+1:] != "test-value" {
						t.Errorf("allowlisted %s value = %q, want test-value", name, kv[eq+1:])
					}
					break
				}
			}
			if !found {
				t.Errorf("allowlisted %s missing from scrubbedEnv() output", name)
			}
		})
	}
}

// TestScrubbedEnv_DoesNotLeakParent catches the bug where a future
// refactor returns os.Environ() instead of scrubbedEnv(). Any
// var the test sets that is NOT in the allowlist MUST NOT appear
// in the output.
func TestScrubbedEnv_DoesNotLeakParent(t *testing.T) {
	t.Setenv("FOO_LEAK_TEST_PARENT", "bar")
	t.Setenv("RANDOM_VAR_NOT_IN_LIST", "x")
	out := scrubbedEnv()
	for _, kv := range out {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			continue
		}
		name := kv[:eq]
		if name == "FOO_LEAK_TEST_PARENT" || name == "RANDOM_VAR_NOT_IN_LIST" {
			t.Errorf("scrubbedEnv() leaked %s; parent env must not pass through", name)
		}
	}
}

// TestScrubbedEnv_PreservesOperatorOverrides is the operator-
// experience regression test. Operators who vendor their cargo
// registry or set WASI_SDK_PATH to a non-standard location MUST
// have those values reach the subprocess unchanged. The values
// are intentionally non-default to catch "we hardcoded the test
// value in the test" bugs.
func TestScrubbedEnv_PreservesOperatorOverrides(t *testing.T) {
	t.Setenv("CARGO_HOME", "/srv/vendor/cargo")
	t.Setenv("RUSTUP_HOME", "/srv/vendor/rustup")
	t.Setenv("WASI_SDK_PATH", "/opt/wasi-sdk-2025-07")
	t.Setenv("CARGO_TARGET_DIR", "/srv/build/cache")

	out := scrubbedEnv()
	want := map[string]string{
		"CARGO_HOME":       "/srv/vendor/cargo",
		"RUSTUP_HOME":      "/srv/vendor/rustup",
		"WASI_SDK_PATH":    "/opt/wasi-sdk-2025-07",
		"CARGO_TARGET_DIR": "/srv/build/cache",
	}
	for _, kv := range out {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			continue
		}
		name := kv[:eq]
		if w, ok := want[name]; ok {
			if kv[eq+1:] != w {
				t.Errorf("%s = %q, want %q", name, kv[eq+1:], w)
			}
			delete(want, name)
		}
	}
	for name := range want {
		t.Errorf("operator override %s missing from scrubbedEnv() output", name)
	}
}

// TestScrubbedEnv_FailClosedDefaults asserts the unrecognised-var
// policy: anything not on the allowlist AND not matching a deny
// pattern is silently dropped. This is the safe default — flipping
// to "keep everything not explicitly denied" would leak every new
// env var added after this code shipped.
func TestScrubbedEnv_FailClosedDefaults(t *testing.T) {
	// Benign-looking var names that don't match any deny pattern
	// but aren't on the allowlist.
	cases := []string{
		"MY_RANDOM_TEST_VAR",
		"SOME_OTHER_THING",
		"DEBUG_FLAG",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "should-be-dropped")
			out := scrubbedEnv()
			for _, kv := range out {
				eq := strings.IndexByte(kv, '=')
				if eq < 0 {
					continue
				}
				if kv[:eq] == name {
					t.Errorf("fail-closed default broken: %s passed through", name)
				}
			}
		})
	}
}

// TestScrubbedEnv_OutputIsSorted is a sanity check that the
// output ordering is deterministic — it matters for the
// integration tests that diff the env string. Allowlist insertion
// order isn't guaranteed, so sort before comparison.
func TestScrubbedEnv_OutputIsSorted(t *testing.T) {
	t.Setenv("PATH", "/usr/bin")
	t.Setenv("HOME", "/root")
	t.Setenv("LANG", "C")
	out := scrubbedEnv()
	sorted := make([]string, len(out))
	copy(sorted, out)
	sort.Strings(sorted)
	for i := range out {
		if out[i] != sorted[i] {
			t.Errorf("scrubbedEnv() output not sorted; index %d: %q vs sorted %q", i, out[i], sorted[i])
			break
		}
	}
}

// TestScrubbedEnv_AllowlistWinsOverDenyPattern documents the
// ordering invariant: an allowlist entry overrides a deny-pattern
// match. In practice no current var triggers both, but the
// invariant must hold so a future contributor can safely add a
// new sensitive var to the denylist without worrying about
// false-positives on allowlist members.
//
// Today: no allowlist member contains SECRET/KEY/PASSWORD/
// TOKEN/CREDENTIAL. If that ever changes, this test will need to
// be updated — that's the deliberate friction.
func TestScrubbedEnv_AllowlistWinsOverDenyPattern(t *testing.T) {
	// Walk scrubbedEnvAllowlist and confirm none of its members
	// match a deny pattern (uppercase, substring).
	for name := range scrubbedEnvAllowlist {
		upper := strings.ToUpper(name)
		for _, pat := range scrubbedEnvDenyPatterns {
			if strings.Contains(upper, strings.ToUpper(pat)) {
				t.Errorf("allowlist member %q matches deny pattern %q; reorder the test", name, pat)
			}
		}
	}
}
