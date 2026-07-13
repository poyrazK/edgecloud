package service

import (
	"context"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
)

// scrubbedEnvAllowlist is the closed set of env vars passed through
// to migration toolchain subprocesses (issue #622, commit 4). Every
// other var is stripped before `exec.CommandContext`. The set is
// intentionally narrow: only what the toolchain needs to find its
// own binaries and locate the wasi-sdk sysroot + cargo registry.
//
// Operators who vendor their cargo registry or rustup install MUST
// be able to set CARGO_HOME / RUSTUP_HOME in the CP environment and
// have those values reach the subprocess — that's why those two
// are in the allowlist. Everything else is presumed sensitive and
// stripped.
//
// Adding a new var to this list is a deliberate security decision;
// if a contributor needs `FOO=bar` to reach cargo, they should
// explain why in the PR description and add the var with a comment.
var scrubbedEnvAllowlist = map[string]bool{
	// PATH: cargo/rustc/clang/wasm-tools/edge-migrate resolution.
	"PATH": true,
	// HOME: ~/.cargo/bin resolution on operator machines.
	"HOME": true,
	// TMPDIR: cargo scratch space + the CP's tmp dir for the
	// synthetic Cargo project.
	"TMPDIR": true,
	// Locale vars: some toolchains emit UTF-8 BOM or mojibake on
	// non-C locales; passing these through prevents that.
	"LANG":        true,
	"LC_ALL":      true,
	"LC_COLLATE":  true,
	"LC_CTYPE":    true,
	"LC_MESSAGES": true,
	"LC_MONETARY": true,
	"LC_NUMERIC":  true,
	"LC_TIME":     true,
	// WASI_SDK_PATH: operator-set; clang invocation hard-codes
	// filepath.Join(s.wasiSdkPath, "clang"), but the wasi-sdk
	// itself reads $WASI_SDK_PATH for some helper scripts.
	"WASI_SDK_PATH": true,
	// CARGO_HOME: operator-vendored cargo registry location.
	"CARGO_HOME": true,
	// CARGO_TARGET_DIR: operator-set override for where cargo
	// drops build artifacts.
	"CARGO_TARGET_DIR": true,
	// RUSTUP_HOME: operator's rustup install (when running
	// rustc via cargo).
	"RUSTUP_HOME": true,
	// CLANG: clang-specific env (e.g. CLANG_FLAGS). wasi-sdk's
	// clang reads it.
	"CLANG": true,
}

// scrubbedEnvDenyPatterns is a regex-style suffix/contains list of
// var names that MUST never reach a toolchain subprocess. Used as
// a fail-closed defense: if a new sensitive var is added to
// internal/config/config.go without updating this list, the
// migration_scrub_test.go drift-guard test fails.
//
// The set is structured so a broad pattern (`*SECRET*`, `*KEY*`)
// catches the common case, and the explicit allowlist above
// overrides it for the few vars that legitimately contain "key"
// in their name (CARGO_HOME's bin subdir, etc.). Since the
// allowlist is consulted FIRST and is an exact match, a sensitive
// var cannot accidentally leak by sharing a substring with an
// allowlisted name.
var scrubbedEnvDenyPatterns = []string{
	// JWT / cluster signing — never reaches the toolchain.
	"JWT_SECRET",
	"JWT_KEY",
	"BOOTSTRAP_SECRET",
	"EDGE_INTERNAL_TOKEN",
	// Postgres creds.
	"DATABASE_PASSWORD",
	// Stripe creds (publishable key is "less secret" but still
	// nothing the toolchain needs).
	"STRIPE_SECRET_KEY",
	"STRIPE_WEBHOOK_SECRET",
	"STRIPE_PUBLISHABLE_KEY",
	"STRIPE_PRICE_ID",
	// Signing keyring (multi-key format too).
	"EDGE_SIGNING_KEY",
	"EDGE_SIGNING_KEY_ID",
	"EDGE_SIGNING_KEYRING",
	"EDGE_SIGNING_KEYRING_PATH",
	"EDGE_SIGNING_KEY_PATH",
	// Secrets encryption keyring.
	"EDGE_SECRETS_MASTER_KEY",
	"EDGE_SECRETS_ACTIVE_KEY_ID",
	"EDGE_SECRETS_KEY",
	// Storage backends.
	"STORAGE_S3_ACCESS_KEY",
	"STORAGE_S3_SECRET_KEY",
	"STORAGE_S3_SESSION_TOKEN",
	"STORAGE_PEER_CONTROL_PLANE_INTERNAL_TOKEN",
	// NATS connection string may carry credentials.
	"NATS_URL",
	// Broad patterns — caught case-insensitively. "SECRET" matches
	// JWT_SECRET, EDGE_SECRETS_*, *SECRET*, etc. "KEY" matches
	// STRIPE_*, EDGE_SIGNING_*, EDGE_SECRETS_KEY_*, etc.
	"SECRET",
	"KEY",
	"PASSWORD",
	"TOKEN",
	"CREDENTIAL",
}

// scrubbedEnv returns a `[]string` of "NAME=VALUE" entries
// containing ONLY the allowlisted env vars. The rest of the
// parent's `os.Environ()` is silently dropped — this is the
// fail-closed property: even if a future contributor adds a new
// sensitive var to config.go, it will be stripped here unless
// it's also explicitly added to scrubbedEnvAllowlist.
//
// Two-step filter:
//
//  1. If the var name is in the allowlist → keep it.
//  2. Otherwise, check whether any deny-pattern is a substring
//     (case-insensitive) of the var name. If yes → drop. If no
//     → drop (fail-closed: an unrecognised var is treated as
//     potentially sensitive).
//
// The fail-closed default is intentional: the alternative —
// "keep everything not explicitly denied" — would leak every new
// env var introduced after this code shipped. The deny-pattern
// substring match is a coarse safety net for typos in the
// config-side var name; the explicit list is the authoritative
// denylist.
func scrubbedEnv() []string {
	in := os.Environ()
	out := make([]string, 0, len(scrubbedEnvAllowlist))
	for _, kv := range in {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			continue // malformed; skip
		}
		name := kv[:eq]
		// 1) Allowlist wins (exact match).
		if scrubbedEnvAllowlist[name] {
			out = append(out, kv)
			continue
		}
		// 2) Deny-pattern match (case-insensitive substring).
		upper := strings.ToUpper(name)
		denied := false
		for _, pat := range scrubbedEnvDenyPatterns {
			if strings.Contains(upper, strings.ToUpper(pat)) {
				denied = true
				break
			}
		}
		if denied {
			continue // strip — explicit deny pattern.
		}
		// Fail-closed: drop everything else. Comment this out
		// to flip to fail-open (NOT recommended).
		_ = upper
	}
	// Sort for deterministic output (the allowlist iteration order
	// is non-deterministic; integration tests diff the env string
	// and need stable ordering). Subprocess startup cost is
	// unaffected — `exec.Cmd` runs `sort.Strings` over a ~16-entry
	// slice in nanoseconds.
	sort.Strings(out)
	return out
}

// newToolCmd wraps `exec.CommandContext` with the migration
// service's standard subprocess hardening (issue #622, commit 4).
//
// Why a helper instead of inlining at each call site:
//
//   - Single source of truth for the env-scrub + cancel + WaitDelay
//     settings. Future contributors can't accidentally skip the
//     scrub by writing a fresh `exec.CommandContext`.
//   - Every migration toolchain subprocess (`cargo`, `rustc`,
//     `wasm-tools`, `edge-migrate`, `clang`) gets identical
//     treatment — no per-call drift where one tool sees the host
//     env and another doesn't.
//
// cmd.Env is set explicitly to `scrubbedEnv()` so `os.Environ()`
// from the parent process is never inherited. This is the
// fail-closed property: even if `cmd.Env` is left nil, Go's
// `os/exec` would inherit the parent's env (the historical
// default), so we MUST override.
//
// cmd.Cancel is set so ctx cancellation kills the subprocess via
// SIGKILL (not the polite SIGTERM), avoiding the wasm-time /
// cargo deadlock cases where a graceful shutdown hangs forever
// on a stuck subprocess. cmd.WaitDelay is the fallback that
// forces `cmd.Wait()` to return even if the child ignores the
// signal — the subprocess is reaped, the parent's pipeline can
// continue.
func (s *MigrationService) newToolCmd(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = scrubbedEnv()
	cmd.Cancel = func() error {
		// Best-effort SIGKILL on ctx cancellation. On
		// Windows this falls back to Process.Kill() (no
		// signal distinction), which is fine for our use
		// case.
		if cmd.Process != nil {
			return cmd.Process.Kill()
		}
		return nil
	}
	cmd.WaitDelay = 5 * time.Second
	return cmd
}
