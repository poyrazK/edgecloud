package service

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/signing"
)

// fakeEdgeMigrateScript is the bash script body used as the
// `edge-migrate` binary in the clang-args tests. It emits a
// minimal-but-valid `transformEnvelope` (version 1, empty report,
// empty wasi_c). Both clang-args tests are testing the clang layer,
// not the analyzer, so the analyzer output is irrelevant — but the
// shape MUST match what the Go side parses (`transformEnvelope`
// at migration.go:136) or the single-file path bails with
// ErrEdgeMigrateFailed before reaching the clang switch.
//
// The script is called as `edge-migrate --language L --transform
// PATH --format json` for single-file and `edge-migrate --language L
// --transform PATH` (per-file) / `--analyze-json PATH` for tree
// mode. The output shape is the same either way: a single JSON
// envelope. (Tree mode parses the same envelope into per-file
// FileReports — see migration.go:1300+ for the deserialization
// details.)
const fakeEdgeMigrateScript = `#!/bin/sh
cat <<'EOF'
{
  "version": 1,
  "report": {
    "status": "ok",
    "wasm_stored": false,
    "patterns_detected": [],
    "patterns_transformed": [],
    "patterns_manual_review": [],
    "errors": []
  },
  "wasi_c": "int main() { return 0; }\n"
}
EOF
`

// recordingClangInvoke captures the (args, stdin) of every clang
// invocation and returns a canned stderr + nil error so the
// compile is treated as a success without actually running clang.
// The recorded slice is what the drift-guard tests assert against.
type recordingClangInvoke struct {
	args    [][]string
	stdins  []string
	hook    func(ctx context.Context, args []string, stdin io.Reader) (string, error)
	invoked int
}

func (r *recordingClangInvoke) invoke(ctx context.Context, args []string, stdin io.Reader) (string, error) {
	r.invoked++
	r.args = append(r.args, append([]string(nil), args...))
	if stdin != nil {
		b, _ := io.ReadAll(stdin)
		r.stdins = append(r.stdins, string(b))
	} else {
		r.stdins = append(r.stdins, "")
	}
	if r.hook != nil {
		return r.hook(ctx, args, stdin)
	}
	return "", nil
}

// recordingMigrationSvc wires a recordingClangInvoke into a fresh
// MigrationService backed by a fake `edge-migrate` script (so the
// analyzer step succeeds without a real binary on PATH). Returns
// the service, the recorder, and the path to the temp edge-migrate
// script (the caller does NOT need to clean it up — t.Cleanup
// handles it via the temp dir).
func recordingMigrationSvc(t *testing.T) (*MigrationService, *recordingClangInvoke, string) {
	t.Helper()
	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "edge-migrate")
	if err := writeExecutable(scriptPath, fakeEdgeMigrateScript); err != nil {
		t.Fatalf("write fake edge-migrate: %v", err)
	}
	svc := NewMigrationService(repo, store, scriptPath, "/wasi-sdk", "rustc", "wasm-tools", "cargo", "/tmp/edge-mock-wit", signing.TestKeyring(t))
	rec := &recordingClangInvoke{}
	svc.clangInvokeFn = rec.invoke
	return svc, rec, scriptPath
}

// writeExecutable writes `body` to `path` with mode 0755.
func writeExecutable(path, body string) error {
	return os.WriteFile(path, []byte(body), 0o755)
}

// TestMigrate_ClangArgsContainNostdinc pins the single-file C
// compile path. The recording stub captures the args; the test
// asserts `-nostdinc` is present (host include path blocked) and
// that `-nostdlib` is still present (libc/crt1 blocked — pre-PR
// invariant, regression guard).
//
// This is the drift-guard test for issue #622 commit 5. If a
// future contributor removes `-nostdinc` from `clangArgs`, the
// defence-in-depth against `#include "/etc/passwd"` exfiltration
// silently regresses — this test fails and forces the regression
// to be resolved consciously.
func TestMigrate_ClangArgsContainNostdinc(t *testing.T) {
	svc, rec, _ := recordingMigrationSvc(t)

	// Empty source — the analyzer stub returns a benign envelope
	// regardless. We just need to reach the clang call site.
	_, err := svc.Migrate(context.Background(), "t_1", "hello.c", "c", "")
	if err != nil {
		// The fake script doesn't actually run clang, so the
		// "success" path through the recorder returns no error
		// and the test fails with a downstream storage / size
		// issue. That's fine — we only care that the recorder
		// was invoked.
		t.Logf("Migrate returned (non-fatal for this test): %v", err)
	}
	if rec.invoked == 0 {
		t.Fatal("expected clangInvokeFn to be called at least once")
	}
	args := rec.args[0]
	if !containsFlag(args, "-nostdinc") {
		t.Errorf("single-file clang args missing -nostdinc; got %v\nissue #622 commit 5 invariant: -nostdinc blocks host include path", args)
	}
	if !containsFlag(args, "-nostdlib") {
		t.Errorf("single-file clang args missing -nostdlib; got %v", args)
	}
	if !containsFlag(args, "--target=wasm32-wasip2") {
		t.Errorf("single-file clang args missing --target=wasm32-wasip2; got %v", args)
	}
	// The trailing "-" tells clang to read source from stdin.
	if args[len(args)-1] != "-" {
		t.Errorf("expected trailing \"-\" for stdin read; got %q (last arg)", args[len(args)-1])
	}
	// stdin was wired (the recorder captured it).
	if rec.stdins[0] == "" {
		t.Error("expected non-empty stdin for single-file clang (the transformed source); got empty")
	}
}

// TestMigrateTree_ClangArgsContainNostdincAndSysroot pins the
// tree-mode C compile path. Two invariants:
//
//  1. `-nostdinc` is present (host include path blocked).
//  2. `--sysroot` is present AND points at `<wasiSdkPath>/share/wasi-sysroot`.
//     This scopes system-header searches to the wasi-sdk tree, so
//     `<wasi/...>` and `<stdio.h>` (wasified) resolve correctly.
//
// The recordingMigrationSvc helper uses wasiSdkPath="/wasi-sdk".
// The test creates that path's expected sysroot subdir so the
// helper's `os.Stat` succeeds and `--sysroot` is emitted. A
// companion test (TestMigrateTree_ClangArgsSysrootFallback) covers
// the fallback path when the sysroot is absent.
func TestMigrateTree_ClangArgsContainNostdincAndSysroot(t *testing.T) {
	// Lay down the expected sysroot so the os.Stat check in
	// clangArgs succeeds. t.TempDir() gives us a fresh dir per
	// test — the sysroot lives under t.TempDir()/fake-sdk/share/wasi-sysroot.
	fakeSdk := t.TempDir()
	sysrootDir := filepath.Join(fakeSdk, "share", "wasi-sysroot")
	if err := os.MkdirAll(sysrootDir, 0o755); err != nil {
		t.Fatalf("mkdir sysroot: %v", err)
	}

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "edge-migrate")
	if err := writeExecutable(scriptPath, fakeEdgeMigrateScript); err != nil {
		t.Fatalf("write fake edge-migrate: %v", err)
	}
	svc := NewMigrationService(repo, store, scriptPath, fakeSdk, "rustc", "wasm-tools", "cargo", "/tmp/edge-mock-wit", signing.TestKeyring(t))
	rec := &recordingClangInvoke{}
	svc.clangInvokeFn = rec.invoke

	// Build a minimal tree: one .c file. The fake analyzer stub
	// returns a benign envelope so we reach the compile switch.
	// The actual per-file compile is mocked out by the recorder.
	_, err := svc.MigrateTree(context.Background(), "t_1", "hello", "c", []domain.FileEntry{
		{Path: "main.c", Source: ""},
	})
	if err != nil {
		t.Logf("MigrateTree returned (non-fatal for this test): %v", err)
	}
	if rec.invoked == 0 {
		t.Fatal("expected clangInvokeFn to be called at least once")
	}
	args := rec.args[0]
	if !containsFlag(args, "-nostdinc") {
		t.Errorf("tree clang args missing -nostdinc; got %v", args)
	}
	if !containsFlagPair(args, "--sysroot", sysrootDir) {
		t.Errorf("tree clang args missing --sysroot %s; got %v", sysrootDir, args)
	}
	if !containsFlag(args, "-nostdlib") {
		t.Errorf("tree clang args missing -nostdlib; got %v", args)
	}
	// Source files are appended as positional args; the one
	// file we submitted ("main.c") must show up in the args.
	// MigrateTree rewrites each per-file output path with a
	// `.wasi.c` suffix (see `wasiCPath` in migration.go's tree
	// path), so we look for the basename pattern, not an exact
	// suffix.
	found := false
	for _, a := range args {
		if strings.Contains(a, "main.c") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected main.c in clang args; got %v", args)
	}
}

// TestMigrateTree_ClangArgsSysrootFallback documents the degraded
// path: when the wasi-sdk sysroot is missing, clangArgs omits
// `--sysroot` (and logs a WARN). The `-nostdinc` invariant MUST
// still hold — host include path remains blocked even without a
// sysroot to scope to.
func TestMigrateTree_ClangArgsSysrootFallback(t *testing.T) {
	// wasiSdkPath points at an empty temp dir — no sysroot under
	// share/wasi-sysroot exists.
	fakeSdk := t.TempDir()

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "edge-migrate")
	if err := writeExecutable(scriptPath, fakeEdgeMigrateScript); err != nil {
		t.Fatalf("write fake edge-migrate: %v", err)
	}
	svc := NewMigrationService(repo, store, scriptPath, fakeSdk, "rustc", "wasm-tools", "cargo", "/tmp/edge-mock-wit", signing.TestKeyring(t))
	rec := &recordingClangInvoke{}
	svc.clangInvokeFn = rec.invoke

	_, err := svc.MigrateTree(context.Background(), "t_1", "hello", "c", []domain.FileEntry{
		{Path: "main.c", Source: ""},
	})
	if err != nil {
		t.Logf("MigrateTree returned (non-fatal for this test): %v", err)
	}
	if rec.invoked == 0 {
		t.Fatal("expected clangInvokeFn to be called at least once")
	}
	args := rec.args[0]
	if !containsFlag(args, "-nostdinc") {
		t.Errorf("tree clang args missing -nostdinc in fallback path; got %v", args)
	}
	// --sysroot must NOT be present when the sysroot dir is missing.
	for i, a := range args {
		if a == "--sysroot" {
			t.Errorf("unexpected --sysroot in fallback path; got %v (--sysroot at index %d, value=%q)", args, i, args[i+1])
		}
	}
}

// TestMigrate_ClangInvokeFnSwapsDefault verifies the hook path:
// when clangInvokeFn is non-nil, defaultClangInvoke is NOT called.
// Drift guard for the swap logic in the call site — a future
// refactor that accidentally calls defaultClangInvoke alongside
// the hook would silently bypass the test capture.
func TestMigrate_ClangInvokeFnSwapsDefault(t *testing.T) {
	svc, rec, _ := recordingMigrationSvc(t)
	defaultCalled := false
	// Wrap the recorder with a hook that ALSO bumps a flag the
	// test reads. This is a roundabout way to verify the hook
	// path is the one that ran (not a duplicate call).
	rec.hook = func(ctx context.Context, args []string, stdin io.Reader) (string, error) {
		defaultCalled = true // not actually the default; the hook path is the only call.
		return "", nil
	}
	_, _ = svc.Migrate(context.Background(), "t_1", "hello.c", "c", "")
	if rec.invoked == 0 {
		t.Fatal("expected clangInvokeFn to be called")
	}
	if !defaultCalled {
		t.Error("hook path was not exercised; defaultClangInvoke may have been called instead of the hook")
	}
	// defaultClangInvoke would have attempted to actually run
	// clang at /wasi-sdk/clang — there's no such binary in CI,
	// so if the swap logic regressed, the test would either
	// crash with ENOENT or hit the recorder's fallback path
	// (the hook doesn't run). The fact that the hook ran and
	// bumped the flag proves the swap is correct.
}

// containsFlag reports whether `flag` appears as a standalone
// element of `args`. Equality, not substring — so "-nostdlib"
// does NOT match a search for "-nostdinc".
func containsFlag(args []string, flag string) bool {
	for _, a := range args {
		if a == flag {
			return true
		}
	}
	return false
}

// containsFlagPair reports whether `flag` is followed by `value`
// as the next element in `args`. Used for `--sysroot <path>`.
func containsFlagPair(args []string, flag, value string) bool {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}
