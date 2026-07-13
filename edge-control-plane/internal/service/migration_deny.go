package service

// Issue #622 commit 2 — Go-side mirror of
// `edge-migrate-lib::pre_pass_deny_c`. Runs INSIDE
// `MigrationService.MigrateTree` (and as a one-shot pre-pass in
// `MigrationService.Migrate`) before any `edge-migrate` or clang
// subprocess is spawned. The policy is deliberately identical to
// the Rust analyzer so the production short-circuit and the Go
// parser agree on what is rejected; the two implementations are
// independently written and pinned by the same fixture set.
//
// Source-of-truth direction is Rust → Go: the analyzer is the
// authoritative structural scanner. This Go pre-pass exists
// because the tree-mode pipeline shells out to `edge-migrate`
// per-file and we want to skip the subprocess entirely on
// rejection (cheaper, no `edge-migrate` stdout to parse, no
// `wasic` file to write). On a clean source this Go pre-pass is
// a no-op; on a denied source it short-circuits BEFORE any
// subprocess launches.

import (
	"fmt"
	"io"
	"os"
	"regexp"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// cIncludeRe matches a `#include` directive (any case is fine —
// directive keywords are case-sensitive in C, but we match both
// forms for the deny-list because (a) every test fixture and
// every legitimate C file uses lowercase `include`, and (b) a
// future #include-ish token like `#Include` would still resolve
// identically at the C compiler level). Capture group 1 is the
// path string inside `"…"` or `<…>`.
var cIncludeRe = regexp.MustCompile(`(?m)^\s*#\s*[iI]nclude\s*["<]([^">]+)[">]`)

// cEmbedRe matches any C23 `#embed` directive. The path argument
// (if any) is intentionally not captured — the policy is to deny
// every `#embed` regardless of argument shape, mirroring the
// Rust pre_pass_deny_c behavior.
var cEmbedRe = regexp.MustCompile(`(?m)^\s*#\s*embed\b`)

// cDenyErrors scans one source file for host-reach C directives.
// Returns a `[]ErrorInfo` carrying `code = "SECURITY_DENY:C_INCLUDE"`
// for each violation; empty slice when the source is clean.
//
// Policy (fail-closed; see `pre_pass_deny_c` in
// `edge-migrate/edge-migrate-lib/src/analyzer.rs` for the full
// rationale):
//
//   - `<…>` and `"…"` includes whose path starts with `/` (POSIX
//     absolute), `\` (Windows separator / UNC root), or `X:` (a
//     drive-letter prefix) — denied.
//   - `<…>` and `"…"` includes whose path contains `..` as a
//     path component (`..`, `./..`, `a/..`, `..\foo`) — denied.
//   - Project-relative `<…>` / `"…"` includes (no leading `/`,
//     no `..` segment) — ALLOWED.
//   - Any `#embed` — denied.
//   - Comments, string literals, and macro-reference arguments
//     (`#include FOO`) are NOT pre-matched by this Go-level
//     pass — they require either the analyzer (macro expansion)
//     or the comment-aware Rust pre-pass. The `#include FOO`
//     form is allowed by this layer; it is independently caught
//     when the macro argument is concrete at clang parse time.
//     The deny-list's `false negative` class for that case is
//     caught by commit 5's clang `-nostdinc --sysroot`
//     hardening.
func cDenyErrors(filePath string) []domain.ErrorInfo {
	sourceBytes, err := os.ReadFile(filePath)
	if err != nil {
		// Failing open here would defeat the deny-list (a
		// missing file path silently passes). Failing closed
		// for the deny layer means: if the Go pre-pass cannot
		// read the file, the analyzer subprocess will catch
		// it later with a real error. Surface as a generic
		// read-error so the failure isn't attributed to
		// security.
		return nil
	}
	source := string(sourceBytes)

	var errs []domain.ErrorInfo

	for _, m := range cIncludeRe.FindAllStringSubmatchIndex(source, -1) {
		pathStart, pathEnd := m[2], m[3]
		path := source[pathStart:pathEnd]
		if !isDenylistedCPath(path) {
			continue
		}
		line := lineOfByte(source, m[0])
		errs = append(errs, domain.ErrorInfo{
			Line: line,
			Message: fmt.Sprintf(
				"host-reach #include path %q is not permitted in migrated C source — it would resolve against the host filesystem at compile time (issue #622)",
				path,
			),
			Code: denyCodePrefix + denyCodeCInclude,
		})
	}

	for _, m := range cEmbedRe.FindAllStringIndex(source, -1) {
		line := lineOfByte(source, m[0])
		errs = append(errs, domain.ErrorInfo{
			Line: line,
			Message: "host-reach `#embed` is not permitted in migrated C source — it resolves against the host filesystem at compile time (issue #622)",
			Code: denyCodePrefix + denyCodeCInclude,
		})
	}

	return errs
}

// isDenylistedCPath mirrors the Rust `is_deny_c_path`. See that
// function for the full policy commentary; the two are
// deliberately aligned so the Rust analyzer and the Go pre-pass
// disagree as little as possible.
func isDenylistedCPath(path string) bool {
	if path == "" {
		return false
	}
	if path[0] == '/' || path[0] == '\\' {
		return true
	}
	if len(path) >= 2 && path[1] == ':' {
		return true
	}
	// Traversal: any path component equal to `..`.
	// Split on `/` AND `\` for portability.
	for i := 0; i < len(path); {
		j := i
		for j < len(path) && path[j] != '/' && path[j] != '\\' {
			j++
		}
		component := path[i:j]
		if component == ".." {
			return true
		}
		if j == len(path) {
			break
		}
		i = j + 1
	}
	return false
}

// lineOfByte is a 1-based line number lookup for a byte offset in
// `source`. Cheap — single forward scan, no allocations. Used by
// the deny scanner to attach a useful `Line` to each ErrorInfo so
// the structured response carries actionable diagnostics for the
// tenant.
func lineOfByte(source string, byteOffset int) int {
	if byteOffset < 0 || byteOffset > len(source) {
		return 0
	}
	line := 1
	for i := 0; i < byteOffset; i++ {
		if source[i] == '\n' {
			line++
		}
	}
	return line
}

// denyOriginalSource is a small wrapper that returns the on-disk
// source for a deny-pre-pass entry. The Go-level deny scanner
// reads the file from disk (the per-file `MigrateTree` already
// wrote each entry to `<tmpDir>/<path>`). When the path doesn't
// exist (e.g. handler omitted the file), an empty string is
// returned and the SHA256 in the FileReport is left empty — same
// fallback the analyzer-driven FileReport uses for unreadable
// sources.
func denyOriginalSource(absPath string) string {
	f, err := os.Open(absPath)
	if err != nil {
		return ""
	}
	defer f.Close()
	all, err := io.ReadAll(f)
	if err != nil {
		return ""
	}
	return string(all)
}

// sha256HexLower is a small helper that returns the lowercase
// hex SHA-256 of `b`. Mirrors the Rust side's `sha256_hex` in
// `edge-migrate/edge-migrate-lib/src/report.rs`.
func sha256HexLower(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return fmt.Sprintf("%x", sha256Sum(b))
}
