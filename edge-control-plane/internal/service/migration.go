package service

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/signing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
	"github.com/google/uuid"
)

// ErrMigrateTreeFailed is returned when the tree-mode migration fails
// (per-file errors don't trigger this; only tree-level errors do —
// the report is still returned in the partial-success case).
var ErrMigrateTreeFailed = fmt.Errorf("tree migration failed")

// ErrMigrationFailed is returned when the single-file `Migrate` path
// hits a terminal failure (oversized artifact, missing toolchain,
// etc.) but the request itself was syntactically valid. The handler
// maps this to HTTP 422 and emits the structured report body so
// callers can see the per-pattern error detail.
var ErrMigrationFailed = fmt.Errorf("migration failed")

// ErrEdgeMigrateFailed is returned when the edge-migrate subprocess fails.
var ErrEdgeMigrateFailed = fmt.Errorf("edge-migrate transform failed")

// ErrClangFailed is returned when the wasi-sdk clang subprocess fails.
var ErrClangFailed = fmt.Errorf("wasi-sdk clang compilation failed")

// ErrRustcFailed is returned when the rustc subprocess fails.
var ErrRustcFailed = fmt.Errorf("rustc compilation failed")

// ErrWasmToolsFailed is returned when the wasm-tools component-wrap
// subprocess fails (issue #415). Distinct from ErrRustcFailed so
// operators can grep for wrap-vs-compile failures separately.
var ErrWasmToolsFailed = fmt.Errorf("wasm-tools component wrap failed")

// ErrCargoBuildFailed is returned when the cargo build subprocess
// fails or the synthesized Cargo project is malformed (issue #415).
// Bare rustc cannot resolve the wit_bindgen::generate! proc-macro
// or its wasi:* dependency surface, so the rust migration path
// shells out to cargo instead.
var ErrCargoBuildFailed = fmt.Errorf("cargo build failed")

// C-path WIT version audit (issue #415):
//
// The C path (`clang --target=wasm32-wasip2 -nostdlib <source>`)
// relies on wasi-sdk's bundled clang + wit-component encoder.
// The most recent wasi-sdk releases (≥ 24) bundle a
// wit-component encoder that emits wasi:http@0.2.1 (matching
// the wasmtime 45.0.3 linker). Operators on older wasi-sdk
// (< 24) would hit the same wasi:http@0.2.4 mismatch the Rust
// path did before #415.
//
// We do not pin the C path's clang to a specific bundle
// because the operator's installed wasi-sdk is the runtime
// contract for the C path; if a 0.2.4 regression surfaces,
// the fix is "upgrade wasi-sdk" — same as the upstream
// recommendation. Track any future C-path break in:
// https://github.com/edgeclouderz/edge-cloud/issues/415 (audit)

// DeploymentRepoInterface abstracts deployment creation for testing.
type DeploymentRepoInterface interface {
	Create(ctx context.Context, d *domain.Deployment) error
	// UpdateHashAndSignature writes the post-SaveAndHash fields
	// (hash, signature, signing_key_id) to the row created by the
	// earlier Create call. Used by the migration service after
	// the artifact is on disk and signed (issue #307). Idempotent
	// on missing row (no-op) so a retry after a transient error
	// doesn't fail with "row not found".
	UpdateHashAndSignature(ctx context.Context, d *domain.Deployment) error
	// DeleteByID removes a deployment row by ID. Idempotent on missing
	// row. Used as the compensating write when the artifact save
	// fails after the row was inserted.
	DeleteByID(ctx context.Context, id string) error
}

// ArtifactStoreInterface abstracts wasm artifact storage for testing.
// Mirrors storage.ArtifactStore (ctx-aware) so rollbackArtifactSave
// and test mocks can pass through to the production type without a
// signature adapter.
type ArtifactStoreInterface interface {
	Save(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) error
	// SaveAndHash streams the artifact to disk and returns its SHA-256
	// in a single io.Copy pass (no intermediate buffer). Hash + write
	// are concurrent via io.MultiWriter; the final path either
	// contains the full artifact (with a verified hash) or doesn't
	// exist (atomic temp-rename). Prefer this over Save when the
	// caller needs the hash; the older Save was retained for callers
	// that don't (and for the migration pre-compile path that
	// already has the bytes hashed separately).
	SaveAndHash(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) ([]byte, error)
	// Delete removes an artifact. Idempotent on missing file. Used as
	// the compensating write when the row insert fails after the
	// artifact was written.
	Delete(ctx context.Context, tenantID, appName, deploymentID string) error
}

// transformEnvelope mirrors edge-migrate-lib's `TransformOutput`.
// Emitted by `edge-migrate --transform --format json`. The Go control
// plane is the only consumer in this repo. The `Version` field lets the
// server reject envelopes from a binary whose wire shape this server
// doesn't know how to parse.
type transformEnvelope struct {
	Version uint32                 `json:"version"`
	Report  domain.MigrationReport `json:"report"`
	WasiC   string                 `json:"wasi_c"`
}

// MigrationService transforms POSIX C source to WASI and compiles it to wasm.
type MigrationService struct {
	deploymentRepo  DeploymentRepoInterface
	artifactStore   storage.ArtifactStore
	edgeMigratePath string
	wasiSdkPath     string
	// rustcPath is the absolute path to a rustc binary. The rust
	// migration path (language == "rust") uses cargo build — not
	// bare rustc — so the binary is invoked transitively. The
	// `cargo` binary on $PATH is what does the work; this field is
	// retained for the C-with-extra-rust-deps edge case (currently
	// unused but kept for forward-compat).
	rustcPath string
	// wasmToolsPath is the absolute path to a `wasm-tools` binary
	// capable of running `wasm-tools component new`. Used to wrap the
	// cargo-produced core module into a wasi:http@0.2.1 component
	// (issue #415). When the binary is not on PATH the rust
	// migration path returns ErrWasmToolsFailed before any compile
	// work.
	wasmToolsPath string
	// cargoPath is the absolute path to a `cargo` binary capable of
	// resolving the synthetic Cargo.toml the rust migration path
	// writes into its temp dir. Same error semantics as
	// wasmToolsPath.
	cargoPath string
	// witDir is the absolute path to a directory containing the
	// canonical edge-cloud WIT tree (edge-cloud.wit + deps/*). The
	// path is passed to `wit_bindgen::generate!` via its `path:`
	// argument. The MigrationService is constructed with a
	// per-process materialized copy of the embedded FS at
	// edge-control-plane/internal/service/wit/; see
	// NewMigrationService for the materialization step.
	witDir string
	// keyring stamps every new deployment's artifact (issue #307 PR1;
	// was a single `*signing.Signer` before PR1). Required — set by
	// the constructor; a nil keyring would cause `Migrate` and
	// `MigrateTree` to return an error. Mirrors
	// `DeploymentService.keyring` so artifacts produced by either
	// service carry the same active key id.
	keyring *signing.Keyring
}

// NewMigrationService creates a MigrationService.
//
// The constructor materializes the embedded WIT tree (see the wit
// subpackage) into a per-process tmp dir and caches the absolute
// path on the returned service. Operators who want a stable path
// can set EDGE_WIT_DIR; the constructor uses that path verbatim
// without touching the embedded copy.
func NewMigrationService(
	deploymentRepo DeploymentRepoInterface,
	artifactStore storage.ArtifactStore,
	edgeMigratePath, wasiSdkPath, rustcPath, wasmToolsPath, cargoPath, witDir string,
	keyring *signing.Keyring,
) *MigrationService {
	return &MigrationService{
		deploymentRepo:  deploymentRepo,
		artifactStore:   artifactStore,
		edgeMigratePath: edgeMigratePath,
		wasiSdkPath:     wasiSdkPath,
		rustcPath:       rustcPath,
		wasmToolsPath:   wasmToolsPath,
		cargoPath:       cargoPath,
		witDir:          witDir,
		keyring:         keyring,
	}
}

// injectWitBindgen prepends a `wit_bindgen::generate!({ ... })` block
// to the transformed Rust source and rewrites the transformer's
// `use wasi::` imports to `use crate::wasi::` so they resolve
// against the bindings the macro generates (issue #415,
// edge-migrate-lib's rust_transformer.rs:44-50 + wit-bindgen 0.45's
// `generate_all` placement convention — bindings land under
// `crate::wasi::*`, not at the root). The macro emits the
// `mod wasi { ... }` declarations the trailing `use` statements
// need to be able to find. `path:` points at the materialized
// canonical WIT tree (see wit.Materialize); `world` matches
// `edge-runtime-handler`, the only world the FaaS migration path
// supports today.
func (s *MigrationService) injectWitBindgen(transformed []byte) []byte {
	// Embed the WIT dir as a Rust string literal via `%q`. Go's `%q`
	// quotes and escapes for Go syntax (not Rust), but for the
	// characters our WIT dir can legally contain — ASCII
	// backslashes and double quotes — the escape rules happen to
	// match. os.MkdirTemp results on Linux/macOS don't contain
	// backslashes; paths on Windows do and `%q` doubles each one,
	// which is also valid Rust.
	macro := []byte(fmt.Sprintf(
		`wit_bindgen::generate!({
    world: "edge-runtime-handler",
    path: %q,
    generate_all,
});
`, s.witDir))
	// The transformer emits `use crate::wasi::*` directly via
	// WASI_RUST_PRELUDE (edge-migrate-lib/src/rust_transformer.rs),
	// so no import rewriting is needed here. The previous
	// `bytes.ReplaceAll(..., "use wasi::", "use crate::wasi::")`
	// was a stop-gap for issue #416; fixed at the source by #417.
	out := make([]byte, 0, len(macro)+len(transformed))
	out = append(out, macro...)
	out = append(out, transformed...)
	return out
}

// compileRustAsComponent compiles the injected Rust source into a
// wasm32 core module by shelling out to cargo (issue #415). It
// creates a self-contained synthetic Cargo project in a per-call
// tmp dir, writes Cargo.toml + src/lib.rs, runs
// `cargo build --target wasm32-unknown-unknown --release`, and
// returns the absolute path to the produced core module. The
// caller is responsible for os.RemoveAll-ing the tmp dir and for
// wrapping the core module into a component via wrapAsComponent.
//
// Bare `rustc` is insufficient because `wit_bindgen::generate!`
// is a procedural macro that needs cargo's dependency resolution
// (`--extern`, registry fetch, proc-macro loading) — exactly
// mirroring samples/hello/Cargo.toml post-PR-#414.
func (s *MigrationService) compileRustAsComponent(
	ctx context.Context,
	injected []byte,
	appName string,
	tmpBase string,
) (coreWasmPath string, cargoDir string, err error) {
	// Sanitize appName to a Rust package-name-compatible identifier
	// (lowercase alnum + dashes, must start with a letter — package
	// names can't start with a digit). The handler has already
	// validated `^[a-z0-9][a-z0-9-]{0,62}$` so this is just
	// defense-in-depth.
	pkgName := sanitizeRustPackageName(appName)

	dir, mkErr := os.MkdirTemp(tmpBase, "migrate-cargo-*")
	if mkErr != nil {
		return "", "", fmt.Errorf("compileRustAsComponent: mkdir: %w", mkErr)
	}
	cargoDir = dir

	// Best-effort cleanup. Storing paths in named returns so the
	// caller still sees them even on the error path.
	defer func() {
		if err != nil {
			_ = os.RemoveAll(dir)
		}
	}()

	// Cargo.toml — pinned to wit-bindgen 0.45 to match the
	// canonical samples/hello project (PR #414).
	cargoToml := fmt.Sprintf(`[package]
name = %q
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.45"

[profile.release]
opt-level = "s"
lto = true
codegen-units = 1
`, pkgName)
	if wErr := os.WriteFile(filepath.Join(dir, "Cargo.toml"), []byte(cargoToml), 0o644); wErr != nil {
		return "", dir, fmt.Errorf("compileRustAsComponent: write Cargo.toml: %w", wErr)
	}

	if mErr := os.MkdirAll(filepath.Join(dir, "src"), 0o755); mErr != nil {
		return "", dir, fmt.Errorf("compileRustAsComponent: mkdir src: %w", mErr)
	}
	if wErr := os.WriteFile(filepath.Join(dir, "src", "lib.rs"), injected, 0o644); wErr != nil {
		return "", dir, fmt.Errorf("compileRustAsComponent: write src/lib.rs: %w", wErr)
	}

	cmd := exec.CommandContext(ctx, s.cargoPath,
		"build",
		"--target", "wasm32-unknown-unknown",
		"--release",
		"--manifest-path", filepath.Join(dir, "Cargo.toml"),
		"--target-dir", filepath.Join(dir, "target"),
	)
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	cmd.Stdout = &bytes.Buffer{} // discard
	if runErr := cmd.Run(); runErr != nil {
		// Surface the cargo diagnostics so the caller can attach
		// them to the typed error.
		return "", dir, fmt.Errorf("%w: %s: %s",
			ErrCargoBuildFailed, runErr.Error(), truncateStderr(stderr.String()))
	}

	core := filepath.Join(dir, "target", "wasm32-unknown-unknown", "release", pkgName+".wasm")
	if _, statErr := os.Stat(core); statErr != nil {
		return "", dir, fmt.Errorf("%w: expected output not found at %s", ErrCargoBuildFailed, core)
	}
	return core, dir, nil
}

// wrapAsComponent wraps a wasm32 core module into a wasi component
// by invoking `wasm-tools component new --world
// edge-runtime-handler` in place (issue #415). The wrap rewrites
// the same path in place so the caller's path reference still
// points at the component after the call returns. Mirrors the
// precompile.PrecompileCwasm exec pattern at precompile.go:50-73.
func (s *MigrationService) wrapAsComponent(
	ctx context.Context,
	coreWasmPath string,
) error {
	cmd := exec.CommandContext(ctx, s.wasmToolsPath,
		"component", "new",
		"--world", "edge-runtime-handler",
		coreWasmPath,
		"-o", coreWasmPath,
	)
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	cmd.Stdout = &bytes.Buffer{}
	if err := cmd.Run(); err != nil {
		// Surface wasm-tools' diagnostics so the caller can
		// explain *why* the wrap failed (most often: missing
		// `wasi:http/incoming-handler` impl in the user source).
		return fmt.Errorf("%w: %s: %s",
			ErrWasmToolsFailed, err.Error(), truncateStderr(stderr.String()))
	}
	return nil
}

// truncateStderr keeps the error message bounded. rustc/clang/cargo/
// wasm-tools can emit kilobytes of diagnostics; we only need the
// tail for actionability.
func truncateStderr(s string) string {
	const max = 4 * 1024
	if len(s) <= max {
		return s
	}
	return "...(truncated)...\n" + s[len(s)-max:]
}

// sanitizeRustPackageName turns an edge-cloud app name into a
// package-name-safe Rust identifier. Edge app names already match
// `^[a-z0-9][a-z0-9-]{0,62}$` per the handler; this only needs to
// replace non-identifier characters (dashes) and guarantee a
// non-empty, alpha-leading result.
func sanitizeRustPackageName(name string) string {
	if name == "" {
		return "edge_app"
	}
	out := make([]byte, 0, len(name))
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			out = append(out, c)
		case c == '-':
			out = append(out, '_')
		default:
			out = append(out, '_')
		}
	}
	if out[0] >= '0' && out[0] <= '9' {
		out = append([]byte{'a'}, out...)
	}
	return string(out)
}

// Migrate transforms the given C or Rust source to WASI source,
// compiles it to wasm, stores the artifact, and creates a deployment
// record. The `language` arg is the source language (`"c"` or
// `"rust"`); the handler is expected to have validated it.
//
// M3: previously `_language` was ignored and the service hard-coded
// the C pipeline. The arg is now first-class and drives both the
// edge-migrate subprocess (`--language <lang>`) and the final compile
// step (`clang` for C, `rustc` for Rust).
//
// HEAD's `sanitizeAppName` path-safety check is preserved inline as
// defense-in-depth — the handler already rejects path-traversal
// filenames, but a direct internal caller (or a future test) could
// still pass `../etc.c`.
//
// HEAD's `transformEnvelope` JSON wire format is also preserved —
// the edge-migrate subprocess emits `--format json` and the report
// is built from the envelope's structured `Report` field. The
// `detectTransformedPatterns` / `detectTransformedPatternsRust`
// heuristic stays as a fallback when the envelope carries no
// pattern info (older binaries).
func (s *MigrationService) Migrate(ctx context.Context, tenantID, filename, language, source string) (*domain.MigrationReport, error) {
	// Default to C when language is empty — the handler rejects
	// unknown values, so this is a safe sentinel for any tests that
	// construct a MigrationReport directly.
	if language == "" {
		language = "c"
	}
	// Derive app name: strip the language-appropriate suffix.
	appName := strings.TrimSuffix(filename, extForLanguage(language))
	if appName == "" {
		appName = "app"
	}
	// Validate the derived app_name (HEAD's sanitizeAppName logic).
	if appName == "" {
		return nil, fmt.Errorf("invalid filename %q: cannot derive app name", filename)
	}
	if strings.ContainsAny(appName, "/\\") {
		return nil, fmt.Errorf("invalid filename %q: contains path separator", filename)
	}
	if strings.Contains(appName, "..") {
		return nil, fmt.Errorf("invalid filename %q: contains '..'", filename)
	}

	// Write source to a temp file for edge-migrate (reads a path, not stdin)
	tmpSrc, err := os.CreateTemp("", "migrate-*-"+filepath.Base(extForLanguage(language)))
	if err != nil {
		return nil, fmt.Errorf("creating temp source file: %w", err)
	}
	tmpSrcPath := tmpSrc.Name()
	defer func() {
		if removeErr := os.Remove(tmpSrcPath); removeErr != nil && !os.IsNotExist(removeErr) {
			log.Printf("migration service: failed to remove temp file: %v", removeErr)
		}
	}()
	if _, err := tmpSrc.WriteString(source); err != nil {
		if closeErr := tmpSrc.Close(); closeErr != nil {
			log.Printf("migration service: failed to close temp file: %v", closeErr)
		}
		return nil, fmt.Errorf("writing temp source: %w", err)
	}
	if err := tmpSrc.Close(); err != nil {
		log.Printf("migration service: failed to close temp file: %v", err)
	}

	// Run `edge-migrate --language <lang> --transform <path> --format json`.
	// The binary emits a `transformEnvelope` with the structured report and
	// the transformed (WASI) source. The `--language` flag selects the
	// analyzer (C default; rust is required for Rust sources). The
	// envelope's `WasiC` (or future equivalent) carries the transformed
	// source; `Report` carries the structured per-pattern fields.
	edgeMigCmd := exec.CommandContext(ctx, s.edgeMigratePath,
		"--language", language, "--transform", tmpSrcPath, "--format", "json")
	var edgeMigOut bytes.Buffer
	edgeMigCmd.Stdout = &edgeMigOut
	var edgeMigErr bytes.Buffer
	edgeMigCmd.Stderr = &edgeMigErr
	if err := edgeMigCmd.Run(); err != nil {
		return &domain.MigrationReport{
			Status:     domain.MigrationStatusFailed,
			WasmStored: false,
			AppName:    appName,
			Errors: []domain.ErrorInfo{{
				Line:    0,
				Message: fmt.Sprintf("edge-migrate failed: %s — %s", err, edgeMigErr.String()),
			}},
		}, ErrEdgeMigrateFailed
	}
	// Parse the envelope. HEAD introduced the `transformEnvelope`
	// JSON wire format (version + structured report + transformed
	// source). Release used plain stdout. We keep the envelope as the
	// structured source of truth (carrying per-pattern fields) and
	// fall back to the heuristic when the envelope doesn't carry
	// pattern info (older binaries).
	var envelope transformEnvelope
	var parseErr error
	_ = json.Unmarshal(edgeMigOut.Bytes(), &envelope) // soft-parse; fall through to heuristic on error
	if len(edgeMigOut.Bytes()) > 0 && len(edgeMigOut.Bytes()) < 4 {
		parseErr = fmt.Errorf("edge-migrate output too short to be a valid envelope")
	} else if !bytes.HasPrefix(bytes.TrimSpace(edgeMigOut.Bytes()), []byte("{")) {
		parseErr = fmt.Errorf("edge-migrate output is not JSON")
	} else {
		parseErr = json.Unmarshal(edgeMigOut.Bytes(), &envelope)
	}
	if parseErr != nil {
		return &domain.MigrationReport{
			Status:     domain.MigrationStatusFailed,
			WasmStored: false,
			AppName:    appName,
			Errors: []domain.ErrorInfo{{
				Line:    0,
				Message: fmt.Sprintf("edge-migrate JSON parse failed: %v — stderr: %s", parseErr, edgeMigErr.String()),
			}},
		}, ErrEdgeMigrateFailed
	}
	// Reject envelopes whose wire shape this server doesn't understand.
	if envelope.Version != domain.MigrateEnvelopeVersion {
		return &domain.MigrationReport{
			Status:     domain.MigrationStatusFailed,
			WasmStored: false,
			AppName:    appName,
			Errors: []domain.ErrorInfo{{
				Line: 0,
				Message: fmt.Sprintf(
					"edge-migrate envelope version %d unsupported (server expects %d) — please upgrade",
					envelope.Version, domain.MigrateEnvelopeVersion,
				),
			}},
		}, ErrEdgeMigrateFailed
	}
	// The envelope's WasiC field carries the transformed source.
	// Older binaries may emit plain stdout (no envelope); in that
	// case the entire stdout is the transformed source. We try the
	// envelope's WasiC first, falling back to the full stdout.
	var transformed string
	if envelope.WasiC != "" {
		transformed = envelope.WasiC
	} else {
		transformed = edgeMigOut.String()
	}

	// Build pattern report. Prefer the envelope's structured
	// Report.PatternsTransformed; fall back to the heuristic when
	// the envelope is empty (older binaries).
	var patternsTransformed []domain.PatternInfo
	if len(envelope.Report.PatternsTransformed) > 0 {
		patternsTransformed = envelope.Report.PatternsTransformed
	} else if language == "rust" {
		patternsTransformed = detectTransformedPatternsRust(transformed)
	} else {
		patternsTransformed = detectTransformedPatterns(transformed)
	}

	// Compile → wasm. C uses wasi-sdk clang (reads from stdin).
	// Rust writes the transformed source to a temp file and invokes
	// `rustc --target wasm32-wasip2 --crate-type=cdylib` (rustc
	// reads files, not stdin).
	tmpWasm, err := os.CreateTemp("", "migrate-*.wasm")
	if err != nil {
		return nil, fmt.Errorf("creating temp wasm file: %w", err)
	}
	tmpWasmPath := tmpWasm.Name()
	if err := tmpWasm.Close(); err != nil {
		log.Printf("migration service: failed to close temp Wasm file: %v", err)
	}
	defer func() {
		if removeErr := os.Remove(tmpWasmPath); removeErr != nil && !os.IsNotExist(removeErr) {
			log.Printf("migration service: failed to remove temp Wasm file: %v", removeErr)
		}
	}()

	var compileErrMsg string
	var compileSentinel error
	switch language {
	case "rust":
		// Two-step pipeline (issue #415): inject
		// `wit_bindgen::generate!` at byte 0, then `cargo build
		// --target wasm32-unknown-unknown --release` (bare rustc
		// cannot resolve the proc-macro), then `wasm-tools
		// component new` to wrap the core module into a
		// wasi:http@0.2.1 component.
		injected := s.injectWitBindgen([]byte(transformed))
		corePath, cargoDir, err := s.compileRustAsComponent(ctx, injected, appName, os.TempDir())
		if err != nil {
			defer func() { _ = os.RemoveAll(cargoDir) }()
			compileSentinel = errors.Unwrap(err)
			if compileSentinel == nil {
				compileSentinel = ErrCargoBuildFailed
			}
			compileErrMsg = err.Error()
			break
		}
		// The cargoDir is cleaned up once the wrap completes (or
		// is cleaned up by the deferred RemoveAll on this branch).
		defer func() { _ = os.RemoveAll(cargoDir) }()
		// Move the cargo-produced core into tmpWasmPath so the
		// rest of the flow (size check, SaveAndHash, signature)
		// operates on a single canonical path. Then wrap in
		// place.
		if err := os.Rename(corePath, tmpWasmPath); err != nil {
			compileErrMsg = fmt.Sprintf("moving cargo output to %s: %v", tmpWasmPath, err)
			compileSentinel = ErrCargoBuildFailed
			break
		}
		if err := s.wrapAsComponent(ctx, tmpWasmPath); err != nil {
			compileSentinel = ErrWasmToolsFailed
			compileErrMsg = err.Error()
			break
		}
		// success — compileErrMsg stays empty
	default: // "c"
		clangBin := filepath.Join(s.wasiSdkPath, "clang")
		clangCmd := exec.CommandContext(ctx, clangBin,
			"--target=wasm32-wasip2", "-nostdlib",
			"-o", tmpWasmPath, "-")
		clangCmd.Stdin = strings.NewReader(transformed)
		var clangErr bytes.Buffer
		clangCmd.Stderr = &clangErr
		compileSentinel = ErrClangFailed
		if err := clangCmd.Run(); err != nil {
			compileErrMsg = fmt.Sprintf("clang failed: %s — %s", err, clangErr.String())
		}
	}

	if compileErrMsg != "" {
		// Build failure report from envelope's structured Report (HEAD)
		// when available; fall back to a fresh struct otherwise.
		// Status is Failed: the analyzer ran fine, the toolchain refused
		// to compile the transformed source. Partial is reserved for the
		// analyzer-driven case (some patterns need manual review).
		report := envelope.Report
		report.Status = domain.MigrationStatusFailed
		report.WasmStored = false
		report.AppName = appName
		report.DeploymentID = nil
		report.Errors = []domain.ErrorInfo{{
			Line:    0,
			Message: compileErrMsg,
		}}
		if len(report.PatternsTransformed) == 0 {
			report.PatternsTransformed = patternsTransformed
		}
		return &report, compileSentinel
	}

	// Stat the compiled wasm for the size cap. Avoids buffering the
	// full artifact (up to MaxArtifactSize = 100 MiB) into RAM just
	// to read its length — the streaming SaveAndHash below hashes
	// and writes the file in a single pass.
	info, err := os.Stat(tmpWasmPath)
	if err != nil {
		return nil, fmt.Errorf("stat compiled wasm: %w", err)
	}

	// Enforce MaxArtifactSize. Catches accidental huge builds (e.g.,
	// debug symbols left in, broken optimization) before we ever
	// hit the database or filesystem. Closes the pre-existing gap on
	// the single-file `Migrate` path (M2.C8) — MigrateTree enforces
	// the same cap separately.
	if info.Size() > MaxArtifactSize {
		report := envelope.Report
		report.Status = domain.MigrationStatusFailed
		report.WasmStored = false
		report.AppName = appName
		report.DeploymentID = nil
		report.Errors = []domain.ErrorInfo{{
			Line:    0,
			Message: fmt.Sprintf("wasm exceeds %d bytes (MaxArtifactSize)", MaxArtifactSize),
		}}
		if len(report.PatternsTransformed) == 0 {
			report.PatternsTransformed = patternsTransformed
		}
		// Return a typed sentinel so the handler can map this to
		// HTTP 422 (artifact too large is a request-level failure,
		// not a server-level one). The structured report is still
		// returned so the client can read the error detail.
		return &report, ErrMigrationFailed
	}

	// Reject output that isn't actually wasm. A misconfigured
	// wasi-sdk or a non-wasm target will produce a file that passes the
	// compiler but fails on the worker — surface that here so the
	// migration report reflects a clear failure rather than silently
	// storing a broken artifact. Peek the magic bytes from the file
	// directly (4 bytes is enough; the spec's 8-byte header is just
	// magic + version, and a bad magic always means a bad file).
	magicFile, err := os.Open(tmpWasmPath)
	if err != nil {
		return nil, fmt.Errorf("opening compiled wasm: %w", err)
	}
	var magic [4]byte
	if _, err := io.ReadFull(magicFile, magic[:]); err != nil {
		_ = magicFile.Close()
		return nil, fmt.Errorf("reading wasm magic: %w", err)
	}
	if !bytes.HasPrefix(magic[:], []byte{0x00, 0x61, 0x73, 0x6d}) {
		_ = magicFile.Close()
		report := envelope.Report
		report.Status = domain.MigrationStatusFailed
		report.WasmStored = false
		report.AppName = appName
		report.DeploymentID = nil
		report.Errors = []domain.ErrorInfo{{
			Line:    0,
			Message: "compiled output is not a valid wasm binary (missing magic bytes)",
		}}
		if len(report.PatternsTransformed) == 0 {
			report.PatternsTransformed = patternsTransformed
		}
		return &report, fmt.Errorf("compiled output is not a valid wasm binary")
	}
	// Rewind so SaveAndHash below reads from byte 0, not byte 4.
	if _, err := magicFile.Seek(0, io.SeekStart); err != nil {
		_ = magicFile.Close()
		return nil, fmt.Errorf("rewinding compiled wasm: %w", err)
	}

	// Generate deployment ID
	depID := "d_" + uuid.New().String()

	// Create deployment DB record. Hash is filled in after SaveAndHash
	// returns — the hash is computed in the same io.Copy pass that
	// writes the artifact to disk.
	deployment := &domain.Deployment{
		ID:        depID,
		TenantID:  tenantID,
		AppName:   appName,
		Status:    domain.StatusMigrated,
		Hash:      "",
		CreatedAt: time.Now(),
	}
	if err := s.deploymentRepo.Create(ctx, deployment); err != nil {
		_ = magicFile.Close()
		return nil, fmt.Errorf("creating deployment record: %w", err)
	}

	// Stream the artifact to disk and compute the SHA-256 in a single
	// pass. SaveAndHash is atomic on disk (temp-rename), so a failed
	// read mid-stream leaves no partial blob at the final path. We
	// inline the rollback (DeleteByID + Delete) rather than call
	// rollbackArtifactSave because the production s.artifactStore is
	// the ctx-aware storage.ArtifactStore, while rollbackArtifactSave
	// takes the non-ctx service.ArtifactStoreInterface. The deployment
	// row is rolled back by the caller's tx (or compensated in the
	// no-tx path).
	hash, saveErr := s.artifactStore.SaveAndHash(ctx, tenantID, appName, depID, magicFile)
	_ = magicFile.Close()
	if saveErr != nil {
		if delErr := s.deploymentRepo.DeleteByID(ctx, depID); delErr != nil {
			log.Printf("rollback DeleteByID failed after artifact save error: deployment_id=%s error=%v", depID, delErr)
		}
		if delErr := s.artifactStore.Delete(ctx, tenantID, appName, depID); delErr != nil && !errors.Is(delErr, os.ErrNotExist) {
			log.Printf("rollback artifact.Delete failed after artifact save error: deployment_id=%s error=%v", depID, delErr)
		}
		return nil, fmt.Errorf("%w: saving artifact: %w", ErrMigrationFailed, saveErr)
	}
	deployment.Hash = hex.EncodeToString(hash)

	// Sign the artifact (issue #307). Mirrors `DeploymentService.Deploy`:
	// sign over `sha256(artifact) || deployment.ID` and stamp the
	// result + key id on the row. Done before the report is returned
	// because the deployment row is the persisted proof of the
	// signature — a report without a signed row would tell the
	// tenant "your code is deployed" while leaving the worker with
	// no way to verify it.
	if s.keyring == nil {
		return nil, fmt.Errorf("signing is not configured (migration service requires a keyring at construction)")
	}
	sig, kid, signErr := s.keyring.Sign(deployment.Hash, deployment.ID)
	if signErr != nil {
		return nil, fmt.Errorf("signing artifact: %w", signErr)
	}
	deployment.Signature = sig
	deployment.SigningKeyID = kid
	if updateErr := s.deploymentRepo.UpdateHashAndSignature(ctx, deployment); updateErr != nil {
		// Create runs once at line ~458 *before* SaveAndHash (so the
		// quota check fires against a real row); the post-SaveAndHash
		// fields (hash, signature, signing_key_id) are filled in by
		// this UPDATE. A failure here means the row was created but
		// the signature never reached disk — compensate by removing
		// both the row and the artifact so the tenant doesn't see a
		// "deployed" deployment that the worker can't verify.
		if delErr := s.deploymentRepo.DeleteByID(ctx, depID); delErr != nil {
			log.Printf("rollback DeleteByID failed after sign-then-update: deployment_id=%s error=%v", depID, delErr)
		}
		if delErr := s.artifactStore.Delete(ctx, tenantID, appName, depID); delErr != nil && !errors.Is(delErr, os.ErrNotExist) {
			log.Printf("rollback artifact.Delete failed after sign-then-update: deployment_id=%s error=%v", depID, delErr)
		}
		return nil, fmt.Errorf("updating deployment with hash and signature: %w", updateErr)
	}

	// Build success report from envelope's structured Report (HEAD),
	// overlaying fields the envelope doesn't carry (wasm_stored,
	// deployment_id, app_name).
	report := envelope.Report
	report.Status = domain.MigrationStatusSuccess
	report.WasmStored = true
	report.AppName = appName
	report.DeploymentID = &depID
	if len(report.PatternsTransformed) == 0 {
		report.PatternsTransformed = patternsTransformed
	}
	return &report, nil
}

// extForLanguage returns the canonical file extension for a language
// (used for temp-file naming and app-name derivation). Unknown
// languages default to ".c" so a test that calls Migrate with a
// blank `language` field still gets a sensible filename.
func extForLanguage(language string) string {
	switch language {
	case "rust":
		return ".rs"
	default:
		return ".c"
	}
}

// detectTransformedPatterns scans WASI C output for known WASI function names
// and returns a list of PatternInfo describing what was transformed.
func detectTransformedPatterns(wasiC string) []domain.PatternInfo {
	transforms := []struct {
		contains string
		pattern  string
		wasi     string
	}{
		{"wasi_socket_tcp_create", "socket(AF_INET, SOCK_STREAM, 0)", "wasi_socket_tcp_create"},
		{"wasi_socket_tcp_start_bind", "bind(fd, addr, len)", "wasi_socket_tcp_start_bind"},
		{"wasi_socket_tcp_start_listen", "listen(fd, backlog)", "wasi_socket_tcp_start_listen"},
		{"wasi_socket_tcp_accept", "accept(fd, ...)", "wasi_socket_tcp_accept"},
		{"wasi_socket_tcp_start_connect", "connect(fd, addr, len)", "wasi_socket_tcp_start_connect"},
		{"wasi_output_stream_write", "send(fd, buf, len, flags)", "wasi_output_stream_write"},
		{"wasi_input_stream_read", "recv(fd, buf, len, flags)", "wasi_input_stream_read"},
		{"wasi_filesystem_open", "fopen(path, mode)", "wasi_filesystem_open"},
		{"wasi_ip_name_lookup_resolve", "gethostbyname(name)", "wasi_ip_name_lookup_resolve"},
	}

	var patterns []domain.PatternInfo
	seen := make(map[string]bool)
	for _, t := range transforms {
		if strings.Contains(wasiC, t.contains) && !seen[t.pattern] {
			seen[t.pattern] = true
			patterns = append(patterns, domain.PatternInfo{
				Line:             0,
				Pattern:          t.pattern,
				Snippet:          t.pattern,
				WasiEquivalent:   t.wasi,
				Transformability: "Auto-transformable",
			})
		}
	}
	return patterns
}

// validateWasm checks whether b is a valid wasm binary (magic number check).
func validateWasm(b []byte) bool {
	return bytes.HasPrefix(b, []byte{0x00, 0x61, 0x73, 0x6d})
}

// detectTransformedPatternsRust is a heuristic string-scan fallback
// for the Rust `--analyze-json` path. It scans transformed Rust
// source for known WASI constructs and returns a list of
// PatternInfo describing what was transformed.
//
// **Status: defense-in-depth / effectively dead code.** It is only
// invoked when `edge-migrate --analyze --json` fails or returns
// unparseable output, which does not happen with `edge-migrate` >=
// v0.3 (the version that ships M3 support and always emits
// structured JSON). Kept as a last-resort fallback in case the
// subprocess fails to start or the operator is running a pre-v0.3
// binary.
//
// **Limitations:** does not parse the source; can produce false
// positives (a literal string "TcpSocket::new" in a comment would
// match). The lib's RustAnalyzer is the source of truth via
// `--analyze-json` in tree mode.
//
// **Tracked for removal** in v0.4 once we can guarantee a minimum
// `edge-migrate` version across all tenants. See M2 follow-up #2
// in the project tracker.
func detectTransformedPatternsRust(wasiRs string) []domain.PatternInfo {
	transforms := []struct {
		contains string
		pattern  string
		wasi     string
	}{
		{"TcpSocket::new", "std::net::TcpListener::bind", "wasi::socket::tcp::TcpSocket::new + start_bind + finish_bind + start_listen + finish_listen"},
		{"start_connect", "std::net::TcpStream::connect", "wasi::socket::tcp::TcpSocket::new + start_connect + finish_connect"},
		{"UdpSocket::new", "std::net::UdpSocket::bind", "wasi::socket::udp::UdpSocket::new + start_bind + finish_bind"},
		{"filesystem::open", "std::fs::File::open", "wasi::filesystem::open"},
		{"filesystem::read", "std::fs::read / read_to_string", "wasi::filesystem::read"},
		{"filesystem::write", "std::fs::write", "wasi::filesystem::write"},
	}

	var patterns []domain.PatternInfo
	seen := make(map[string]bool)
	for _, t := range transforms {
		if strings.Contains(wasiRs, t.contains) && !seen[t.pattern] {
			seen[t.pattern] = true
			patterns = append(patterns, domain.PatternInfo{
				Line:             0,
				Pattern:          t.pattern,
				Snippet:          t.pattern,
				WasiEquivalent:   t.wasi,
				Transformability: "Auto-transformable",
			})
		}
	}
	return patterns
}

// detectManualReviewPatternsC scans the transformed WASI source for
// POSIX call tokens that the transformer would have rewritten had a
// WASI equivalent existed. Any token still present means the
// transformer left the call verbatim → manual review.
//
// This is the manual-review counterpart to detectTransformedPatterns.
// When --analyze-json fails, the fallback runs both: the auto list
// and this list, then merges. The Rust analyzer's structured output
// remains the source of truth.
//
// The set of tokens mirrors the Rust analyzer's NotTransformable C
// variants (PosixPattern::transformability() in
// edge-migrate/edge-migrate-lib/src/patterns.rs:384-426). For the
// flag-based variants (O_NONBLOCK, SOCK_RAW) we scan for the flag
// tokens anywhere — false positives in plain C code are unlikely
// since these are rare identifiers. A future improvement could scope
// to a socket() arg list specifically.
//
// Limitations: does not skip comments or string literals (see
// posixCallPresent). The transformer itself emits only
// "// WASI: two-phase <verb>" comments without "(", so false
// positives require user-authored documentation comments containing
// POSIX call signatures — narrow edge case, deferred.
func detectManualReviewPatternsC(wasiSource string) []domain.PatternInfo {
	checks := []struct {
		token   string
		pattern string
		reason  string
	}{
		{"fork(", "Fork", "no WASI equivalent — fork has no WASI equivalent"},
		{"vfork(", "Fork", "no WASI equivalent — fork has no WASI equivalent"},
		{"poll(", "Poll", "no WASI equivalent — poll has no WASI equivalent"},
		{"select(", "Select", "no WASI equivalent — select has no WASI equivalent"},
		{"exec(", "Exec", "no WASI equivalent — exec has no WASI equivalent"},
		{"execve(", "Exec", "no WASI equivalent — exec has no WASI equivalent"},
		{"execl(", "Exec", "no WASI equivalent — exec has no WASI equivalent"},
		{"execvp(", "Exec", "no WASI equivalent — exec has no WASI equivalent"},
		{"socketpair(", "SocketPair", "no WASI equivalent — socketpair has no WASI equivalent"},
		{"shutdown(", "Shutdown", "no WASI equivalent — shutdown not in wasi-sockets"},
		{"accept(", "Accept", "TcpListener::accept() — not transformable in MVP (was: poll loop wrapper; #128)"},
		{"accept4(", "Accept", "TcpListener::accept() — not transformable in MVP (was: poll loop wrapper; #128)"},
		{"gethostbyname(", "GetHostByName", "no WASI equivalent — gethostbyname has no WASI equivalent"},
		{"getaddrinfo(", "GetHostByName", "no WASI equivalent — getaddrinfo has no WASI equivalent"},
		{"gethostbyaddr(", "GetHostByName", "no WASI equivalent — gethostbyaddr has no WASI equivalent"},
		{"O_NONBLOCK", "NonBlocking", "no WASI equivalent — O_NONBLOCK not in wasi-sockets"},
		{"SOCK_RAW", "SockRaw", "no WASI equivalent — SOCK_RAW not in wasi-sockets"},
	}
	seen := make(map[string]bool)
	var patterns []domain.PatternInfo
	for _, c := range checks {
		if seen[c.pattern] {
			continue
		}
		if posixCallPresent(wasiSource, c.token) {
			seen[c.pattern] = true
			patterns = append(patterns, domain.PatternInfo{
				Pattern:          c.pattern,
				Snippet:          c.token,
				WasiEquivalent:   c.reason,
				Transformability: domain.TransformabilityNotTransformable,
			})
		}
	}
	return patterns
}

// detectManualReviewPatternsRust is the Rust counterpart. It scans
// the transformed Rust source for NotTransformable Rust pattern
// markers (RustPattern::transformability() in
// edge-migrate/edge-migrate-lib/src/patterns.rs:460-474).
//
//   - ".accept(" on a TcpListener → TcpAccept. The dot precedes the
//     call; posixCallPresent's word-boundary check accepts "." as a
//     non-ident separator (it's not in [a-zA-Z0-9_]), so
//     posixCallPresent(wasiSource, ".accept(") correctly matches
//     method-call form and rejects identifier-suffix forms like
//     "myaccept(".
//   - "UdpSocket::connect" → UdpConnect. The transformer rewrites
//     TcpStream::connect to wasi_socket_tcp_start_connect, so any
//     ".connect(" left in the transformed source is by elimination
//     non-rewriteable. We match the literal "UdpSocket::connect"
//     rather than ".connect(" to avoid false positives on
//     other-method accept() etc.
//   - "std::process::exit" → ProcessExit.
func detectManualReviewPatternsRust(wasiSource string) []domain.PatternInfo {
	checks := []struct {
		token   string
		pattern string
		reason  string
	}{
		{".accept(", "TcpAccept", "TcpListener::accept() — not transformable in MVP (#128)"},
		{"UdpSocket::connect", "UdpConnect", "no WASI equivalent — UdpSocket::connect not in wasi-sockets"},
		{"std::process::exit", "ProcessExit", "no WASI equivalent — Wasm has no process model"},
	}
	var patterns []domain.PatternInfo
	seen := make(map[string]bool)
	for _, c := range checks {
		if seen[c.pattern] {
			continue
		}
		if posixCallPresent(wasiSource, c.token) {
			seen[c.pattern] = true
			patterns = append(patterns, domain.PatternInfo{
				Pattern:          c.pattern,
				Snippet:          c.token,
				WasiEquivalent:   c.reason,
				Transformability: domain.TransformabilityNotTransformable,
			})
		}
	}
	return patterns
}

// posixCallPresent reports whether `callToken` (e.g. "bind(", "fork(",
// ".accept(") appears in source as an actual call. The naive
// strings.Contains check matches "bind(" inside
// "wasi_socket_tcp_start_bind(", producing false positives for
// transformed source. The word-boundary check ensures we only match
// the real call form.
//
// The check is asymmetric:
//   - If callToken[0] is an identifier char (e.g. 'b' in "bind("),
//     the preceding byte must NOT be an identifier char — otherwise
//     it's a longer identifier like "mybind(" or "wasi_*bind(".
//   - If callToken[0] is a non-identifier char (e.g. '.' in
//     ".accept(" for Rust method calls), the leading non-ident char
//     already breaks any identifier continuation, so any preceding
//     byte is fine.
func posixCallPresent(source, callToken string) bool {
	firstIsIdent := isIdentChar(callToken[0])
	for i := 0; i+len(callToken) <= len(source); {
		j := strings.Index(source[i:], callToken)
		if j < 0 {
			return false
		}
		idx := i + j
		if firstIsIdent {
			if idx == 0 || !isIdentChar(source[idx-1]) {
				return true
			}
		} else {
			return true
		}
		i = idx + 1
	}
	return false
}

func isIdentChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') || b == '_'
}

// posixSnippetForPattern removed: its work is now done by
// detectManualReviewPatternsC / detectManualReviewPatternsRust, which
// scan the transformed WASI source directly for surviving POSIX/Rust
// call tokens instead of diffing against heuristic-detected patterns.

// MigrateTree analyzes + transforms every source file in `entries`
// together and compiles them into a single wasm binary. M2.C9
// (initial C path); M3.C7 added Rust.
//
// Per file, two subprocesses are run:
//  1. `edge-migrate --language <lang> --transform <path>` — produces
//     transformed (WASI) source
//  2. `edge-migrate --language <lang> --analyze-json <path>` —
//     produces a structured `MigrationReport` JSON used to populate
//     `FileReport.patterns_detected` / `transformations` /
//     `manual_review` and `preprocessor`.
//
// If `--analyze-json` fails (older edge-migrate binary), the
// service falls back to a string-scan heuristic on the transformed
// source: `detectTransformedPatterns` (C) or
// `detectTransformedPatternsRust` (Rust). A `// TODO` below flags
// the removal point once edge-migrate ≥ v0.3 ships everywhere.
//
// All transformed files are then compiled together in a single
// toolchain invocation:
//   - language == "c": clang `--target=wasm32-wasip2 -nostdlib
//     -I <tmpdir>` reading each transformed file by path
//   - language == "rust": rustc `--target wasm32-wasip2
//     --crate-type=cdylib` reading each transformed file by path
//
// The wasm size is checked against `MaxArtifactSize` and the
// artifact + deployment row are written only on success.
//
// Per-file errors (parse failure, transform failure) don't abort the
// rest of the tree — the file gets a `FileReport` with
// `status: Failed` and processing continues.
//
// `entries` paths must be forward-slash-relative to the tree root
// (the handler validates). The service enforces the same
// `IsValidAppName` regex as a defense-in-depth check.
func (s *MigrationService) MigrateTree(
	ctx context.Context,
	tenantID, appName, language string,
	entries []domain.FileEntry,
) (*domain.TreeMigrationReport, error) {
	// Defensive: handler also validates, but reject early here.
	if !IsValidAppName(appName) {
		return nil, fmt.Errorf("invalid app name: %q", appName)
	}
	// Belt-and-suspenders: handler rejects unknown languages, but
	// guard here too so internal callers (tests) can't bypass.
	if language == "" {
		language = "c"
	}
	if language != "c" && language != "rust" {
		return nil, fmt.Errorf("unsupported language: %q (only \"c\" and \"rust\" are supported)", language)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("no files in tree")
	}
	// Issue #415: multi-file Rust trees are not supported in the
	// cargo-based pipeline yet. The legacy bare-rustc path emitted
	// wasi:http@0.2.4 (rejected by wasmtime 45.0.3); the fix
	// requires a synthesized Cargo.toml + multi-file src/lib.rs
	// wrapper, which is a follow-up. Reject at the service boundary
	// (handler maps to 400). Single-file Rust is routed through
	// `Migrate` instead.
	if language == "rust" {
		return nil, fmt.Errorf("rust tree-mode migration is not supported; submit a single-file project via POST /api/v1/migrate")
	}

	// Create a temp dir for the source files + transformed output.
	tmpDir, err := os.MkdirTemp("", "migrate-tree-*.d")
	if err != nil {
		return nil, fmt.Errorf("creating temp dir: %w", err)
	}
	defer func() {
		if removeErr := os.RemoveAll(tmpDir); removeErr != nil {
			log.Printf("migration service: failed to remove temp dir: %v", removeErr)
		}
	}()

	// Write each entry to <tmpDir>/<path>. Reject path traversal
	// (defense-in-depth; handler also validates).
	type writtenFile struct {
		path        string
		absPath     string
		wasiCPath   string // populated after transform
		report      domain.FileReport
		transformOK bool
	}
	written := make([]writtenFile, 0, len(entries))

	for _, e := range entries {
		clean := filepath.Clean(e.Path)
		if clean == "." || clean == ".." ||
			strings.HasPrefix(clean, "/") ||
			strings.HasPrefix(clean, "..") ||
			strings.Contains(clean, "\\") {
			return nil, fmt.Errorf("invalid file path: %q", e.Path)
		}
		abs := filepath.Join(tmpDir, clean)
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			return nil, fmt.Errorf("creating dir for %q: %w", e.Path, err)
		}
		if err := os.WriteFile(abs, []byte(e.Source), 0o644); err != nil {
			return nil, fmt.Errorf("writing %q: %w", e.Path, err)
		}
		written = append(written, writtenFile{path: e.Path, absPath: abs})
	}

	// Per-file subprocess: transform + analyze-json.
	// Continues on per-file failure; failures are captured into
	// FileReport.errors and the file's status is set to Failed.
	for i := range written {
		wf := &written[i]

		// 1) `edge-migrate --language <lang> --transform <path>` → WASI output.
		// We write the transformed source to <path>.wasi.c in the
		// same dir so the final toolchain invocation can pick them
		// all up. (The `.wasi.c` suffix is a leftover from the C-only
		// era; for Rust the file contains Rust source. The compile
		// step dispatches on `language` and treats the file
		// accordingly.)
		edgeMigCmd := exec.CommandContext(ctx, s.edgeMigratePath, "--language", language, "--transform", wf.absPath)
		var edgeMigOut bytes.Buffer
		edgeMigCmd.Stdout = &edgeMigOut
		var edgeMigErr bytes.Buffer
		edgeMigCmd.Stderr = &edgeMigErr
		if err := edgeMigCmd.Run(); err != nil {
			wf.report = domain.FileReport{
				Path:   wf.path,
				Status: domain.MigrationStatusFailed,
				Errors: []domain.ErrorInfo{{
					Line:    0,
					Message: fmt.Sprintf("edge-migrate failed: %s — %s", err, edgeMigErr.String()),
				}},
			}
			continue
		}
		wasiSource := edgeMigOut.String()
		wasiCPath := wf.absPath + ".wasi.c"
		if err := os.WriteFile(wasiCPath, []byte(wasiSource), 0o644); err != nil {
			wf.report = domain.FileReport{
				Path:   wf.path,
				Status: domain.MigrationStatusFailed,
				Errors: []domain.ErrorInfo{{
					Line:    0,
					Message: fmt.Sprintf("writing wasi.c: %s", err),
				}},
			}
			continue
		}
		wf.wasiCPath = wasiCPath

		// 2) `edge-migrate --analyze-json <path>` → structured report.
		// On failure (older binary), fall back to a heuristic that's
		// language-aware: C → detectTransformedPatterns, Rust →
		// detectTransformedPatternsRust.
		analyzeCmd := exec.CommandContext(ctx, s.edgeMigratePath, "--language", language, "--analyze-json", wf.absPath)
		var analyzeOut bytes.Buffer
		analyzeCmd.Stdout = &analyzeOut
		var analyzeErr bytes.Buffer
		analyzeCmd.Stderr = &analyzeErr
		var single domain.MigrationReport
		analyzeOK := false
		if err := analyzeCmd.Run(); err == nil {
			if jerr := json.Unmarshal(analyzeOut.Bytes(), &single); jerr == nil {
				analyzeOK = true
			}
		}
		// TODO: remove this fallback once edge-migrate ≥ v0.3 ships
		// everywhere (M2 follow-up #2). The detectTransformedPatterns*
		// helpers are heuristics — they can't tell manual-review from
		// auto. The analyzer's structured output is the source of
		// truth.
		if !analyzeOK {
			var transformed, manualReview []domain.PatternInfo
			if language == "rust" {
				transformed = detectTransformedPatternsRust(wasiSource)
				manualReview = detectManualReviewPatternsRust(wasiSource)
			} else {
				transformed = detectTransformedPatterns(wasiSource)
				manualReview = detectManualReviewPatternsC(wasiSource)
			}
			// PatternsDetected is the union (transformed ∪ manualReview)
			// so the tenant sees every detected pattern; status is
			// classified from the union so a NotTransformable-only file
			// surfaces as Failed, matching the Rust analyzer's convention.
			combined := append(transformed, manualReview...)
			single = domain.MigrationReport{
				Status:               classifyFromPatterns(combined),
				WasmStored:           false,
				AppName:              appName,
				PatternsDetected:     combined,
				PatternsTransformed:  transformed,
				PatternsManualReview: manualReview,
				Errors:               nil,
			}
		}
		// Promote the single-file MigrationReport into a per-file FileReport.
		fr := domain.FileReport{
			Path:             wf.path,
			Status:           single.Status,
			PatternsDetected: single.PatternsDetected,
			Transformations:  single.PatternsTransformed,
			ManualReview:     single.PatternsManualReview,
			Errors:           single.Errors,
			Preprocessor:     single.Preprocessor,
		}
		wf.report = fr
		wf.transformOK = true
	}

	// Build the per-file reports (in input order) and compute tree status.
	files := make([]domain.FileReport, 0, len(written))
	for _, wf := range written {
		files = append(files, wf.report)
	}

	// Compute tree-level aggregates inline (matches the Rust
	// TreeMigrationReport::from_files rules).
	status := aggregateTreeStatus(files)
	filesTotal := len(files)
	filesTransformed := 0
	filesManualReview := 0
	for _, f := range files {
		if len(f.Transformations) > 0 {
			filesTransformed++
		}
		if len(f.ManualReview) > 0 {
			filesManualReview++
		}
	}

	// If any file failed transformation, we cannot compile a complete
	// wasm — return the per-file report as a tree-level failure.
	// Skip clang; just report the partial state.
	anyTransformFailed := false
	for _, wf := range written {
		if !wf.transformOK {
			anyTransformFailed = true
			break
		}
	}
	if anyTransformFailed {
		// Status is Failed: at least one file's transform subprocess died,
		// so no wasm is produced. Partial is reserved for analyzer-driven
		// classifications where the toolchain actually shipped an artifact.
		return &domain.TreeMigrationReport{
			Status:            domain.MigrationStatusFailed,
			WasmStored:        false,
			AppName:           appName,
			Files:             files,
			FilesTotal:        filesTotal,
			FilesTransformed:  filesTransformed,
			FilesManualReview: filesManualReview,
			Errors: []domain.ErrorInfo{{
				Line:    0,
				Message: "one or more files failed to transform; no wasm built",
			}},
		}, ErrMigrateTreeFailed
	}

	// Compile all transformed files in a single toolchain invocation.
	tmpWasm, err := os.CreateTemp("", "migrate-tree-*.wasm")
	if err != nil {
		return nil, fmt.Errorf("creating temp wasm: %w", err)
	}
	tmpWasmPath := tmpWasm.Name()
	if err := tmpWasm.Close(); err != nil {
		log.Printf("migration service: failed to close temp wasm: %v", err)
	}
	defer func() {
		if removeErr := os.Remove(tmpWasmPath); removeErr != nil && !os.IsNotExist(removeErr) {
			log.Printf("migration service: failed to remove temp wasm: %v", removeErr)
		}
	}()

	var compileErrMsg string
	// Issue #415: MigrateTree rejects language=="rust" at function
	// entry; only C reaches this compile switch.
	clangBin := filepath.Join(s.wasiSdkPath, "clang")
	args := []string{
		"--target=wasm32-wasip2", "-nostdlib",
		"-I", tmpDir,
		"-o", tmpWasmPath,
	}
	for _, wf := range written {
		args = append(args, wf.wasiCPath)
	}
	clangCmd := exec.CommandContext(ctx, clangBin, args...)
	var clangErrBuf bytes.Buffer
	clangCmd.Stderr = &clangErrBuf
	if err := clangCmd.Run(); err != nil {
		compileErrMsg = fmt.Sprintf("clang failed: %s — %s", err, clangErrBuf.String())
	}

	if compileErrMsg != "" {
		// Status is Failed: the toolchain refused to compile. Partial
		// is reserved for analyzer-driven classifications (some files
		// need manual review); here every file's analyzer-side result
		// is moot because the resulting wasm is unrunnable.
		return &domain.TreeMigrationReport{
			Status:            domain.MigrationStatusFailed,
			WasmStored:        false,
			AppName:           appName,
			Files:             files,
			FilesTotal:        filesTotal,
			FilesTransformed:  filesTransformed,
			FilesManualReview: filesManualReview,
			Errors: []domain.ErrorInfo{{
				Line:    0,
				Message: compileErrMsg,
			}},
		}, ErrMigrateTreeFailed
	}

	// Stat the compiled wasm for the size cap. Avoids buffering the
	// full artifact (up to MaxArtifactSize = 100 MiB) into RAM just
	// to read its length — the streaming SaveAndHash below hashes
	// and writes the file in a single pass.
	info, err := os.Stat(tmpWasmPath)
	if err != nil {
		return nil, fmt.Errorf("stat compiled wasm: %w", err)
	}
	if info.Size() > MaxArtifactSize {
		return &domain.TreeMigrationReport{
			Status:            domain.MigrationStatusFailed,
			WasmStored:        false,
			AppName:           appName,
			Files:             files,
			FilesTotal:        filesTotal,
			FilesTransformed:  filesTransformed,
			FilesManualReview: filesManualReview,
			Errors: []domain.ErrorInfo{{
				Line:    0,
				Message: fmt.Sprintf("wasm exceeds %d bytes (MaxArtifactSize)", MaxArtifactSize),
			}},
		}, ErrMigrateTreeFailed
	}
	// Peek the wasm magic bytes from the file directly (4 bytes is
	// enough; the spec's 8-byte header is just magic + version, and
	// a bad magic always means a bad file). Avoids a full
	// os.ReadFile on a 100 MiB blob.
	magicFile, err := os.Open(tmpWasmPath)
	if err != nil {
		return nil, fmt.Errorf("opening compiled wasm: %w", err)
	}
	var magic [4]byte
	if _, err := io.ReadFull(magicFile, magic[:]); err != nil {
		_ = magicFile.Close()
		return nil, fmt.Errorf("reading wasm magic: %w", err)
	}
	if !bytes.HasPrefix(magic[:], []byte{0x00, 0x61, 0x73, 0x6d}) {
		_ = magicFile.Close()
		// Same Failed semantics as the compile-failure branch above —
		// the toolchain emitted bytes that don't have the wasm magic;
		// per-file analyzer status is irrelevant.
		return &domain.TreeMigrationReport{
			Status:            domain.MigrationStatusFailed,
			WasmStored:        false,
			AppName:           appName,
			Files:             files,
			FilesTotal:        filesTotal,
			FilesTransformed:  filesTransformed,
			FilesManualReview: filesManualReview,
			Errors: []domain.ErrorInfo{{
				Line:    0,
				Message: "compiled artifact failed wasm magic-number check",
			}},
		}, ErrMigrateTreeFailed
	}
	// Rewind so SaveAndHash below reads from byte 0, not byte 4.
	if _, err := magicFile.Seek(0, io.SeekStart); err != nil {
		_ = magicFile.Close()
		return nil, fmt.Errorf("rewinding compiled wasm: %w", err)
	}

	// Persist: deployment row + artifact blob.
	depID := "d_" + uuid.New().String()
	deployment := &domain.Deployment{
		ID:        depID,
		TenantID:  tenantID,
		AppName:   appName,
		Status:    "migrated",
		Hash:      "",
		CreatedAt: time.Now(),
	}
	if err := s.deploymentRepo.Create(ctx, deployment); err != nil {
		_ = magicFile.Close()
		return nil, fmt.Errorf("creating deployment: %w", err)
	}
	// Stream the artifact to disk and compute the SHA-256 in a single
	// pass. See the rollback comment in MigrationService.Migrate for
	// why the blob is cleaned up here. We inline the rollback
	// (DeleteByID + Delete) rather than call rollbackArtifactSave
	// because the production s.artifactStore is the ctx-aware
	// storage.ArtifactStore, while rollbackArtifactSave takes the
	// non-ctx service.ArtifactStoreInterface.
	hash, saveErr := s.artifactStore.SaveAndHash(ctx, tenantID, appName, depID, magicFile)
	_ = magicFile.Close()
	if saveErr != nil {
		if delErr := s.deploymentRepo.DeleteByID(ctx, depID); delErr != nil {
			log.Printf("rollback DeleteByID failed after artifact save error: deployment_id=%s error=%v", depID, delErr)
		}
		if delErr := s.artifactStore.Delete(ctx, tenantID, appName, depID); delErr != nil && !errors.Is(delErr, os.ErrNotExist) {
			log.Printf("rollback artifact.Delete failed after artifact save error: deployment_id=%s error=%v", depID, delErr)
		}
		return nil, fmt.Errorf("%w: saving artifact: %w", ErrMigrateTreeFailed, saveErr)
	}
	deployment.Hash = hex.EncodeToString(hash)

	// Sign the artifact (issue #307). Mirrors `Migrate` and
	// `DeploymentService.Deploy` — sign over
	// `sha256(artifact) || deployment.ID` and persist the
	// signature + key id on the row. The original code created
	// the row with an empty hash and never updated it, so this
	// is the first time the row carries both hash and signature.
	// The Create call below is a no-op insert (row already
	// exists) and will fail; to make this work we have to use a
	// real Update path. Since `DeploymentRepoInterface` only
	// exposes Create, we AddUpdate here as well (see below).
	if s.keyring == nil {
		return nil, fmt.Errorf("signing is not configured (migration service requires a keyring at construction)")
	}
	sig, kid, signErr := s.keyring.Sign(deployment.Hash, deployment.ID)
	if signErr != nil {
		return nil, fmt.Errorf("signing artifact: %w", signErr)
	}
	deployment.Signature = sig
	deployment.SigningKeyID = kid
	if err := s.deploymentRepo.UpdateHashAndSignature(ctx, deployment); err != nil {
		if delErr := s.deploymentRepo.DeleteByID(ctx, depID); delErr != nil {
			log.Printf("rollback DeleteByID failed after sign-then-update: deployment_id=%s error=%v", depID, delErr)
		}
		if delErr := s.artifactStore.Delete(ctx, tenantID, appName, depID); delErr != nil && !errors.Is(delErr, os.ErrNotExist) {
			log.Printf("rollback artifact.Delete failed after sign-then-update: deployment_id=%s error=%v", depID, delErr)
		}
		return nil, fmt.Errorf("updating deployment with hash and signature: %w", err)
	}

	return &domain.TreeMigrationReport{
		Status:            status,
		WasmStored:        true,
		DeploymentID:      &depID,
		AppName:           appName,
		Files:             files,
		FilesTotal:        filesTotal,
		FilesTransformed:  filesTransformed,
		FilesManualReview: filesManualReview,
	}, nil
}

// classifyFromPatterns maps a list of detected patterns to a
// MigrationStatus. Mirrors the Rust MigrationReport::from_pattern_matches
// rule: empty manual_review → Success; only manual_review → Failed;
// mixed → Partial. Used by the detectTransformedPatterns fallback
// path when --analyze --json is unavailable.
func classifyFromPatterns(patterns []domain.PatternInfo) domain.MigrationStatus {
	hasTransformed := false
	hasManual := false
	for _, p := range patterns {
		if p.Transformability == "NotTransformable" || p.Transformability == "Not-transformable" {
			hasManual = true
		} else {
			hasTransformed = true
		}
	}
	switch {
	case !hasManual:
		return domain.MigrationStatusSuccess
	case !hasTransformed:
		return domain.MigrationStatusFailed
	default:
		return domain.MigrationStatusPartial
	}
}

// aggregateTreeStatus mirrors the Rust TreeMigrationReport::from_files
// rules: any Failed → Failed; any Partial → Partial; else Success.
func aggregateTreeStatus(files []domain.FileReport) domain.MigrationStatus {
	if len(files) == 0 {
		return domain.MigrationStatusSuccess
	}
	anyFailed := false
	anyPartial := false
	for _, f := range files {
		switch f.Status {
		case domain.MigrationStatusFailed:
			anyFailed = true
		case domain.MigrationStatusPartial:
			anyPartial = true
		case domain.MigrationStatusSuccess:
		}
	}
	switch {
	case anyFailed:
		return domain.MigrationStatusFailed
	case anyPartial:
		return domain.MigrationStatusPartial
	default:
		return domain.MigrationStatusSuccess
	}
}
