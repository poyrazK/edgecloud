# `edge-migrate` — Security Deny-List Semantics

**Audience:** security reviewers, future contributors extending the analyzer, operators auditing wire output from `POST /api/v1/migrate*`.
**Scope:** what the Rust and C analyzers deny at compile-time, how denials are surfaced, and which operator-side hooks need to fire when the deny-list rejects an upload.

## Threat model

The control plane compiles tenant-supplied source on the CP host. The compile step can bake host resources into the produced wasm before any tenant-side sandbox (Wasmtime, `wasi:cloud/*`) applies. The relevant host-reach primitives:

- **Rust** — compile-time macros that bake host files (`include_bytes!`, `include_str!`, `include!`), host env (`env!`, `option_env!`), or arbitrary error messages (`compile_error!`) into the wasm.
- **Rust attributes** — `#[path = "..."]` and `#[include = "..."]` change module resolution and reach host files at compile time.
- **C / C23** — `#include "..."` and `#include <...>` resolve against the host filesystem when the compiler's include path is not constrained; C23 `#embed "..."` is a direct host-file read primitive.

The deny-list in `edge-migrate` is the **authoritative** AST-precise rejection for these patterns. The Go control plane also runs a **handler preflight** regex (cheap, loose) before invoking `edge-migrate` — see `edge-control-plane/docs/security/migration-sandbox.md` for the layer ordering. This document covers L2 only.

## Denied patterns

### Rust

`edge-migrate-lib/src/rust_analyzer.rs::RustAnalyzer::match_deny` emits `ErrorInfo { code: "SECURITY_DENY:RUST_MACRO", ... }` on every hit:

| Macro | Reaches | Why denied |
|---|---|---|
| `include_bytes!(...)` | Host file → `&[u8]` literal | Exfiltrates the file contents into the wasm |
| `include_str!(...)` | Host file → `&str` literal | Exfiltrates text files (signing keys, configs) |
| `include!(...)` | Host file → expanded source | Allows arbitrary host-source inclusion (`include!("/etc/edgecloud/something.rs")`) |
| `env!(...)` | Host env var → `&str` literal | Exfiltrates any env var visible to the compile process |
| `option_env!(...)` | Host env var → `Option<&str>` literal | Same as `env!`, optional form |
| `compile_error!(...)` | Host-side error message | `cargo` prints the message to stderr; the CP captures stderr and returns it in `MigrationReport.errors[].message` (issue #622 commit 4 narrowed this via env scrubbing, but the message body is still attacker-controlled) |

| Attribute | Reaches | Why denied |
|---|---|---|
| `#[path = "..."]` | Host file → module source | Lets the compiler read an arbitrary host file as a Rust module |
| `#[include = "..."]` | Host file → injected via `include_str!` | Side-channel for the same exfiltration as `include_str!` |

The matcher enforces identifier boundaries — `my_include_bytes_helper!()` and `let env = "...";` do not match. Comments containing the literal `include_bytes!` are not matched (regex is line-prefix anchored on `^` plus word boundary).

### C

`edge-migrate-lib/src/analyzer.rs::pre_pass_deny_c` emits `ErrorInfo { code: "SECURITY_DENY:C_INCLUDE", ... }` on every hit. The pre-pass is a regex scan over the raw source that runs **before** tree-sitter parsing, so it also catches includes hidden behind `#define` macros (the `clang -E -nostdinc` preprocessor expansion at commit 1 is the upstream step that makes the pre-pass see those).

Rejected paths:

- `#include "/abs/path"` — absolute quoted include
- `#include <...>` containing a `..` segment — system header traversal
- `#include "../../rel/path"` — quoted relative-traversal
- `#include "./explicit/rel"` — explicit-traversal prefix
- `#embed "/abs/path"` — C23 file-embed (any path; tree mode is the legitimate include path)
- `#embed "../../rel"` — relative-traversal form

The path-rejection logic lives in `edge-migrate-lib/src/analyzer.rs::is_deny_c_path`. Pure `stdio.h` and `#include "relative.h"` (no leading `./`, no `..` segment, no absolute prefix) are allowed.

## Wire shape

Every deny-list hit appends an `ErrorInfo` to `MigrationReport.errors[]` (single-file mode) or `TreeMigrationReport.files[].errors[]` (tree mode). The shape, additive over the pre-issue-#622 schema:

```json
{
  "code": "SECURITY_DENY:RUST_MACRO",
  "message": "denied: include_bytes!() reads host files at compile time",
  "line": 12,
  "column": 4
}
```

| Field | Type | Notes |
|---|---|---|
| `code` | string | `"SECURITY_DENY:RUST_MACRO"` or `"SECURITY_DENY:C_INCLUDE"`. The `SECURITY_DENY:` prefix is a stable operator hook — Prometheus alerts and log search rules should match on the prefix, not the suffix, so the deny-list can grow without alert-config drift. |
| `message` | string | Human-readable description of the violation. Treated as informational only; do not parse. |
| `line`, `column` | uint | Source location. Always 1-indexed. |

`MigrationReport.status` is `failed` when *any* `ErrorInfo` is present (or when a non-security analyzer failure occurred). The CP's `MigrationService.Migrate` checks `envelope.Report.Status == "failed"` and short-circuits the compile step — without that guard, the deny-list would only *report* the macro, and the compile would still run and exfiltrate. See commits 1 (single-file) and 2 (tree-mode) of PR #668.

## Layer ordering (defense-in-depth)

The deny-list is one of five layers closing the host-reach vector. They are intentionally redundant, not duplicates:

| Layer | Where | Catches |
|---|---|---|
| **L1** handler preflight | `internal/handler/migration_preflight.go` | Cheap regex reject at the HTTP boundary, before any subprocess |
| **L2** analyzer deny-list | **This document** | AST-precise, every call path (HTTP, internal callers, future tooling) |
| **L3** env scrubbing | `internal/service/migration.go::scrubbedEnv` | Strips `JWT_SECRET` / `EDGE_SIGNING_KEY` / `DATABASE_PASSWORD` from the subprocess env |
| **L4** clang hardening | `internal/service/migration.go::clangArgs` (`-nostdinc --sysroot`) | Blocks host-filesystem `#include` / `#embed` at the C compile step |
| **L5** operator sandbox | `edge-control-plane/docs/security/migration-sandbox.md` | nsjail / bwrap / gVisor around the toolchain process |

L1 is fast and loose. L2 is slow and precise. L3 + L4 are independent defenses that mitigate even if a future regression bypasses L1 or L2. L5 is operator-deployed and not in the code PR.

## Adding a new pattern

When the security audit identifies a new host-reach primitive:

1. Add the `ErrorInfo.code` value as a new `pub const DENY_CODE_*` in `rust_analyzer.rs` or `analyzer.rs`. Use the `SECURITY_DENY:<UPPER_SNAKE>` shape so the prefix-based operator hook still matches.
2. Implement the matcher. Mirror the existing `match_deny_macro` (Rust) or `pre_pass_deny_c` (C) shape — identifier-boundary check, line/column metadata, no first-match early exit (collect every violation).
3. Add the corresponding constant in `edge-control-plane/internal/handler/migration_preflight.go::preflightReason*` and update `edge-control-plane/internal/service/metrics.go::migratePreflightReasons` — the preflight metric label set must include the new reason so L1 rejections are observable in Prometheus.
4. Update both `TestMigratePreflight_AllReasonsCovered` (metric drift-guard) and the existing analyzer test tables.
5. Cross-check `edge-control-plane/internal/service/migration.go::denyCodePrefix` — the operator-side hook should match on the new code via the `SECURITY_DENY:` prefix, no Go-side change needed.
6. Update `edge-control-plane/docs/security/migration-sandbox.md` if the new pattern affects the operator threat model (e.g. if it would survive `-nostdinc --sysroot`, document the residual sandbox requirement).

## Testing

- `edge-migrate/edge-migrate-lib/src/rust_analyzer.rs::tests` (asserts `DENY_CODE_RUST_MACRO` is set, plus positive + negative fixtures).
- `edge-migrate/edge-migrate-lib/src/analyzer.rs::deny_tests` (asserts `DENY_CODE_C_INCLUDE` is set, plus positive + negative fixtures for absolute, traversal, embed, and the negative `stdio.h`/`relative.h` cases).
- `edge-control-plane/internal/service/migration_test.go::TestMigrate_RustAnalyzerFailure_SkipsCompile` — short-circuit guarantee: an analyzer `Status: failed` does not invoke `compileRustAsComponent`.
- `edge-control-plane/internal/service/migration_test.go::TestMigrateTree_CAnalyzerFailure_SkipsCompile` — tree-mode short-circuit guarantee.
- `edge-control-plane/internal/service/metrics_test.go::TestMigratePreflight_AllReasonsCovered` — drift guard against `migratePreflightReasons` drift.

## References

- PR #668 — `fix/issue-622-migration-exfil`. The 6-commit sequence lands L1–L5.
- Issue #622 — original security audit finding.
- PR #416 — closed the proc-macro / `build.rs` RCE vector. Combined with L2 deny-list, even a malicious `Cargo.toml` cannot run arbitrary build scripts.
- `edge-control-plane/docs/security/migration-sandbox.md` — operator deployment contract for L5.
- `edge-migrate/docs/design.md` §2.2 — clang preprocessor invocation (`-nostdinc -E`) that surfaces hidden `#define` macros to L2.