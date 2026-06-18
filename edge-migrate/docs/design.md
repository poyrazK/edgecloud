# edge-migrate Design

> **Status:** Draft v0.2
> **Date:** 2026-06-18
> **Owner:** edgeCloud team
>
> **v0.2 changes:** Added §2.2 paragraph on the C preprocessor (always-on when `clang` is reachable, silent fallback to unexpanded source). The `MigrationReport` and `TransformResult` now carry a `preprocessor: Option<PreprocessorInfo>` field.

---

## 1. Overview

`edge-migrate` is the migration pipeline for edgeCloud. It accepts C source files containing POSIX patterns, transforms them to WASI-compatible code, and compiles the result to a `.wasm` binary stored on edgeCloud's infrastructure.

**The wasm binary never leaves edgeCloud.** The developer receives a transformation report (what was auto-converted, what couldn't be) and can proceed to `edge deploy` if migration succeeded.

```
Developer machine                    edgeCloud
─────────────────                    ────────
edge migrate hello_world.c
  → Uploads hello_world.c
  → App name derived: "hello_world"
  → edgeCloud:
      - Static analysis (AST + pattern matching)
      - Auto-transformation of safe POSIX → WASI patterns
      - Compilation to wasm32-wasip2
      - Stores wasm at /registry/{tenant_id}/hello_world/{deployment_id}.wasm
  → Returns transformation report + deployment_id
  → Developer runs edge deploy hello_world --id d_xyz789
```

---

## 2. Design Decisions

### 2.1 Binary + Library-first

The tool ships as both:
- **`edge-migrate` binary** — standalone CLI, uploads source, displays report
- **`edge-migrate-lib` library** — core analysis + transformation engine, reusable for IDE plugins, LSP, future integrations

Crate lives under `edgeCloud/edge-migrate/` as its own workspace member. Installed via `cargo install edge-migrate`.

### 2.2 Language Scope

| Language | Phase 1 Support |
|----------|----------------|
| C | ✅ Full analysis + auto-transform for safe patterns (with preprocessor expansion — see below) |
| Rust | Analysis-only (`std::net` detection + suggestions) |
| Other | Future extension |

Safe patterns are defined in §4.

**C preprocessor expansion.** POSIX patterns are routinely hidden behind
project-internal macros: `#define socket(f, t, p) make_socket(f, t, p)` is
indistinguishable from a user-defined function to a tree-sitter parse. To
catch these, the analyzer runs the source through `clang -E -nostdinc`
before tree-sitter analysis whenever a `Preprocessor` is attached.

A preprocessor is **always-on when `clang` is reachable** (PATH lookup,
falling back to `$WASI_SDK_PATH/bin/clang`). When reachable, patterns
hidden behind macros become visible to the analyzer; report `line`
fields are remapped to the **original** source line via the
preprocessor's `line_map`. When `clang` is not reachable, the analyzer
**silently falls back to the unexpanded source** — analysis never fails
because the preprocessor is missing. A `tracing::warn!` is logged on
fallback.

**Limitations** (documented for honesty):

- `clang -E` emits `# <lineno> "<file>"` linemarkers only at file
  boundaries, not at every source line. The remap is therefore
  best-effort: matches on synthetic lines (no preceding user-file
  linemarker) keep their expanded line number. For most real-world
  C code this is invisible because the re-entry linemarker for the
  user file is emitted near the top of the expanded source.
- `-nostdinc` means project-internal headers are not auto-included.
  A future `--include-dir` flag will close this gap; tracked as a
  follow-up issue.
- The macro count in the report is a best-effort estimate from
  counting `#define` directives in the original source, not an
  authoritative expansion count from clang.

Preprocessor metadata (clang version, files processed, macro count)
is attached to `MigrationReport.preprocessor` and `TransformResult.preprocessor`.

### 2.3 Source Upload Model

The CLI supports two upload modes:

1. **Single file** — `edge migrate <file.c>` POSTs the file to `POST /api/migrate`. App name is derived from the file stem (`hello_world.c` → `hello_world`).
2. **Directory / tree** — `edge-migrate --tree <DIR> [--app-name NAME]` walks the directory for `.c`/`.h` files (skipping `build/`, `target/`, `node_modules/`, etc.) and POSTs the whole tree to `POST /api/migrate-tree`. The developer supplies an explicit `--app-name` that must match `^[a-z0-9][a-z0-9-]{0,62}$`. Without `--app-name`, the dir basename is used.

In tree mode, all transformed `.c` files are compiled together in a single clang invocation (`--target=wasm32-wasip2 -nostdlib -I <tmpdir>`) and produce one wasm binary. The server response includes a per-file `FileReport` with the patterns detected, transformations applied, and any errors.

**App name derivation (single-file mode):** The app name is derived from the uploaded filename (without extension). For example, `edge migrate hello_world.c` sets the app name to `hello_world`. This is stored in the deployment record and used by `edge deploy`.

For directory mode, the explicit `--app-name` is required to be a valid `^[a-z0-9][a-z0-9-]{0,62}$` name (defense-in-depth + DNS-safety for the eventual `*.edgecloud.dev` URL). See §6.1.2 for the full request/response shape.

**Standalone binary:** `edge-migrate` is its own binary, not a subcommand of `edge-cli`. Developers install it separately: `cargo install edge-migrate`.

### 2.4 In-place vs. Output

`edge migrate` does **not** modify the source file. It does **not** produce a local transformed file. The source stays untouched. The transformed source + compiled wasm lives on edgeCloud.

### 2.5 Authentication

The CLI requires the user to be authenticated (`edge auth login` or equivalent). The wasm binary is stored under the authenticated tenant's account.

---

## 3. Transformation Report

The migration result is a structured report returned to the developer.

### 3.1 Success

```
✅ Migration successful

Binary stored. Run `edge deploy` to go live.

Transformations applied:
  • Line 15: socket(AF_INET, SOCK_STREAM, 0) → create-tcp-socket(ipv4)
  • Line 23: connect() → start-connect() + finish-connect()
  • Line 31: recv() → input-stream read via wasi:io/streams

Manual review required (1):
  • Line 67: poll() — not transformable.
    Suggestion: Use wasi:poll or restructure your event loop.

Auto-transformable patterns: 3
  Total patterns detected: 4

Preprocessor: 1 files processed, 3 macros expanded
  (Apple clang version 17.0.0 (clang-1700.3.19.1))
```

The `Preprocessor` line is omitted when no preprocessor is reachable or
the analyzer falls back to the unexpanded source. It is informational
only — the transformation result does not depend on the preprocessor
having run.

### 3.2 Failure

```
❌ Migration failed

The following patterns could not be auto-transformed:

  • Line 42: poll() — not transformable.
    This server uses event-loop multiplexing which has no WASI equivalent.
    Recommendation: Rewrite the poll loop to use wasi:poll or switch to a
    sequential accept() pattern.

  • Line 58: socketpair() — not transformable.
    Recommendation: Use two separate create-tcp-socket() calls.

Fix these issues and re-run `edge migrate <file>`.
```

---

## 4. Transformable Patterns

### 4.1 Auto-Transformable (Safe)

These patterns are mechanically derivable and safe to auto-transform.

| POSIX Pattern | WASI Equivalent | Notes |
|---|---|---|
| `socket(AF_INET, SOCK_STREAM, 0)` | `create-tcp-socket(ipv4)` | Single socket creation |
| `socket(AF_INET, SOCK_DGRAM, 0)` | `create-udp-socket(ipv4)` | UDP socket creation |
| `bind()` | `start-bind() + finish-bind()` | Two-phase, sequential server |
| `listen()` | `start-listen() + finish-listen()` | Two-phase |
| `accept()` | `accept()` → poll loop wrapper | Auto-wrap blocking accept in poll loop |
| `connect()` (single, no timeout) | `start-connect() + finish-connect()` | One-shot connection |
| `recv()` / `read()` on socket | `input-stream` read via wasi:io | Stream-based I/O |
| `send()` / `write()` on socket | `output-stream` write via wasi:io | Stream-based I/O |
| `gethostbyname()` / `getaddrinfo()` | `wasi:ip-name-lookup` | DNS resolution interface |
| `close()` on socket | `drop()` on socket resource | Resource cleanup |
| `fopen/fread/fwrite/fclose` | WASI filesystem | Maps cleanly to wasi:filesystem |

### 4.2 Transformable (Best-Effort, Auto-Wrap Blocking Calls)

These patterns require a poll-loop wrapper to become non-blocking. The tool can auto-generate the wrapper but the resulting code needs developer review.

| POSIX Pattern | Challenge |
|---|---|
| Sequential `accept()` in a loop | `accept()` is non-blocking — inject poll loop |
| `recv()` in a loop | Wrap in poll loop |
| `send()` in a loop | Wrap in poll loop |

### 4.3 Not Transformable (Analysis-Only)

These patterns have no WASI equivalent or require fundamental architectural changes. The tool reports them with a `manual review required` annotation but does not attempt auto-transform.

| POSIX Pattern | Reason |
|---|---|
| `poll()` / `select()` | No WASI equivalent — requires event-loop restructuring |
| `fork()` / `vfork()` | Wasm has no process model |
| `exec()` | Wasm has no process model |
| `socketpair()` | No WASI equivalent |
| `O_NONBLOCK` / non-blocking mode | WASI sockets are always non-blocking |
| `SOCK_RAW` | Raw sockets not supported in WASI |
| `shutdown()` (full-duplex) | Not in wasi-sockets |

---

## 5. Architecture

### 5.1 Crate Structure

```
edge-migrate/
├── edge-migrate-lib/           # Core library (analysis + transformation)
│   ├── src/
│   │   ├── lib.rs
│   │   ├── analyzer.rs         # C AST analysis (tree-sitter)
│   │   ├── transformer.rs      # POSIX → WASI transformation
│   │   ├── patterns.rs         # Pattern definitions
│   │   └── report.rs           # Structured report types
│   └── Cargo.toml
├── edge-migrate-bin/           # CLI binary wrapper
│   ├── src/
│   │   ├── main.rs             # CLI entry point (clap)
│   │   ├── upload.rs           # HTTP upload to migration endpoint
│   │   └── report.rs           # Display transformation report
│   └── Cargo.toml
└── Cargo.toml                  # Workspace manifest
```

### 5.2 Server-Side Pipeline (edgeCloud Control Plane)

The migration endpoint (`POST /api/migrate`) performs:

1. **Receive** — Accept the uploaded C source file (single-file mode)
2. **Analyze** — Run `edge-migrate-lib` analysis (AST + pattern matching, with preprocessor if `clang` is reachable)
3. **Transform** — Apply auto-transformations for safe patterns
4. **Compile** — Compile transformed C to wasm32-wasip2 via `clang`
5. **Store** — Store wasm binary under tenant + app (or temporary store for deploy)
6. **Report** — Return structured transformation report to CLI

**Tree mode (`POST /api/migrate-tree`, M2):** Accepts a multipart form with one `file` part per source plus a `tree` JSON manifest, OR a single `tree` part with `Content-Type: application/zip`. For each `.c` file:

1. Run `edge-migrate --transform <path>` → WASI C
2. Run `edge-migrate --analyze --json <path>` → structured `MigrationReport` JSON (used to populate per-file `FileReport.patterns_detected` / `transformations` / `manual_review` and `preprocessor`)
3. On `--analyze --json` failure (older binary), fall back to the `detectTransformedPatterns` heuristic

All transformed C files are compiled together in a single clang invocation. The wasm size is checked against `MaxArtifactSize` (100 MiB); oversized builds return a `Failed` tree report with an error entry. The artifact + deployment row are written only on success.

### 5.3 Pattern Engine

The core engine uses **tree-sitter** for AST-level analysis:

- **C parser** (`tree-sitter-c`) — parses C source to AST
- **Pattern matcher** — matches known POSIX socket/file I/O patterns against AST
- **Transformer** — rewrites matched patterns to WASI equivalents
- **Report generator** — produces structured report (JSON) for the CLI

### 5.4 Compiler

The server uses the **wasi-sdk** — Bytecode Alliance's official WASI SDK (pre-configured clang + wasi-libc targeting wasm32-wasip2).

```bash
/path/to/wasi-sdk/bin/clang --target=wasm32-wasip2 \
      -nostdlib \
      -o output.wasm \
      transformed.c
```

**Why wasi-sdk over raw clang:**
- Pre-built wasm32-wasip2 target + wasi-libc in one download (~200MB)
- No toolchain setup on the server
- Maintained by Bytecode Alliance, matches the same toolchain `edge-runtime` uses
- `emcc` is avoided because it does its own POSIX→WASI transformation internally, which would conflict with `edge-migrate`'s transformation

**Artifact storage:** Compiled wasm is stored in the same registry as `edge deploy`:
```
/registry/{tenant_id}/{app_name}/{deployment_id}.wasm
```
The deployment is **not** auto-activated. The developer runs `edge deploy` to activate it.

---

## 6. API

> **Note:** The `POST /api/migrate` endpoint is not yet defined in the control plane. This section specifies the required interface — the control plane team will implement it.

### 6.1 CLI → Server

```
POST /api/migrate
Authorization: Bearer <api-key>
Content-Type: multipart/form-data

Fields:
  - file: <source.c>
  - filename: <original-filename>
  - language: "c" | "rust"

Response: 200 OK
{
  "status": "success" | "failed",
  "wasm_stored": true | false,
  "deployment_id": "d_<uuid>" | null,
  "report": {
    "patterns_detected": [...],
    "patterns_transformed": [...],
    "patterns_manual_review": [...],
    "errors": [...]
  }
}
```

**Response fields:**
- `status`: `"success"` if wasm was stored, `"failed"` if migration could not complete
- `wasm_stored`: whether the wasm binary was saved to the registry
- `deployment_id`: the deployment ID assigned (same format as `edge deploy`). Used by `edge deploy --id` to activate.
- `app_name`: the app name derived from the uploaded filename (e.g., `hello_world` from `hello_world.c`)
- `report`: transformation report (see §3)

### 6.1.2 Tree Upload — `POST /api/migrate-tree`

Accepts a multi-file C project. Two wire formats are supported; the
handler dispatches based on which `tree` form part is present:

**Variant A — multipart parts** (preferred for small projects):

```
POST /api/migrate-tree
Authorization: Bearer <api-key>
Content-Type: multipart/form-data

Fields:
  - app_name: <string>             (required; regex ^[a-z0-9][a-z0-9-]{0,62}$)
  - language: "c"                  (required; only "c" is accepted in M2)
  - tree: <json string>            (required: {"files": ["src/main.c", ...]})
  - file: <binary>                 (one per entry in `tree.files`; the
                                   multipart part's filename = the
                                   relative path from the manifest)
```

The handler validates that every entry in `tree.files` has a
matching `file` part and that no path contains `..`, starts with `/`,
or contains a backslash. Mismatch → `400`.

**Variant B — zip archive** (preferred for projects with many files):

```
POST /api/migrate-tree
Authorization: Bearer <api-key>
Content-Type: multipart/form-data

Fields:
  - app_name: <string>             (required; same regex as variant A)
  - language: "c"                  (required)
  - tree: <binary application/zip> (required; the directory tree zipped;
                                   entries are the source of truth;
                                   `tree` JSON manifest is not parsed)
```

Zip entries are read in order. Non-`.c`/`.h` entries are skipped.
Entry names are validated against `isSafeFilePath` (rejects `..`,
absolute paths, backslashes, and Windows drive letters — zip-slip
protection). Cap: 256 files, 50 MiB decompressed.

**Response: 200 OK**

```json
{
  "status": "success" | "partial" | "failed",
  "wasm_stored": true | false,
  "deployment_id": "d_<uuid>" | null,
  "app_name": "tree_project",
  "files": [
    {
      "path": "src/main.c",
      "status": "success" | "partial" | "failed",
      "patterns_detected": [ ...PatternInfo... ],
      "transformations":  [ ...PatternInfo... ],
      "manual_review":    [ ...PatternInfo... ],
      "errors":           [ ...ErrorInfo... ],
      "preprocessor":     { "clang_version": "...", "files_processed": 1, "macros_expanded": 0 } | null
    }
  ],
  "errors": [],                       // tree-level errors (clang, wasm size, DB)
  "files_total": <int>,
  "files_transformed": <int>,
  "files_manual_review": <int>
}
```

**Tree-level `status` aggregation:**

| Per-file statuses | Tree status |
|---|---|
| All `success` | `success` |
| Any `failed` | `failed` (wasm not stored) |
| All `success`/`partial`, no `failed` | `partial` (wasm stored) |
| Empty `files` | `failed` (handler rejects earlier with 400) |

`wasm_stored` is `true` only when at least one file reached
`success`/`partial` and the clang compile succeeded. A `failed` file
**does not** prevent other files from compiling — the resulting wasm
may still build, but the tree-level status reflects the worst file.

**Limits (server-enforced):**

| Limit | Value | Source |
|---|---|---|
| Request body | 50 MiB | `maxTreeBodyBytes` in `handler/migration.go` |
| File count | 256 | `maxTreeFiles` in `handler/migration.go` |
| Output wasm | 100 MiB | `MaxArtifactSize` in `service/migration.go` |
| App name length | 1–63 | regex `^[a-z0-9][a-z0-9-]{0,62}$` |

**Per-file data sources:**

| `FileReport` field | Source |
|---|---|
| `status` | classified from per-file patterns (see §3 status rules) |
| `patterns_detected`, `transformations`, `manual_review` | parsed from `edge-migrate --analyze --json <path>` subprocess stdout (one subprocess per `.c` file) |
| `errors` | per-file parse/transform errors (stderr captured) |
| `preprocessor` | mirror of the preprocessor's `PreprocessorInfo` for that file |

If `--analyze --json` is unavailable (older `edge-migrate` binary),
the service falls back to the `detectTransformedPatterns` heuristic
on the transformed WASI C output. This fallback is tracked for
removal once the version floor advances. See §5.2 for the full
subprocess flow.

**Status codes:**

| HTTP Status | Meaning |
|---|---|
| 200 | Migration attempted (any of `success` / `partial` / `failed` in the body) |
| 400 | Invalid input (bad `app_name`, manifest mismatch, zip-slip, non-C language, too many files, bad manifest JSON) |
| 401 | Missing or invalid tenant API key |
| 413 | Request body exceeds 50 MiB (caught mid-stream by `http.MaxBytesReader`) |
| 500 | Server-side error (DB, internal) |

### 6.2 Error Responses

| HTTP Status | Meaning |
|---|---|
| 200 | Migration attempted (success or partial) |
| 400 | Invalid input (not a C file, etc.) |
| 401 | Not authenticated |
| 422 | Compilation failed (transformation produced invalid code) |
| 500 | Server-side error |

### 6.3 Deployment Record

`edge migrate` creates a `deployments` DB record (status: `migrated`) but does **not** create an `active_deployments` record. The developer activates via `edge deploy`.

This is distinct from `edge deploy` which creates both records and activates in one step.

### 6.4 Deploying a Migrated Binary

```
edge deploy hello_world --id d_xyz789
```

The developer specifies the `deployment_id` returned by `edge migrate`. This connects the two steps: migration creates the artifact, deployment activates it.

---

## 7. Future Extensions

### Phase 2
- Directory upload (walk tree, transform multiple files)
- Rust support: auto-transform `std::net` patterns
- Output to `edge deploy` directly (skip intermediate storage)

### Phase 3
- C preprocessor macro expansion (handle `#define socket(...)`)
- IDE integration via LSP
- VS Code extension using `edge-migrate-lib`

---

## 8. Glossary

| Term | Definition |
|------|------------|
| **Tree-sitter** | Incremental parsing library used for AST-level code analysis |
| **WASI Preview 2** | Second-generation WASI using WIT definitions and the component model |
| **wasm32-wasip2** | Clang target for compiling C to WASI Preview 2 Wasm |
| **wasi-sdk** | Pre-configured clang + wasi-libc targeting wasm32-wasip2, official Bytecode Alliance toolchain |
| **edge-migrate-lib** | The reusable analysis + transformation library (library-first design) |

---

## Appendix A: POSIX → WASI Socket Mapping

```
POSIX                           WASI Preview 2
─────────────────────────────────────────────────────
socket(AF_INET, SOCK_STREAM)    create-tcp-socket(ipv4)
socket(AF_INET, SOCK_DGRAM)     create-udp-socket(ipv4)
bind(fd, addr)                   start-bind() + finish-bind()
listen(fd)                       start-listen() + finish-listen()
                                 set-listen-backlog-size()
accept(fd)                       accept() → poll loop wrapper
connect(fd, addr)                start-connect() + finish-connect()
recv(fd, buf, len, 0)            input-stream.read()
send(fd, buf, len, 0)            output-stream.write()
gethostbyname(name)              wasi:ip-name-lookup
close(fd)                        drop(tcp-socket) / drop(udp-socket)
```