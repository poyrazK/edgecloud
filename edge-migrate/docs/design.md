# edge-migrate Design

> **Status:** Draft v0.3
> **Date:** 2026-06-19
> **Owner:** Hüseyin Poyraz Küçükarslan
>
> **v0.3 changes:** Added Rust support end-to-end. `Language` is now a first-class parameter on the CLI (`--language rust`) and on the HTTP API (`language: rust`); the bin dispatches to `RustAnalyzer` + `RustTransformer` and the Go control plane compiles with `rustc --target wasm32-wasip2`. New §4.4 lists the Rust pattern mapping table. The server now requires `rustc` with the `wasm32-wasip2` target installed (alongside the existing `clang` requirement), controlled via `RUSTC_PATH` env var. `pattern` on `PatternMatch` is now a `PatternKind` sum type (`Posix(...)` | `Rust(...)`); the JSON wire format remains a flat string for backward compatibility.
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

| Language | Support |
|----------|---------|
| C | ✅ Full analysis + auto-transform for safe patterns (with preprocessor expansion — see below) |
| Rust | ✅ Full analysis + auto-transform for `std::net`, `std::fs`, `std::process` patterns (M3; gated behind the `rust` Cargo feature and `--language rust`) |
| Other | Future extension |

Rust patterns detected and auto-transformed include `std::net::TcpListener::bind`, `std::net::TcpStream::connect`, `std::net::UdpSocket::bind`, `std::fs::File::open`, `std::fs::read`, `std::fs::write`, and `file.close()` (best-effort). The Rust transformer emits a `use crate::wasi::sockets::tcp_create_socket::create_tcp_socket; use crate::wasi::sockets::udp_create_socket::create_udp_socket; use crate::wasi::filesystem::types::{Descriptor, PathFlags, OpenFlags, DescriptorFlags}; use crate::wasi::filesystem::preopens;` prelude (matching the wit-bindgen 0.45 binding tree — issue #417) and rewrites each match to the canonical WASI Rust API surface (`create_tcp_socket(IpAddressFamily::Ipv4)?.start_bind(&instance_network(), parse_addr_v4(addr))?.finish_bind()?...`, etc.). `std::process::exit` and `UdpSocket::connect` are flagged as `NotTransformable` because WASM has no process model and `wasi::sockets::udp` has no connect analogue. The full mapping table lives in §4.4.

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
- **Byte-range remap.** Beyond `line_map`, the analyzer maintains a
  parallel `byte_map` (one entry per expanded line) so pattern
  byte ranges can be brought back into original-source coordinates
  before the transformer slices the original source. Without this,
  byte offsets from the expanded source (which always lead with
  ~135 bytes of `# <line> "<file>"` linemarkers) overflow the
  original source's length and panic with "range end index N out
  of range for slice of length M". When the linear-interpolation
  remap produces a range that doesn't contain the match's snippet
  text (typically because clang emitted only one linemarker for
  the whole file), the analyzer falls back to a content search
  bounded by 1 KiB.
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
2. **Directory / tree** — `edge-migrate --tree <DIR> [--app-name NAME]` walks the directory for `.c`/`.h` files (skipping `build/`, `target/`, `node_modules/`, etc.) and POSTs the whole tree to `POST /api/migrate-tree`. The developer supplies an explicit `--app-name` that must match `^[a-z0-9][a-z0-9.\-_]{0,62}$`. Without `--app-name`, the dir basename is used.

In tree mode, all transformed `.c` files are compiled together in a single clang invocation (`--target=wasm32-wasip2 -nostdlib -I <tmpdir>`) and produce one wasm binary. The server response includes a per-file `FileReport` with the patterns detected, transformations applied, and any errors.

**App name derivation (single-file mode):** The app name is derived from the uploaded filename (without extension). For example, `edge migrate hello_world.c` sets the app name to `hello_world`. This is stored in the deployment record and used by `edge deploy`.

For directory mode, the explicit `--app-name` is required to be a valid `^[a-z0-9][a-z0-9.\-_]{0,62}$` name (defense-in-depth + DNS-safety for the eventual `*.edgecloud.dev` URL). See §6.1.2 for the full request/response shape.

**Standalone binary:** `edge-migrate` is its own binary, not a subcommand of `edge-cli`. Developers install it separately: `cargo install edge-migrate`.

### 2.4 In-place vs. Output

`edge migrate` does **not** modify the source file. It does **not** produce a local transformed file. The source stays untouched. The transformed source + compiled wasm lives on edgeCloud.

### 2.5 Authentication

The CLI requires the user to be authenticated (`edge auth login` or equivalent). The wasm binary is stored under the authenticated tenant's account.

---

## 3. Transformation Report

The migration result is a structured report returned to the developer. The
report carries a `language` discriminator: `c` patterns render as
`Posix(SocketTcp)` etc., `rust` patterns as `Rust(TcpBind)` etc. The
human-readable section header switches to "POSIX patterns" (C) or
"Rust std patterns" (Rust) accordingly.

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

### 4.4 Rust → WASI Mapping (M3)

When `language == "rust"`, `RustAnalyzer` walks tree-sitter-rust's
AST and emits `PatternKind::Rust(RustPattern)` matches. The
`RustTransformer` then rewrites each match to the canonical WASI
Rust API. The C path is unaffected — `language: c` (the default)
continues to flow through §4.1–§4.3.

**Target API.** Emits target the wit-bindgen 0.45 binding tree
(wit-bindgen = "0.45"), which generates items under `crate::wasi::*`
— see PR #416 and issue #417. Imports are emitted as
`use crate::wasi::sockets::tcp_create_socket::create_tcp_socket;`
(not `use wasi::socket::tcp::TcpSocket;`). The full
canonical target shape is in
`edge-migrate-lib/src/rust_transformer.rs::WASI_RUST_PRELUDE`
and `::generate_wasi_code`; this table lists the per-pattern
emit shape as it appears in transformer output.

**`parse_addr_v4` helper.** The prelude ships an inline
`fn parse_addr_v4(s: &str) -> IpSocketAddress` (no extra deps) so
the address literal `addr` can be routed through it. Bindgen 0.45
types `start_bind` / `start_connect`'s second parameter as
`IpSocketAddress`, not `&str`.

| Rust source | `RustPattern` | `Transformability` | Transformed output |
|---|---|---|---|
| `std::net::TcpListener::bind(addr)` | `TcpBind` | `AutoTransformable` | `let _s = create_tcp_socket(IpAddressFamily::Ipv4)?;` `let _n = instance_network();` `_s.start_bind(&_n, parse_addr_v4({addr}))?.finish_bind()?.start_listen()?.finish_listen()?;` |
| `listener.accept()` | `TcpAccept` | `BestEffort` | `loop { match self.accept() { Ok(s) => break s, Err(_) => std::thread::yield_now() } }` (with `// TODO: replace busy-spin with poll subscription` comment) |
| `std::net::TcpStream::connect(addr)` | `TcpConnect` | `AutoTransformable` | `let _s = create_tcp_socket(IpAddressFamily::Ipv4)?;` `let _n = instance_network();` `let (_rx, _tx) = _s.start_connect(&_n, parse_addr_v4({addr}))?.finish_connect()?;` |
| `std::net::UdpSocket::bind(addr)` | `UdpBind` | `AutoTransformable` | `let _s = create_udp_socket(IpAddressFamily::Ipv4)?;` `let _n = instance_network();` `_s.start_bind(&_n, parse_addr_v4({addr}))?.finish_bind()?;` |
| `udp.connect(addr)` | `UdpConnect` | `NotTransformable` | No direct `wasi::sockets::udp` connect analogue — flagged for manual review |
| `std::process::exit(code)` | `ProcessExit` | `NotTransformable` | WASM has no process model — flagged for manual review |
| `std::fs::File::open(p)` | `FsOpen` | `AutoTransformable` | `let _preopens = preopens::get_directories();` `let _base = _preopens.get(0).expect("no preopens").0.clone();` `let _d = _base.open_at(PathFlags::empty(), {p}, OpenFlags::empty(), DescriptorFlags::READ)?;` |
| `std::fs::read(p)` / `read_to_string` | `FsRead` | `AutoTransformable` | open a `Descriptor` (same shape as `FsOpen`), then `_d.read(0, 0)?`. The `length=0` placeholder is intentional — see scope note below. |
| `std::fs::write(p, ...)` | `FsWrite` | `AutoTransformable` | `let _d = _base.open_at(PathFlags::empty(), {p}, OpenFlags::CREATE | OpenFlags::TRUNCATE, DescriptorFlags::WRITE)?;` `let _n: u64 = _d.write({data}.to_vec(), 0)?;` |
| `file.close()` (explicit) | `FsClose` | `AutoTransformable` | `drop(var)` (bindgen generates a `Drop` impl for `Descriptor`) |

**FS scope note (deliberate, typecheck-only).** The synthesized
cargo project gives the guest zero preopens, so the
`preopens::get_directories()` call returns an empty list at runtime
and the subsequent `expect("no preopens")` panics. The transformer
output typechecks against `wit-bindgen = "0.45"` — the component
*loads* in the worker (wasmtime 45.0.3) but cannot open files until
the synthesized Cargo.toml is wired with host preopens. That runtime
plumbing is follow-up work; the typecheck goal of issue #417 is met.

**Out of scope for v1** (intentional limitations, tracked as
follow-ups):

- `tokio::net`, `async-std`, `#![no_std]` — only `std` is matched.
- Rust macros declared via `macro_rules!` may hide patterns from
  tree-sitter-rust. A future `rustc -Zunpretty=expanded` integration
  (analogous to the C preprocessor) could close this gap.
- `tree-sitter-rust` is pinned at 0.24 to match the workspace's
  `tree-sitter = "0.24"`. Bump in lockstep if `tree-sitter` advances.

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

1. **Receive** — Accept the uploaded source file (single-file mode; `language: c` or `language: rust`)
2. **Analyze** — Run `edge-migrate-lib` analysis (AST + pattern matching, with preprocessor if `clang` is reachable; Rust path uses `tree-sitter-rust` and skips the preprocessor)
3. **Transform** — Apply auto-transformations for safe patterns (C: WASI C; Rust: WASI Rust via `wasi::socket` + `wasi::filesystem`)
4. **Compile** — Compile transformed source to wasm32-wasip2. C: `clang --target=wasm32-wasip2 -nostdlib`. Rust: `rustc --target wasm32-wasip2 --crate-type=cdylib --edition 2021`. The dispatcher is `MigrationService.Migrate` in `edge-control-plane/internal/service/migration.go`.
5. **Store** — Store wasm binary under tenant + app (or temporary store for deploy)
6. **Report** — Return structured transformation report to CLI

**Tree mode (`POST /api/migrate-tree`, M2):** Accepts a multipart form with one `file` part per source plus a `tree` JSON manifest, OR a single `tree` part with `Content-Type: application/zip`. For each source file:

1. Run `edge-migrate --transform --language <lang> <path>` → WASI source
2. Run `edge-migrate --analyze-json --language <lang> <path>` → structured `MigrationReport` JSON (used to populate per-file `FileReport.patterns_detected` / `transformations` / `manual_review`)
3. On `--analyze-json` failure (older binary), fall back to the language-aware string scanner (`detectTransformedPatterns` for C, `detectTransformedPatternsRust` for Rust)

The `language` form field gates both the per-file subprocess flag and the final compile step. All transformed files are compiled together in a single invocation (clang for C; a single `rustc` invocation listing every transformed `.rs` file with `--crate-type=cdylib` for Rust). The wasm size is checked against `MaxArtifactSize` (100 MiB); oversized builds return a `Failed` tree report with an error entry. The artifact + deployment row are written only on success.

### 5.3 Pattern Engine

The core engine uses **tree-sitter** for AST-level analysis:

- **C parser** (`tree-sitter-c`) — parses C source to AST
- **Pattern matcher** — matches known POSIX socket/file I/O patterns against AST
- **Transformer** — rewrites matched patterns to WASI equivalents
- **Report generator** — produces structured report (JSON) for the CLI

### 5.4 Compiler

The server uses the **wasi-sdk** — Bytecode Alliance's official WASI SDK (pre-configured clang + wasi-libc targeting wasm32-wasip2) — for C compilation. For Rust compilation the server also requires `rustc` with the `wasm32-wasip2` target installed (`rustup target add wasm32-wasip2`); the path is controlled via the `RUSTC_PATH` env var (default: `rustc`).

```bash
# C:
/path/to/wasi-sdk/bin/clang --target=wasm32-wasip2 \
      -nostdlib \
      -o output.wasm \
      transformed.c

# Rust (M3):
$RUSTC_PATH --target wasm32-wasip2 \
      --crate-type=cdylib \
      --edition 2021 \
      -o output.wasm \
      transformed.rs
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

Accepts a multi-file source tree (`language: c` or `language: rust`).
Two wire formats are supported; the handler dispatches based on
which `tree` form part is present:

**Variant A — multipart parts** (preferred for small projects):

```
POST /api/migrate-tree
Authorization: Bearer <api-key>
Content-Type: multipart/form-data

Fields:
  - app_name: <string>             (required; regex ^[a-z0-9][a-z0-9.\-_]{0,62}$)
  - language: "c" | "rust"         (required; M3 widens to both)
  - tree: <json string>            (required: {"files": ["src/main.rs", ...]})
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
  - language: "c" | "rust"         (required)
  - tree: <binary application/zip> (required; the directory tree zipped;
                                   entries are the source of truth;
                                   `tree` JSON manifest is not parsed)
```

Zip entries are read in order. Non-source entries are skipped (C:
`.c`/`.h`, Rust: `.rs` — controlled by `treeUploadExts` in
`handler/migration.go`). Entry names are validated against
`isSafeFilePath` (rejects `..`, absolute paths, backslashes, and
Windows drive letters — zip-slip protection). Cap: 256 files, 50
MiB decompressed.

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
| App name length | 1–63 | regex `^[a-z0-9][a-z0-9.\-_]{0,62}$` |

**Per-file data sources:**

| `FileReport` field | Source |
|---|---|
| `status` | classified from per-file patterns (see §3 status rules) |
| `patterns_detected`, `transformations`, `manual_review` | parsed from `edge-migrate --analyze-json --language <lang> <path>` subprocess stdout (one subprocess per source file) |
| `errors` | per-file parse/transform errors (stderr captured) |
| `preprocessor` | mirror of the preprocessor's `PreprocessorInfo` for that file (C only — Rust has no preprocessor in v1) |

If `--analyze-json` is unavailable (older `edge-migrate` binary),
the service falls back to a language-aware string scanner
(`detectTransformedPatterns` for C, `detectTransformedPatternsRust`
for Rust). This fallback is tracked for removal once the version
floor advances. See §5.2 for the full subprocess flow.

**Status codes:**

| HTTP Status | Meaning |
|---|---|
| 200 | Migration attempted (any of `success` / `partial` / `failed` in the body) |
| 400 | Invalid input (bad `app_name`, manifest mismatch, zip-slip, unknown language, too many files, bad manifest JSON) |
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

### Phase 2 (shipped in M2)
- Directory upload (walk tree, transform multiple files) — see §6.1.2

### Phase 3 (shipped in M1 + M3)
- C preprocessor macro expansion (handle `#define socket(...)`) — see §2.2
- Rust support: auto-transform `std::net`, `std::fs`, `std::process` patterns — see §4.4
- Output to `edge deploy` directly (skip intermediate storage)
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