# ADR 0001: Classify Patterns as "Not Transformable" Rather Than Faking Translations

* **Status:** Accepted
* **Date:** 2026-07-01
* **Authors:** Hüseyin Poyraz Küçükarslan
* **Supersedes:** —
* **Superseded by:** —

## Context

`edge-migrate` auto-rewrites a subset of POSIX C / Rust `std` patterns to
their WASI equivalents (see §4 of `design.md`). For patterns with no
clean WASI mapping — `fork()`, `poll()`/`select()`, `socketpair()`,
`SOCK_RAW`, `O_NONBLOCK`, etc. — we face a choice:

**(a) Force a transformation.** Generate *something* that compiles to
wasm. The output may be a stub, a busy-loop, a partial implementation,
or an incorrect translation that compiles but does the wrong thing at
runtime. Examples: emitting a `poll()`-loop wrapper that busy-spins on
`std::thread::yield_now()`, or stubbing `fork()` to a `wasm-bindgen`
import that doesn't exist.

**(b) Mark the pattern `NotTransformable` and surface it in the report.**
The developer rewrites the offending construct manually. The tool's
report names the pattern, gives the line number, and suggests a
remediation hint.

## Decision

We adopt **(b)** consistently. `edge-migrate` will never emit a
"fake" or stub translation for a pattern whose semantics cannot be
faithfully preserved in WASI. Every pattern that lacks a 1:1 WASI
equivalent is classified `Transformability::NotTransformable` in
`edge-migrate-lib/src/patterns.rs` and listed in §4.3 of
`design.md`.

The tool's transformation result is **either correctly transformed
or honestly not-transformed**. There is no silently-incorrect third
state.

## Rationale

**1. Semantic correctness over developer ergonomics.**
A stub or busy-loop is a Trojan horse for the runtime. A guest that
"calls `fork()`" but actually busy-spins will look like it works in
development (no children to fork), then mysteriously degrade in
production when it encounters a real fork. Worse, the mismatch between
source intent and runtime behavior creates a debugging story that
requires developers to learn wasmtime internals to diagnose — exactly
the opposite of what a migration tool should provide.

**2. WASI's process model is absent at the spec level.**
`fork()`, `exec()`, `vfork()`, `posix_spawn()` all presume a POSIX
process tree — independent address spaces, signal delivery between
related processes, file descriptor inheritance. WASI has *no* process
concept at all (`wasm` modules are single-isolated units of
execution). We could synthesize one out of `wasmtime_wasi::process`,
but that would be a custom runtime extension, not WASI, and would
contradict the goal of producing portable WASI components.

**3. Manual review is the documented remediation.**
When a developer runs `edge migrate <file>.c` and the report lists
"NotTransformable: poll() at line 67", they know to (a) read line 67,
(b) understand WASI's event-loop gap, and (c) either restructure the
event loop or reach for `wasmtime_wasi::io::poll` (a runtime-side
polyfill that we expose but don't auto-inject). The cost of one manual
step is dwarfed by the cost of diagnosing silent incorrectness.

**4. Auto-wrap blocking patterns are tracked separately.**
`accept()` in a loop, blocking `recv()`, blocking `send()` DO have WASI
equivalents (the pollable stream + `wasi:io/poll` subscription), so
they are classified `BestEffort` (§4.2) and the transformer injects a
canonical poll-loop wrapper around the call site. The wrapper's
correctness is reviewable because it's mechanical — every site gets the
same wrapper. This is fundamentally different from faking semantics.

**5. The `Transformability` enum is part of the contract.**
`PatternMatch::transformability` is serialized to JSON and consumed by
the Go control plane's `MigrationService` and by `edge-cli`'s report
display. New patterns can only be added with an explicit classification
that the analyzer/transformer agree on. The classification forces a
design conversation every time — exactly the conversation that prevents
silent errors.

## Consequences

**Positive:**
- Every migrated component either works as the source intended, or
  has a clearly enumerated set of non-translated patterns the
  developer must address.
- The runtime never executes a guest that "looks like fork() but
  isn't" — the analysis-time check is the guarantee.
- A new developer reading a `MigrationReport` knows exactly what to
  expect: rows in the "Auto-transformed" section were 1:1
  translations; rows in the "Manual review required" section need
  attention; no row is silently incomplete.

**Negative:**
- More frequent manual rewrites for developers porting non-trivial
  POSIX code (event loops using `poll()`, anything touching
  `SOCK_RAW`).
- The tool can't promise "just compile it and it works" — only
  "compile it and address the N patterns I report".

**Mitigations for the negative case:**
- §4.3 of `design.md` provides a remediation hint for every
  `NotTransformable` pattern. The hint names the WASI construct or
  the architectural change required.
- Future expansion (see "Future Work" below) can promote some
  `NotTransformable` patterns to `BestEffort` once the WASI spec
  solidifies.

## Future Work

- **`poll()` polyfill**: `wasmtime_wasi::io::poll` is already shipped in
  the runtime. A future M-version could promote `PosixPattern::Poll`
  from `NotTransformable` to `BestEffort` by detecting `poll()` calls
  and rewriting to the wasi-iox runtime polyfill. Tracked as
  `edge-migrate#42`.
- **Non-blocking I/O patterns**: code that uses `O_NONBLOCK` + manual
  fd-set manipulation CAN be lifted to a pollable stream, but the
  transformation is non-trivial (the entire fd-set becomes a
  `wasi:io/poll::Pollable` set). Tracked as `edge-migrate#43`.
- **`socketpair()` polyfill**: a future M-version could lift this to
  two pre-connected TCP sockets via `wasi:sockets`, with a note that
  the semantics (in-band vs out-of-band close) differ from POSIX.
  Tracked as `edge-migrate#44`.

## Alternatives Considered

**(c) Mark `NotTransformable` AND emit `// MIGRATION: TODO` comments in
the output source.** Rejected: the comment clutters the output and the
developer's IDE may interpret it as a normal TODO. A structured JSON
report with one row per unreviewed pattern is more discoverable.

**(d) Embed developer documentation in the runtime's response.** When
the runtime links a wasm that contains a `NotTransformable` import,
return a "migration report" alongside the link. Rejected: the runtime
doesn't know what the developer's original source looked like. The
migration tool is the authoritative place for that report.

**(e) Provide a `--force` flag that emits stubs.** Rejected: the
divergence between `--force` and default behavior is a footgun. A
developer who runs once with `--force` and once without gets two
different reports and can't tell which is the "real" one.
