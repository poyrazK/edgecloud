# Phase D Test Fixtures

`wasip2`-compiled WebAssembly components used by the Phase E L1–L10
integration tests in `edge-worker/tests/layer_integration.rs` and the
existing supervisor tests.

## What's here

| File                         | Status | Notes |
|------------------------------|--------|-------|
| `handler/` (Rust source)     | ✅ builds | `cargo build --target wasm32-unknown-unknown --release` produces `target/wasm32-unknown-unknown/release/edge_fixture_handler.wasm` |
| `handler.wasm` (pre-built)   | ✅ committed | SHA-256 in `test_fixtures_match_source.rs` |
| `redis_lite/` (Rust source)  | ✅ builds | `samples/redis-lite/` LR TCP/RESP guest (issue #496). Build via `cd samples/redis-lite && ../../target/release/edge build` |
| `redis_lite.wasm` (pre-built)| ✅ committed | SHA-256 in `test_fixtures_match_source.rs`; e2e in `edge-worker/tests/redis_lite_e2e.rs` |
| `kv/`                        | ⏳ stub | deferred — see "Open items" |
| `test-handle.wasm`           | ✅ legacy | retained for the 9 supervisor integration tests from v0.1 |
| `wit/`                       | ✅ vendored | shared by all fixture crates |

## Build workflow (Phase D-final)

The v0.2 fixtures use the **`wasm32-unknown-unknown` + `wasm-tools
component new`** workflow rather than `wasm32-wasip2`. The
`wasm32-wasip2` target embeds `wit-component 0.241.x` which emits
`wasi:io@0.2.6` / `wasi:http/types@0.2.4` into the type index, while
`wasmtime-wasi-http 25.0.3` expects `@0.2.1`. The
`wasm-tools component new` toolchain (built on `wit-component
0.252.0`) emits `@0.2.1` correctly and `wasmtime 25.0.3` parses the
result cleanly.

**Build procedure:**

```bash
cd edge-worker/tests/fixtures/handler

# Step 1: compile the Rust fixture to a core wasm module (no component
# model wrapping yet). The `wit_bindgen::generate!` macro emits raw
# function imports/exports that `wasm-tools` later wraps.
cargo build --target wasm32-unknown-unknown --release

# Step 2: wrap the core wasm into a wasi:http component using the
# vendored WIT directory. The `--world edge-runtime-handler` flag
# resolves the world's exports/imports from the WIT.
wasm-tools component new \
    --world edge-runtime-handler \
    -o ../handler.wasm \
    target/wasm32-unknown-unknown/release/edge_fixture_handler.wasm

# Step 3: verify and capture the SHA-256.
wasm-tools validate ../handler.wasm
sha256sum ../handler.wasm
# Update EXPECTED_HANDLER_HASH in
# edge-worker/tests/test_fixtures_match_source.rs to the new digest.
```

**Required runtime support:**

The runtime's `wasmtime::Engine` MUST have
`config.wasm_reference_types(true)`. The core wasm uses multi-byte
LEB128 zero encoding for memory indices in bulk-memory instructions
(`memory.copy`, `memory.fill`, etc.); without reference types, the
parser runs in single-memory mode and rejects those positions with
"zero byte expected". See `edge-runtime/src/engine.rs` and the test
`edge-runtime/tests/handler_fixture_load.rs`.

The linker MUST be instantiated via `linker.instantiate_async()` (not
`instantiate()`), because `wasmtime_wasi::add_to_linker_async` and
`wasmtime_wasi_http::add_only_http_to_linker_async` (both used by
`create_component_linker_handler`) require async support.

## L1–L4 status: ✅ passing

The linker-level smoke tests in `edge-runtime/tests/v0_2_smoke.rs`
cover L1–L4:

| Test                                          | Layer |
|-----------------------------------------------|-------|
| `long_running_linker_factory_returns_a_linker`| L1    |
| `handler_linker_factory_returns_a_linker`     | L1    |
| `linker_factory_builds`                       | L2    |
| `runtime_state_clones_for_proxy_pre`          | L3    |
| `wasi_http_view_clone_preserves_state`        | L4    |

These prove the bindgen wirings are correct for the empty-imports case
and the linker accepts a real `RuntimeState`. They run in `cargo test
--manifest-path edge-runtime/Cargo.toml`.

## L5 status: ✅ smoke passing

`edge-runtime/tests/handler_fixture_load.rs::handler_fixture_instantiates`
confirms the new `wasm-tools`-wrapped component parses through
`Component::from_binary` AND instantiates through
`create_component_linker_handler`. This is the first positive
end-to-end fixture test. The L5 dispatch round-trip and downstream
L6–L10 tests in `edge-worker/tests/layer_integration.rs` are not yet
written — they require `LayerHarness` scaffolding and are deferred to
the next round.

## L6–L10 status: ⏳ pending

The fixture's surface (`GET /` and `GET /busy`) covers L5 (round-trip)
and L7 (per-request timeout). The following paths were added to
exercise edge:cloud interfaces:

| Path                        | Interface call               |
|-----------------------------|------------------------------|
| `GET /env/{key}`            | `process.get-env(key)`       |
| `GET /time/now`             | `time.now()`                 |
| `GET /kv/set?key=x&val=y`   | `kv-store.set(x, y)`        |
| `GET /kv/get?key=x`         | `kv-store.get(x)`           |
| `GET /kv/del?key=x`         | `kv-store.delete(x)`        |
| `GET /cache/set?key=x&val=y`| `cache.set(x, y)`           |
| `GET /cache/get?key=x`      | `cache.get(x)`              |
| `GET /cache/del?key=x`      | `cache.delete(x)`           |
| `GET /log?msg=...`          | `observe.emit-log(...)`     |
| `GET /sched/once?ms=N`     | `scheduling.schedule-once(N)`|

## Paths implemented by `handler.wasm`

| Path                          | Behavior                                    |
|-------------------------------|---------------------------------------------|
| `GET /`                       | 200, body `{"hello":"handler","path":"/"}`  |
| `GET /busy`                   | Busy-loops for ~5s, then 200                |
| `GET /env/{key}`              | 200 with env value, or 404                  |
| `GET /time/now`               | 200 with timestamp (u64)                    |
| `GET /kv/set?key=x&val=y`     | 200 "ok"                                    |
| `GET /kv/get?key=x`           | 200 with value, or 404                      |
| `GET /kv/del?key=x`           | 200 "ok"                                    |
| `GET /cache/set?key=x&val=y`  | 200 "ok"                                    |
| `GET /cache/get?key=x`        | 200 with value, or 404                      |
| `GET /cache/del?key=x`        | 200 "ok"                                    |
| `GET /log?msg=...`            | 200 "ok"                                    |
| `GET /sched/once?ms=N`       | 200 with task ID (UUID)                     |

All other paths return 404. The handler also exports `wasi:cli/run` as
a trap (unreachable) so the macro-generated `export!` is satisfied; the
real entry point is `wasi:http/incoming-handler#handle`.

## Why a busy loop?

`edge:cloud/time::sleep` is `tokio::time::sleep` (verified in
`edge-runtime/src/interfaces/time.rs:19-23`) which does **not** yield
to the wasmtime epoch clock. To exercise the per-request epoch
deadline, the guest must burn its own CPU. `/busy` does that.

## Toolchain

- `rustup target add wasm32-unknown-unknown` (one-time setup)
- `wasm-tools 1.252.0+` (Cargo install: `cargo install wasm-tools`)
- `cargo 1.78+` (matches the wasmtime 25.0.3 `rust-version` floor)
- `wit-bindgen 0.45` (transitive, pinned via the host's wasmtime)

## Open items

- `long_running/` (L8) — implemented by `redis_lite/` above (issue
  #496) — a `wasi:sockets/*` TCP RESP server exercising the LR
  supervisor path. Replaces the stub row in the table.
- `kv/` (L6/L9/L10) — needs `edge:cloud/kv-store` and
  `wasi:filesystem/*` plus `wasi:http/outgoing-handler`. Deferred;
  these need additional wit-bindgen API gymnastics (resource lifetimes,
  `Result<T, E>` lifting) that are out of scope for the v0.2 cut.
- L6–L10 tests in `edge-worker/tests/layer_integration.rs` — write
  once the `LayerHarness` scaffolding lands.