//! Fixture integrity test — guards against stale `.wasm` files in CI.
//!
//! Each Phase D fixture has a committed SHA-256 in this file. If a
//! developer rebuilds a fixture but forgets to update the hash here,
//! the test fails with a clear message. If a developer edits the
//! fixture source without rebuilding, the .wasm bytes are unchanged
//! so the test still passes (the source isn't hashed — that's a
//! belt-and-suspenders check for the per-crate build workflow).
//!
//! Run with: `cargo test --manifest-path edge-worker/Cargo.toml --test test_fixtures_match_source`
//! Skip in CI: only if the fixture build is gated behind a feature flag
//! (currently always on — fixture files are committed).

use std::path::PathBuf;

use sha2::{Digest, Sha256};

const EXPECTED_HANDLER_HASH: &str =
    "200c4e49ea1956660e97f4d68aee51d316bb2173db63523175389546591185b7";

// Issue #448: `samples/hello-js-ws` is the long-running JS shim
// (`world = "edge-runtime"`, `export start: func()`) wrapped via
// `wasm-tools component new --adapt
// wasi_snapshot_preview1.reactor.wasm`. The fixture is committed at
// `tests/fixtures/js_websocket_handler.wasm`. Rebuild with:
//   cd samples/hello-js-ws
//   npx esbuild src/handler.js --bundle --format=iife \
//     --platform=neutral --outfile=.edge/bundle.js
//   EDGE_JS_BUNDLE=$PWD/.edge/bundle.js \
//     cargo build --target wasm32-wasip1 --release
//   wasm-tools component new \
//     <repo>/target-cache/edgecloud/wasm32-wasip1/release/hello_js_ws.wasm \
//     --adapt \
//     ../../edge-cli/adapters/wasi_snapshot_preview1.reactor.wasm \
//     -o ../edge-worker/tests/fixtures/js_websocket_handler.wasm
// (The reactor adapter is vendored at `edge-cli/adapters/` — see
// `edge-cli/adapters/SHA256SUMS` for the pinned SHA-256; issue #423.
// The vendored bytes are byte-identical to the prior
// registry-sourced adapter, so the rebuild preserves this fixture's
// hash.)
const EXPECTED_JS_WEBSOCKET_HASH: &str =
    "5f785981710bc687f16b640e62a508f61b9aa00327b66caa2a35db09f9345c6e";

// Issue #496 — `samples/redis-lite` is the long-running Rust guest
// (`world = "edge-runtime"`, `export start: func()`) that speaks RESP
// over raw TCP. The fixture is committed at
// `tests/fixtures/redis_lite.wasm`. Rebuild with:
//   cd samples/redis-lite
//   ../../target/release/edge build   # produces target/component.wasm
//   cp target/component.wasm ../../edge-worker/tests/fixtures/redis_lite.wasm
//   sha256sum ../../edge-worker/tests/fixtures/redis_lite.wasm
const EXPECTED_REDIS_LITE_HASH: &str =
    "a714d28b1afbe4594889d0408499f57e4c82f51029c012e79d61da4782e3d9ab";

fn sha256_hex(bytes: &[u8]) -> String {
    // Production SHA-256 via the `sha2` crate (already a regular
    // `[dependencies]` entry on `edge-worker` — used by
    // `src/downloader.rs::sha256_hex` and the worker's artifact
    // integrity check at `supervisor.rs`). Reusing it here keeps
    // the test fixture hash consistent with the production
    // verification path: any change in how edge-worker hashes a
    // blob is a single point of repair.
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn fixture_path(rel: &str) -> Option<PathBuf> {
    let candidates = [
        format!("tests/fixtures/{rel}"),
        format!("edge-worker/tests/fixtures/{rel}"),
        format!("../edge-worker/tests/fixtures/{rel}"),
    ];
    candidates
        .into_iter()
        .map(PathBuf::from)
        .find(|p| p.exists())
}

fn assert_hash(rel: &str, expected: &str) {
    let path = match fixture_path(rel) {
        Some(p) => p,
        None => {
            eprintln!("SKIPPED: {rel} not present in this checkout");
            return;
        }
    };
    let bytes = std::fs::read(&path).expect("read fixture");
    let actual = sha256_hex(&bytes);
    assert_eq!(
        actual, expected,
        "fixture {rel} hash mismatch.\n\
         expected: {expected}\n\
         actual:   {actual}\n\
         If you rebuilt the fixture, run `sha256sum {path:?}` and update \
         EXPECTED_*_HASH in tests/test_fixtures_match_source.rs.",
    );
}

#[test]
fn handler_fixture_intact() {
    assert_hash("handler.wasm", EXPECTED_HANDLER_HASH);
}

#[test]
fn js_websocket_fixture_intact() {
    assert_hash("js_websocket_handler.wasm", EXPECTED_JS_WEBSOCKET_HASH);
}

#[test]
fn redis_lite_fixture_intact() {
    assert_hash("redis_lite.wasm", EXPECTED_REDIS_LITE_HASH);
}

#[test]
fn legacy_test_handle_fixture_intact() {
    // The v0.1-era test-handle.wasm ships in-tree (committed in
    // `tests/fixtures/test-handle.wasm`) and is used by the existing
    // supervisor integration tests. We don't hash it here because
    // it's not regenerated — it's a frozen artifact. This test
    // simply asserts the file is present.
    let path = fixture_path("test-handle.wasm")
        .expect("legacy test-handle.wasm must be present in fixtures/");
    let bytes = std::fs::read(&path).expect("read");
    assert!(!bytes.is_empty(), "test-handle.wasm is empty");
}
