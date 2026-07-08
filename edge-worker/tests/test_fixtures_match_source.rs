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
    "f039dc033db3dec5f0c365ae1c9ead16702879b760b2ef5eb11ff72ea25e508a";

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
