//! Cross-language Ed25519 signing wire-contract tests (issue #611).
//!
//! These tests decode the same single positive JSON fixture committed at
//! `edge-control-plane/internal/signing/testdata/well_known_signature.json`
//! that the Go-side `internal/signing/wire_contract_test.go` consumes.
//! The fixture is the *byte-exact* output of the Go signer's all-zero-seed
//! test keypair over `(sha256("") || "d_00000000000000000000")` — a real
//! Ed25519 signature that the Rust worker keyring must accept.
//!
//! A drift on either side — payload-layout reorder, hash-vs-hex swap,
//! base64url vs standard, kid-mismatch — turns a test red on whichever
//! side introduced the change *and* on the opposite side's acceptance
//! test (i.e. the Go side byte-equals fails when only the Rust verifier
//! shifts order, and vice versa). Mirrors the pattern in
//! `tests/nats_wire_contract.rs`.

use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use edge_worker::verifier::{Keyring, SigError};
use serde::Deserialize;

/// One positive Ed25519 fixture used by both languages. Decoded once
/// per test (cheap; the JSON is 7 lines). The Rust field names match
/// the JSON keys exactly, so no `#[serde(rename = ...)]` is needed —
/// absence is deliberate, signaling "no field-name transformation."
#[derive(Debug, Deserialize)]
struct WellKnownFixture {
    kid: String,
    pubkey_hex: String,
    deployment_id: String,
    hash_hex: String,
    signature: String,
}

fn signing_fixture() -> &'static str {
    include_str!("../../edge-control-plane/internal/signing/testdata/well_known_signature.json")
}

fn parse_fixture() -> WellKnownFixture {
    let fx: WellKnownFixture =
        serde_json::from_str(signing_fixture()).expect("well_known_signature.json must parse");
    // Belt-and-braces: serde_json by default errors on missing fields
    // (we get `Error("missing field ...")` at parse time above), but a
    // field explicitly set to `""` would deserialize as an empty
    // String and silently slip past the parse step. Assert the
    // surface string-level invariant so a future fixture edit
    // (e.g. someone writes `"signature": ""` to silence an unrelated
    // check) still surfaces a clear diagnostic instead of a confusing
    // drift message downstream. Mirror the Go loader's empty-field
    // check.
    assert!(
        !fx.kid.is_empty(),
        "well_known_signature.json: required field 'kid' is empty"
    );
    assert!(
        !fx.pubkey_hex.is_empty(),
        "well_known_signature.json: required field 'pubkey_hex' is empty"
    );
    assert!(
        !fx.deployment_id.is_empty(),
        "well_known_signature.json: required field 'deployment_id' is empty"
    );
    assert!(
        !fx.hash_hex.is_empty(),
        "well_known_signature.json: required field 'hash_hex' is empty"
    );
    assert!(
        !fx.signature.is_empty(),
        "well_known_signature.json: required field 'signature' is empty"
    );
    fx
}

/// Builds a one-entry keyring whose single key matches the fixture's
/// `pubkey_hex`. Mirrors `go/control-plane`'s `LoadKeyringFromInline`
/// file format (`<kid> = <64-hex>`) — and the same wire shape the Go
/// side uses via `allZeroKeyringInline` in `wire_contract_test.go`.
/// The fixture's pubkey is the verifying key derived from the
/// all-zero-seed test keypair (deterministic across processes).
fn keyring_for_fixture(fx: &WellKnownFixture) -> Keyring {
    let raw = format!("{} = {}\n", fx.kid, fx.pubkey_hex);
    Keyring::from_inline(&raw).expect("fixture pubkey must build a valid keyring")
}

// 1. Positive: the well-known Go signer signature must verify in the
//    Rust keyring. This is the headline test — a payload-layout or
//    base64-alphabet drift on either side breaks this.
//
//    Failure-shape notes:
//      - If verifier.rs changed order → returns Ok(false) (catches the drift).
//      - If verifier.rs swapped to base64-standard → returns
//        Err(SigError::InvalidBase64(_)) (decode step rejects `=`/`+`).
//      - If the fixture pubkey is wrong → returns Ok(false) (wrong-key check).
//      - If the Go signer reverted to hex form → the Rust side still
//        verifies with no error and Ok(true) would be misleading… but
//        the Go-side byte-equality test fires first in that case, so
//        the failure surfaces there.
#[test]
fn well_known_signature_verifies_in_rust_keyring() {
    let fx = parse_fixture();
    let keyring = keyring_for_fixture(&fx);

    let ok = keyring
        .verify(
            &fx.hash_hex,
            &fx.deployment_id,
            &fx.signature,
            Some(&fx.kid),
        )
        .expect("well-known signature must parse and route through verify without erroring");

    assert!(
        ok,
        "well-known signature did NOT verify in the Rust keyring — \
         check verifier.rs for hash/deployment_id order drift, base64 \
         alphabet, or pubkey/seed handling drift"
    );
}

// 2. Replay across deployment_ids. Same signature, swap to a different
//    id → Ok(false), not Err. The (hash || id) binding is the whole
//    reason the id is in the signed payload.
#[test]
fn well_known_signature_rejects_replay_across_deployment_ids() {
    let fx = parse_fixture();
    let keyring = keyring_for_fixture(&fx);

    let ok = keyring
        .verify(&fx.hash_hex, "d_replay", &fx.signature, Some(&fx.kid))
        .expect("replay attempt must not error at wire-format time");

    assert!(
        !ok,
        "Replay across deployment_ids was accepted — the \
         (hash || deployment_id) binding no longer works"
    );
}

// 3. Tampered byte. Decode fixture.signature with URL_SAFE_NO_PAD,
//    flip a bit at position 32 of the 64-byte Ed25519 sig, re-encode,
//    and expect Ok(false). Verify must not error (the bytes still parse
//    cleanly) but must reject cryptographically.
#[test]
fn well_known_signature_rejects_tampered_byte() {
    let fx = parse_fixture();
    let keyring = keyring_for_fixture(&fx);

    let mut raw = URL_SAFE_NO_PAD
        .decode(&fx.signature)
        .expect("fixture signature must base64url-decode");
    assert_eq!(raw.len(), 64, "Ed25519 signature must be 64 bytes");
    raw[32] ^= 0x80;
    let tampered = URL_SAFE_NO_PAD.encode(&raw);

    let ok = keyring
        .verify(&fx.hash_hex, &fx.deployment_id, &tampered, Some(&fx.kid))
        .expect("tampered signature must not error at wire-format time");

    assert!(
        !ok,
        "Verify accepted a tampered signature — single-bit drift \
         in the signature is no longer detected"
    );
}

// 4. Standard base64 (with `+/=`) is rejected at the decode step.
//    Take 64 deterministic bytes, encode with the standard alphabet,
//    hand the resulting string to verify, expect Err(InvalidBase64(_))
//    — never Ok(false). The Rust verifier surfaces a typed error so
//    operators can distinguish wire-format drift from cryptographic
//    rejection.
#[test]
fn well_known_signature_rejects_standard_base64() {
    let fx = parse_fixture();
    let keyring = keyring_for_fixture(&fx);

    let bytes = [0xAA_u8; 64];
    let standard = BASE64_STANDARD.encode(bytes);
    assert!(
        standard.contains('=') || standard.contains('+'),
        "test invariant: standard-base64 of 64 bytes should introduce \
         `=` padding or `+` chars; got {standard:?}"
    );

    let err = keyring
        .verify(&fx.hash_hex, &fx.deployment_id, &standard, Some(&fx.kid))
        .expect_err("standard-base64 signature must error at decode time");

    assert!(
        matches!(err, SigError::InvalidBase64(_)),
        "expected SigError::InvalidBase64(_), got: {err:?}"
    );
}

// 5. Explicit-kid sanity. The fixture's `kid` resolves to the same
//    key the keyring line declares — catches a future fixture edit
//    where `kid` and the keyring `<kid> = <hex>` line disagree. Runs
//    through `keys.contains_key` rather than `verify` so a typo'd
//    kid would surface even if the signature itself still verifies.
#[test]
fn well_known_signature_kid_resolves_in_keyring() {
    let fx = parse_fixture();
    let keyring = keyring_for_fixture(&fx);

    assert!(
        keyring.keys.contains_key(&fx.kid),
        "keyring does not contain the fixture's kid {:?}; fixture-typo or \
         keyring-parsing drift — check Keyring::from_inline and the \
         fixture's `<kid> = <hex>` line for consistency",
        fx.kid
    );
}
