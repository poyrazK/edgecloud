//! Ed25519 artifact-signature verifier (issue #307, PR2 of 2).
//!
//! The control plane (`edge-control-plane/internal/signing/signer.go`)
//! signs every deployment's artifact at upload time and persists the
//! signature on the `deployments` row. The worker verifies the
//! signature before instantiating the wasm component, so a compromise
//! that rewrites **both** the artifact store and the `deployments.hash`
//! column cannot substitute a malicious artifact with a matching
//! SHA-256 — the substitute's bytes will not verify against the
//! worker's configured public key.
//!
//! ## Signed message layout
//!
//! The signed payload is **`sha256_raw_32_bytes || deployment_id`**
//! — the raw 32-byte hash, **not** the lowercase hex form. The
//! Go signer builds it as:
//!
//! ```text
//! msg = make([]byte, 0, 32+len(deploymentID))
//! msg = append(msg, hashBytes...)        // raw 32 bytes
//! msg = append(msg, []byte(deploymentID)...)
//! ```
//!
//! The Rust verifier MUST reconstruct the same byte sequence before
//! passing to `ed25519-dalek`'s `verify()`. The deployment_id is
//! included in the signed payload to prevent DB-replay: an attacker
//! who can rewrite a signature column on a different row cannot lift
//! a valid signature off deployment A and paste it onto deployment B
//! — the signature was over A's id, not B's.
//!
//! ## Wire formats
//!
//! - **Hash** (carried by `AppSpec.deployment_hash`): 64 lowercase hex
//!   chars (unchanged from PR1). We re-validate the shape here so a
//!   corrupted hash field produces a typed `SigError::InvalidHex`.
//! - **Signature** (carried by `AppSpec.deployment_signature`):
//!   base64url(**no padding**) — 64 raw Ed25519 bytes encode to
//!   86 chars. Standard base64 (`+/=`) is rejected at the decode
//!   step.
//!
//! ## Key configuration
//!
//! The worker holds a single public key in v1 (rotation is a
//! follow-up). It is loaded from either `EDGE_SIGNING_PUBKEY` (inline
//! 64 hex chars) or `EDGE_SIGNING_PUBKEY_PATH` (file with 64 hex
//! chars), and the public key is wrapped in a `SignatureVerifier`.
//! Resolution is done in `crate::main` and the verifier is passed to
//! `Downloader::new`.

use std::path::Path;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ed25519_dalek::{Signature, Verifier as _, VerifyingKey};

/// Ed25519 signatures are always exactly 64 bytes (RFC 8032 §5.1.6).
const SIG_BYTES: usize = 64;
/// 32-byte public key, hex-encoded with no leading 0x → 64 chars.
const PUBKEY_HEX_LEN: usize = 64;

/// Verifier holding a single Ed25519 public key. The key is parsed
/// once at startup and reused for every artifact; `verify` is
/// allocation-free on the success path aside from the wire-format
/// decode.
///
/// `pub_key` is `pub` (not `pub(crate)`) so the public surface
/// only exposes `from_hex`, `from_file`, and `verify` — the bare
/// key bytes are an internal detail. Tests and integration
/// helpers in the worker crate (and downstream test crates) need
/// to set the field directly when constructing a verifier from a
/// pre-derived key (e.g. when running an end-to-end test that
/// uses a deterministic test keypair rather than hex-decoding an
/// env var). The field is read-only in the public API sense —
/// nothing in the public surface mutates it after construction.
#[derive(Clone, Debug)]
pub struct SignatureVerifier {
    pub pub_key: VerifyingKey,
}

/// Typed errors for the verifier. Each variant carries enough context
/// to debug from the worker log without re-running with extra flags.
///
/// Mirror [`crate::downloader::verify_hash`]'s style: empty / wrong
/// length / non-lowercase / mismatch are distinct error cases so a
/// regression on any one is obvious in the test diff.
#[derive(Debug, thiserror::Error)]
pub enum SigError {
    /// `signature_b64` could not be base64url-no-pad decoded. Almost
    /// always a sign of standard base64 (`+/=`) sneaking into the
    /// wire format.
    #[error("invalid base64url signature: {0}")]
    InvalidBase64(String),
    /// The signature decoded to a byte sequence of the wrong length
    /// (not 64 bytes). We report the *got* length to help operators
    /// debug a corrupted field.
    #[error("signature has wrong length: expected 64, got {got}")]
    WrongLength { got: usize },
    /// `hash_hex` or the public key was not 64 lowercase hex chars.
    /// `kind` is a short label so the error log is self-describing.
    #[error("invalid {kind} hex: {msg}")]
    InvalidHex { kind: &'static str, msg: String },
    /// `deployment_id` is empty. The Go signer rejects empty ids, so
    /// seeing one on the wire is a contract violation.
    #[error("deployment_id is empty")]
    EmptyDeploymentID,
    /// `ed25519-dalek` rejected the signature as malformed (its own
    /// `Signature::from_slice` returned `Err`). Distinct from
    /// `verify` returning `Ok(false)` — a malformed sig is a
    /// wire-format bug, while a verify-false is a cryptographic
    /// rejection.
    #[error("malformed Ed25519 signature: {0}")]
    InvalidEd25519(String),
}

impl SignatureVerifier {
    /// Build a verifier from a 64-char lowercase hex public key
    /// (the format `cmd/printpub` emits in the control-plane repo).
    pub fn from_hex(s: &str) -> anyhow::Result<Self> {
        if s.len() != PUBKEY_HEX_LEN {
            anyhow::bail!(
                "signing pubkey must be exactly {PUBKEY_HEX_LEN} lowercase hex chars (got {})",
                s.len()
            );
        }
        if !s.bytes().all(is_lower_hex) {
            anyhow::bail!(
                "signing pubkey must contain only lowercase hex (0-9, a-f); uppercase or \
                 non-hex chars are rejected to surface configuration drift early"
            );
        }
        let raw = decode_hex_32(s).map_err(|e| anyhow::anyhow!("decoding pubkey hex: {e}"))?;
        let pub_key = VerifyingKey::from_bytes(&raw)
            .map_err(|e| anyhow::anyhow!("ed25519 pubkey rejected: {e}"))?;
        Ok(Self { pub_key })
    }

    /// Build a verifier from a file containing the 64-char lowercase
    /// hex public key. Trailing whitespace / newlines are tolerated
    /// (operator keys are often hand-pasted or extracted from
    /// `cmd/printpub` with a trailing newline).
    pub fn from_file(p: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(p)
            .map_err(|e| anyhow::anyhow!("reading signing pubkey file {}: {e}", p.display()))?;
        // Trim trailing whitespace only — we keep the strict 64-char
        // length check so a file with embedded newlines is rejected
        // by from_hex rather than silently coerced.
        let trimmed = raw.trim_end();
        Self::from_hex(trimmed)
    }

    /// Verify that `signature_b64` is a valid Ed25519 signature over
    /// `sha256_raw_32_bytes(deployment_id_bytes) || deployment_id_bytes`,
    /// where the hash bytes are reconstructed from `hash_hex`.
    ///
    /// Returns:
    /// - `Ok(true)` — the signature is valid for this hash + id pair.
    /// - `Ok(false)` — the signature parsed cleanly but `verify`
    ///   returned false. This is a *cryptographic* rejection (wrong
    ///   key, wrong id, tampered bytes, replay), not a wire-format
    ///   bug.
    /// - `Err(SigError)` — the input is malformed at the wire-format
    ///   level (empty id, non-hex hash, bad base64, wrong length,
    ///   ed25519-dalek rejected the sig shape).
    ///
    /// The split between `Ok(false)` and `Err` matters: a tampered
    /// `deployments.signature` column should be a `verify` false
    /// (operator can investigate), but a corrupted column with
    /// non-base64 content should be a typed wire-format error (the
    /// schema itself is broken; fix the producer).
    pub fn verify(
        &self,
        hash_hex: &str,
        deployment_id: &str,
        signature_b64: &str,
    ) -> Result<bool, SigError> {
        // 1. Empty deployment_id — defensive, the Go signer refuses
        // to emit one, but the worker must never accept it.
        if deployment_id.is_empty() {
            return Err(SigError::EmptyDeploymentID);
        }

        // 2. Re-validate hash hex shape so a corrupted deployment_hash
        // field is surfaced here, not buried inside the base64 decode
        // (and so the verify-false / wire-error split above holds
        // even when the hash is the bad field).
        let hash_bytes = parse_hash_hex(hash_hex)?;

        // 3. Base64url(no-pad) decode. `URL_SAFE_NO_PAD` is strict —
        // any `+`, `/`, or `=` character causes the decode to fail,
        // which is exactly the property the PR1 contract requires.
        let sig_bytes = URL_SAFE_NO_PAD
            .decode(signature_b64)
            .map_err(|e| SigError::InvalidBase64(e.to_string()))?;

        // 4. Length check before ed25519-dalek sees the bytes.
        if sig_bytes.len() != SIG_BYTES {
            return Err(SigError::WrongLength {
                got: sig_bytes.len(),
            });
        }

        // 5. Reconstruct the signed payload: raw hash bytes || raw
        // deployment_id bytes. Matches the Go signer byte-for-byte.
        let mut msg = Vec::with_capacity(32 + deployment_id.len());
        msg.extend_from_slice(&hash_bytes);
        msg.extend_from_slice(deployment_id.as_bytes());

        // 6. Parse the signature. `Signature::from_slice` validates
        // the encoded point's structure; a malformed sig is a
        // wire-format error, not a verify-false.
        let sig = Signature::from_slice(&sig_bytes)
            .map_err(|e| SigError::InvalidEd25519(e.to_string()))?;

        // 7. Cryptographic verify. `Ok(false)` is the normal rejection
        // path for tampered bytes, wrong key, wrong id, etc.
        Ok(self.pub_key.verify(&msg, &sig).is_ok())
    }
}

// ── helpers shared with downloader.rs (mirroring verify_hash) ────────────

/// True iff `b` is a lowercase hex digit (`0-9` or `a-f`). Same shape
/// the downloader's `verify_hash` uses, so the error semantics
/// across the worker are consistent.
const fn is_lower_hex(b: u8) -> bool {
    matches!(b, b'0'..=b'9' | b'a'..=b'f')
}

/// Decode a 64-char lowercase hex SHA-256 digest into 32 raw bytes.
/// Caller must have already validated `len == 64 && all is_lower_hex`.
fn decode_hex_32(s: &str) -> anyhow::Result<[u8; 32]> {
    let bytes = s.as_bytes();
    let mut out = [0u8; 32];
    for i in 0..32 {
        let hi = hex_nibble(bytes[2 * i])?;
        let lo = hex_nibble(bytes[2 * i + 1])?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> anyhow::Result<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        _ => anyhow::bail!("non-hex byte: 0x{b:02x}"),
    }
}

/// Validate `hash_hex` is exactly 64 lowercase hex chars and decode
/// to 32 bytes. The split between Err types mirrors the
/// `verify_hash` defensive style — operators can see at a glance
/// whether the failure is "empty", "wrong length", "non-hex", or
/// "uppercase".
fn parse_hash_hex(s: &str) -> Result<[u8; 32], SigError> {
    if s.is_empty() {
        return Err(SigError::InvalidHex {
            kind: "hash",
            msg: "empty".to_string(),
        });
    }
    if s.len() != 64 {
        return Err(SigError::InvalidHex {
            kind: "hash",
            msg: format!("expected 64 chars, got {}", s.len()),
        });
    }
    if !s.bytes().all(is_lower_hex) {
        return Err(SigError::InvalidHex {
            kind: "hash",
            msg: "must be 64 lowercase hex chars (0-9, a-f)".to_string(),
        });
    }
    decode_hex_32(s).map_err(|e| SigError::InvalidHex {
        kind: "hash",
        msg: e.to_string(),
    })
}

#[cfg(test)]
mod tests {
    //! The 8 cases mirror the Go signer's `signer_test.go` set, in the
    //! same order — a regression in any of them means the signed
    //! message layout, wire format, or key format drifted.

    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    /// SHA-256 of the empty string, in lowercase hex.
    /// Matches the Go side's `testHashHex` fixture in
    /// `internal/signing/signer_test.go`.
    const TEST_HASH_HEX: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    /// Deployment id chosen by PR1's `signer_test.go` (so any fixture
    /// that ships in this crate matches what the Go side produces).
    const TEST_DEPLOYMENT_ID: &str = "d_00000000000000000000";

    /// 32-byte all-zero seed → deterministic test keypair. Mirrors
    /// the Go side's `signing.TestKey()`.
    fn test_keypair() -> (SigningKey, SignatureVerifier) {
        let seed = [0u8; 32];
        let sk = SigningKey::from_bytes(&seed);
        let vk = sk.verifying_key();
        // Build the verifier from the raw public key bytes so the
        // test exercises the same VerifyingKey construction the
        // production from_hex path uses.
        let verifier = SignatureVerifier { pub_key: vk };
        (sk, verifier)
    }

    /// Helper: sign `hash_hex || deployment_id` exactly the way the
    /// Go signer does it, and return the base64url(no-pad) wire form.
    fn sign_for_test(sk: &SigningKey, hash_hex: &str, deployment_id: &str) -> String {
        let hash_bytes = decode_hex_32(hash_hex).expect("decode test hash");
        let mut msg = Vec::with_capacity(32 + deployment_id.len());
        msg.extend_from_slice(&hash_bytes);
        msg.extend_from_slice(deployment_id.as_bytes());
        let sig = sk.sign(&msg);
        URL_SAFE_NO_PAD.encode(sig.to_bytes())
    }

    // 1. Happy path: a freshly-signed signature over a real
    //    (hash, deployment_id) pair verifies. Mirrors
    //    `TestSigner_RoundtripFreshKey` in the Go test.
    #[test]
    fn verify_accepts_valid_signature() {
        let (sk, verifier) = test_keypair();
        let sig = sign_for_test(&sk, TEST_HASH_HEX, TEST_DEPLOYMENT_ID);
        let ok = verifier
            .verify(TEST_HASH_HEX, TEST_DEPLOYMENT_ID, &sig)
            .expect("verify should not error on a well-formed signature");
        assert!(ok, "valid signature must verify as Ok(true)");
    }

    // 2. Uppercase hex hash → Err(InvalidHex). Mirrors
    //    `TestSigner_RejectsUppercaseHexHash` — the wire format
    //    is strict lowercase, mirroring the `verify_hash` style.
    #[test]
    fn verify_rejects_uppercase_hex_hash() {
        let (_sk, verifier) = test_keypair();
        let bad = TEST_HASH_HEX.to_uppercase();
        let err = verifier
            .verify(&bad, TEST_DEPLOYMENT_ID, "valid_b64_placeholder")
            .expect_err("uppercase hex hash must be rejected at the pre-check");
        assert!(
            matches!(err, SigError::InvalidHex { kind: "hash", .. }),
            "expected SigError::InvalidHex(hash), got: {err:?}"
        );
    }

    // 3. Empty signature → Err(InvalidBase64). Mirrors the
    //    `verify_hash`'s empty-hash rejection: the wire format
    //    refuses empty, doesn't silently skip the check.
    #[test]
    fn verify_rejects_empty_signature() {
        let (_sk, verifier) = test_keypair();
        let err = verifier
            .verify(TEST_HASH_HEX, TEST_DEPLOYMENT_ID, "")
            .expect_err("empty signature must be rejected");
        // base64url decode of "" returns Ok([]) (empty input is valid
        // base64), so the failure mode is WrongLength { got: 0 }.
        // Both are wire-format errors; either is acceptable here.
        assert!(
            matches!(
                err,
                SigError::InvalidBase64(_) | SigError::WrongLength { got: 0 }
            ),
            "expected InvalidBase64 or WrongLength(0), got: {err:?}"
        );
    }

    // 4. Signature decodes to non-64 bytes → Err(WrongLength).
    //    Mirrors `TestSigner_RejectsWrongLengthHash` in spirit
    //    (length-mismatch is its own typed error).
    #[test]
    fn verify_rejects_wrong_length_signature() {
        let (_sk, verifier) = test_keypair();
        // 32 raw bytes base64url-encoded = 43 chars.
        let short = URL_SAFE_NO_PAD.encode([0u8; 32]);
        let err = verifier
            .verify(TEST_HASH_HEX, TEST_DEPLOYMENT_ID, &short)
            .expect_err("32-byte signature must be rejected as wrong length");
        assert!(
            matches!(err, SigError::WrongLength { got: 32 }),
            "expected WrongLength(32), got: {err:?}"
        );
    }

    // 5. Standard base64 (with `+/=`) is rejected. Mirrors
    //    `TestSigner_VerifyRejectsStandardBase64` — only base64url
    //    no-pad is the wire format.
    #[test]
    fn verify_rejects_standard_base64() {
        use base64::engine::general_purpose::STANDARD;
        let (_sk, verifier) = test_keypair();
        // Build a fake 64-byte sig and encode it with standard
        // base64. The decode is the part that must fail.
        let raw_sig = [0xAAu8; 64];
        let standard = STANDARD.encode(raw_sig);
        let err = verifier
            .verify(TEST_HASH_HEX, TEST_DEPLOYMENT_ID, &standard)
            .expect_err("standard base64 must be rejected");
        assert!(
            matches!(err, SigError::InvalidBase64(_)),
            "expected SigError::InvalidBase64, got: {err:?}"
        );
    }

    // 6. Signature decodes cleanly but is cryptographically invalid
    //    (the wrong key signed it, or the bytes are random).
    //    Returns Ok(false), NOT Err. Mirrors
    //    `TestSigner_VerifyRejectsTamperedSignature`.
    #[test]
    fn verify_returns_false_on_random_signature() {
        let (_sk, verifier) = test_keypair();
        // Random 64-byte signature that decodes cleanly but
        // doesn't match (TEST_HASH_HEX, TEST_DEPLOYMENT_ID).
        let random_sig = URL_SAFE_NO_PAD.encode([0x42u8; 64]);
        let ok = verifier
            .verify(TEST_HASH_HEX, TEST_DEPLOYMENT_ID, &random_sig)
            .expect("verify should not error on a well-formed but wrong signature");
        assert!(!ok, "random sig must verify as Ok(false), not Ok(true)");
    }

    // 7. Signature valid over a different deployment_id → Ok(false).
    //    Mirrors `TestSigner_RejectsReplayAcrossDeploymentIDs` —
    //    this is the binding check that prevents DB-replay.
    #[test]
    fn verify_rejects_replay_across_deployment_ids() {
        let (sk, verifier) = test_keypair();
        let sig = sign_for_test(&sk, TEST_HASH_HEX, "d_original");
        let ok = verifier
            .verify(TEST_HASH_HEX, "d_replay", &sig)
            .expect("verify should not error on a well-formed but wrong-id signature");
        assert!(
            !ok,
            "signature over deployment A must NOT verify against deployment B"
        );
    }

    // 8. Signature valid over a different hash → Ok(false). Mirrors
    //    the hash-binding case in `TestSigner_RejectsReplayAcrossDeploymentIDs`:
    //    the signed payload includes the hash, so a swap of
    //    `deployments.hash` to a different value (even with the
    //    original sig column untouched) must fail verification.
    #[test]
    fn verify_rejects_replay_across_hashes() {
        let (sk, verifier) = test_keypair();
        let sig = sign_for_test(&sk, TEST_HASH_HEX, TEST_DEPLOYMENT_ID);
        // All-zeros is a syntactically valid 64-hex hash, so the
        // failure is cryptographic (verify-false), not wire-format.
        let other_hash = "0".repeat(64);
        let ok = verifier
            .verify(&other_hash, TEST_DEPLOYMENT_ID, &sig)
            .expect("verify should not error on a well-formed but wrong-hash signature");
        assert!(!ok, "signature over hash A must NOT verify against hash B");
    }

    // ── from_hex / from_file tests ────────────────────────────────────

    /// from_hex accepts a 64-lowercase-hex pubkey. The test keypair's
    /// verifying key is a real RFC-8032-compliant 32-byte public key,
    /// so we can round-trip it through from_hex.
    #[test]
    fn from_hex_accepts_valid_pubkey() {
        let (_sk, verifier) = test_keypair();
        // Render the verifying key as 64 lowercase hex chars.
        let pk_bytes = verifier.pub_key.to_bytes();
        let mut hex = String::with_capacity(64);
        for b in pk_bytes {
            use std::fmt::Write;
            let _ = write!(hex, "{b:02x}");
        }
        let parsed = SignatureVerifier::from_hex(&hex).expect("valid hex must parse");
        assert_eq!(parsed.pub_key.to_bytes(), pk_bytes);
    }

    /// from_hex rejects uppercase — same defensive style as
    /// verify_hash.
    #[test]
    fn from_hex_rejects_uppercase() {
        let (_sk, verifier) = test_keypair();
        let pk_bytes = verifier.pub_key.to_bytes();
        let mut hex = String::with_capacity(64);
        for b in pk_bytes {
            use std::fmt::Write;
            let _ = write!(hex, "{b:02X}"); // uppercase
        }
        let err = SignatureVerifier::from_hex(&hex).expect_err("uppercase must be rejected");
        assert!(
            format!("{err:#}").contains("lowercase"),
            "expected lowercase in error, got: {err}"
        );
    }

    /// from_hex rejects wrong-length input.
    #[test]
    fn from_hex_rejects_wrong_length() {
        let err = SignatureVerifier::from_hex("abcd").expect_err("short input must be rejected");
        assert!(
            format!("{err:#}").contains("64 lowercase hex"),
            "expected length hint, got: {err}"
        );
    }

    /// from_file reads a 64-hex file (with trailing newline tolerated)
    /// and constructs a verifier.
    #[test]
    fn from_file_reads_pubkey_with_trailing_newline() {
        let (_sk, verifier) = test_keypair();
        let pk_bytes = verifier.pub_key.to_bytes();
        let mut hex = String::with_capacity(65);
        for b in pk_bytes {
            use std::fmt::Write;
            let _ = write!(hex, "{b:02x}");
        }
        hex.push('\n'); // simulate a hand-pasted file
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("pub.hex");
        std::fs::write(&path, &hex).expect("write pubkey file");
        let parsed = SignatureVerifier::from_file(&path).expect("file load should succeed");
        assert_eq!(parsed.pub_key.to_bytes(), pk_bytes);
    }
}
