//! Ed25519 artifact-signature verifier with keyring support (issue #307 PR2 +
//! issue #307 follow-up PR1 — multi-keyring with per-key `kid`).
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
//! - **Key ID** (carried by `AppSpec.signing_key_id`, PR1 follow-up):
//!   optional short operator-chosen label (e.g. `"k1"`) identifying
//!   which key in the worker's keyring signed this artifact. Absent
//!   (`None`) on legacy deployments — the keyring's **default key**
//!   is used. An empty-string `signing_key_id` is treated identically
//!   to `None` (legacy control planes emit empty rather than null for
//!   string fields).
//!
//! ## Key configuration
//!
//! `Keyring` replaces the single-pubkey `SignatureVerifier` of PR2.
//! Operators load one or more public keys from a TOML/JSON file at
//! startup; the worker resolves a key by `kid` for each artifact.
//! See `Config::signing_keyring[_path]` for resolution order.

use std::collections::BTreeMap;
use std::path::Path;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ed25519_dalek::{Signature, Verifier as _, VerifyingKey};

/// Ed25519 signatures are always exactly 64 bytes (RFC 8032 §5.1.6).
const SIG_BYTES: usize = 64;
/// 32-byte public key, hex-encoded with no leading 0x → 64 chars.
const PUBKEY_HEX_LEN: usize = 64;
/// Implicit fallback kid when an artifact arrives without a kid OR with
/// `kid == ""` (the legacy control-plane form). Keeping this as a single
/// named constant lets a future "explicit kid = default" override
/// ripple to one place.
pub(crate) const DEFAULT_KID: &str = "default";

/// Multi-key verifier — one `VerifyingKey` per `kid`, plus an implicit
/// `default` fallback for artifacts that omit `signing_key_id`.
///
/// Constructed once at worker startup; wrapped in `std::sync::RwLock`
/// at the supervisor boundary so an operator-driven reload (or a
/// later hot-reload PR) can swap the map without restarting. The
/// `verify` call itself does not take the lock — that's a caller
/// concern. The `pub` fields exist so the test-helper crate
/// (`edge-test-helpers`) can construct a `Keyring` directly from a
/// raw keypair, mirroring the PR2 pattern.
#[derive(Clone, Debug)]
pub struct Keyring {
    /// kid → verifying-key lookup. Always non-empty by construction:
    /// at minimum holds the `DEFAULT_KID` entry.
    pub keys: BTreeMap<String, VerifyingKey>,
}

impl Keyring {
    /// Build a keyring from a TOML-style line-by-line file. Each
    /// non-comment line is `<kid> = <64-lowercase-hex>`. Whitespace
    /// around the `=` is tolerated; blank lines and lines starting
    /// with `#` are skipped. A literal `[defaults]` block is NOT
    /// supported in this constructor — pass the desired default kid
    /// explicitly via `with_default_kid`.
    ///
    /// The file MUST contain at least one entry. Returns an error if
    /// the file is empty, contains a malformed line, or contains a
    /// duplicate kid.
    pub fn from_file(p: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(p)
            .map_err(|e| anyhow::anyhow!("reading keyring file {}: {e}", p.display()))?;
        Self::from_inline(&raw)
    }

    /// Parse an inline keyring payload (same format as `from_file`).
    /// Used by `Config::from_env` when `EDGE_SIGNING_KEYRING` is set
    /// without a backing file.
    pub fn from_inline(raw: &str) -> anyhow::Result<Self> {
        let mut keys = BTreeMap::new();
        for (lineno, line) in raw.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            // Expect `<kid> = <64-hex>`. Split on the first `=` so a
            // hex value containing `=` (it can't, but defensively) is
            // taken as one piece.
            let Some((kid_part, pk_part)) = trimmed.split_once('=') else {
                anyhow::bail!(
                    "keyring line {}: expected `<kid> = <64-hex>`, got {trimmed:?}",
                    lineno + 1
                );
            };
            let kid = kid_part.trim();
            let pk_hex = pk_part.trim();
            if kid.is_empty() {
                anyhow::bail!("keyring line {}: kid must be non-empty", lineno + 1);
            }
            if keys.contains_key(kid) {
                anyhow::bail!("keyring line {}: duplicate kid {kid:?}", lineno + 1);
            }
            let pub_key = parse_pubkey_hex(pk_hex)
                .map_err(|e| anyhow::anyhow!("keyring line {}: {e}", lineno + 1))?;
            keys.insert(kid.to_string(), pub_key);
        }
        if keys.is_empty() {
            anyhow::bail!("keyring is empty; expected at least one `<kid> = <64-hex>` line");
        }
        Ok(Self { keys })
    }

    /// Construct a single-key keyring from a raw `VerifyingKey`.
    /// Convenience constructor for tests and for the
    /// `EDGE_SIGNING_KEYRING` env-var fallback when the operator
    /// passes a lone pubkey (kid defaults to `DEFAULT_KID`).
    #[allow(dead_code)] // used by integration_tests.rs and Downloader; bin target sees no callers
    pub fn single(pub_key: VerifyingKey) -> Self {
        let mut keys = BTreeMap::new();
        keys.insert(DEFAULT_KID.to_string(), pub_key);
        Self { keys }
    }

    /// Verify that `signature_b64` is a valid Ed25519 signature over
    /// `sha256_raw_32_bytes(deployment_id_bytes) || deployment_id_bytes`,
    /// resolved against `kid`. The hash bytes are reconstructed from
    /// `hash_hex`. The `kid` interpretation:
    ///
    /// - `kid = None` **or** `kid = Some("")` → look up under `DEFAULT_KID`.
    /// - `kid = Some(k)` where `k != ""` → look up under `keys[k]`.
    ///
    /// Returns:
    /// - `Ok(true)` — the signature is valid for this hash + id pair.
    /// - `Ok(false)` — the signature parsed cleanly but `verify`
    ///   returned false. This is a *cryptographic* rejection (wrong
    ///   key, wrong id, tampered bytes, replay), not a wire-format
    ///   bug.
    /// - `Err(SigError)` — the input is malformed at the wire-format
    ///   level (empty id, non-hex hash, bad base64, wrong length,
    ///   ed25519-dalek rejected the sig shape, or the kid did not
    ///   resolve to a key in the keyring).
    pub fn verify(
        &self,
        hash_hex: &str,
        deployment_id: &str,
        signature_b64: &str,
        kid: Option<&str>,
    ) -> Result<bool, SigError> {
        // 1. Empty deployment_id — defensive, the Go signer refuses
        // to emit one, but the worker must never accept it.
        if deployment_id.is_empty() {
            return Err(SigError::EmptyDeploymentID);
        }

        // 2. Resolve the kid. Empty string from a legacy CP is
        // normalized to "use the default key" — this is the most
        // likely silent foot-gun, so it is treated identically to
        // `None` and pinned by a unit test.
        let resolved_kid: &str = match kid {
            None => DEFAULT_KID,
            Some("") => DEFAULT_KID,
            Some(k) => k,
        };
        let pub_key = self
            .keys
            .get(resolved_kid)
            .ok_or_else(|| SigError::UnknownKey {
                kid: resolved_kid.to_string(),
            })?;

        // 3. Re-validate hash hex shape so a corrupted deployment_hash
        // field is surfaced here, not buried inside the base64 decode.
        let hash_bytes = parse_hash_hex(hash_hex)?;

        // 4. Base64url(no-pad) decode. `URL_SAFE_NO_PAD` is strict —
        // any `+`, `/`, or `=` character causes the decode to fail,
        // which is exactly the property the PR1 contract requires.
        let sig_bytes = URL_SAFE_NO_PAD
            .decode(signature_b64)
            .map_err(|e| SigError::InvalidBase64(e.to_string()))?;

        // 5. Length check before ed25519-dalek sees the bytes.
        if sig_bytes.len() != SIG_BYTES {
            return Err(SigError::WrongLength {
                got: sig_bytes.len(),
            });
        }

        // 6. Reconstruct the signed payload: raw hash bytes || raw
        // deployment_id bytes. Matches the Go signer byte-for-byte.
        let mut msg = Vec::with_capacity(32 + deployment_id.len());
        msg.extend_from_slice(&hash_bytes);
        msg.extend_from_slice(deployment_id.as_bytes());

        // 7. Parse the signature. `Signature::from_slice` validates
        // the encoded point's structure; a malformed sig is a
        // wire-format error, not a verify-false.
        let sig = Signature::from_slice(&sig_bytes)
            .map_err(|e| SigError::InvalidEd25519(e.to_string()))?;

        // 8. Cryptographic verify. `Ok(false)` is the normal rejection
        // path for tampered bytes, wrong key, wrong id, etc.
        Ok(pub_key.verify(&msg, &sig).is_ok())
    }
}

/// Typed errors for the verifier. Each variant carries enough context
/// to debug from the worker log without re-running with extra flags.
///
/// Mirror `crate::downloader::verify_hash`'s style: empty / wrong
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
    /// `hash_hex` was not 64 lowercase hex chars. `kind` is a short
    /// label so the error log is self-describing.
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
    /// `kid` was non-empty but did not resolve to a key in the
    /// keyring. Distinct from `verify` returning `Ok(false)` — this
    /// is a configuration bug (worker's keyring doesn't know about
    /// the kid the CP signed with), not a signature mismatch.
    #[error("unknown signing-key id {kid:?}; not present in worker keyring")]
    UnknownKey { kid: String },
}

/// Parse a 64-character lowercase hex string into a `VerifyingKey`.
/// Kept crate-private — only `Keyring::from_inline` and the test
/// crate need it.
fn parse_pubkey_hex(s: &str) -> anyhow::Result<VerifyingKey> {
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
    VerifyingKey::from_bytes(&raw).map_err(|e| anyhow::anyhow!("ed25519 pubkey rejected: {e}"))
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
    //! The verification cases mirror the Go signer's `signer_test.go`
    //! matrix. PR1 (keyring follow-up) adds cases for kid resolution:
    //! default-key fallback, explicit kid, unknown kid, and the
    //! empty-string-kid normalization.

    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    /// SHA-256 of the empty string, in lowercase hex.
    const TEST_HASH_HEX: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    /// Deployment id chosen by PR1's `signer_test.go` (so any fixture
    /// that ships in this crate matches what the Go side produces).
    const TEST_DEPLOYMENT_ID: &str = "d_00000000000000000000";

    /// 32-byte all-zero seed → deterministic test keypair.
    fn test_keypair(seed_byte: u8) -> (SigningKey, VerifyingKey) {
        let seed = [seed_byte; 32];
        let sk = SigningKey::from_bytes(&seed);
        let vk = sk.verifying_key();
        (sk, vk)
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

    fn pubkey_hex(vk: &VerifyingKey) -> String {
        let bytes = vk.to_bytes();
        let mut hex = String::with_capacity(64);
        for b in bytes {
            use std::fmt::Write;
            let _ = write!(hex, "{b:02x}");
        }
        hex
    }

    // ---- PR2 cases (mirrored from the previous file) ----

    #[test]
    fn verify_accepts_valid_signature() {
        let (sk, vk) = test_keypair(0);
        let keyring = Keyring::single(vk);
        let sig = sign_for_test(&sk, TEST_HASH_HEX, TEST_DEPLOYMENT_ID);
        let ok = keyring
            .verify(TEST_HASH_HEX, TEST_DEPLOYMENT_ID, &sig, None)
            .expect("verify should not error on a well-formed signature");
        assert!(ok);
    }

    #[test]
    fn verify_rejects_uppercase_hex_hash() {
        let (_sk, vk) = test_keypair(0);
        let keyring = Keyring::single(vk);
        let bad = TEST_HASH_HEX.to_uppercase();
        let err = keyring
            .verify(&bad, TEST_DEPLOYMENT_ID, "valid_b64_placeholder", None)
            .expect_err("uppercase hex hash must be rejected");
        assert!(matches!(err, SigError::InvalidHex { kind: "hash", .. }));
    }

    #[test]
    fn verify_rejects_empty_signature() {
        let (_sk, vk) = test_keypair(0);
        let keyring = Keyring::single(vk);
        let err = keyring
            .verify(TEST_HASH_HEX, TEST_DEPLOYMENT_ID, "", None)
            .expect_err("empty signature must be rejected");
        assert!(matches!(
            err,
            SigError::InvalidBase64(_) | SigError::WrongLength { got: 0 }
        ));
    }

    #[test]
    fn verify_rejects_wrong_length_signature() {
        let (_sk, vk) = test_keypair(0);
        let keyring = Keyring::single(vk);
        let short = URL_SAFE_NO_PAD.encode([0u8; 32]);
        let err = keyring
            .verify(TEST_HASH_HEX, TEST_DEPLOYMENT_ID, &short, None)
            .expect_err("32-byte signature must be rejected as wrong length");
        assert!(matches!(err, SigError::WrongLength { got: 32 }));
    }

    #[test]
    fn verify_rejects_standard_base64() {
        use base64::engine::general_purpose::STANDARD;
        let (_sk, vk) = test_keypair(0);
        let keyring = Keyring::single(vk);
        let standard = STANDARD.encode([0xAAu8; 64]);
        let err = keyring
            .verify(TEST_HASH_HEX, TEST_DEPLOYMENT_ID, &standard, None)
            .expect_err("standard base64 must be rejected");
        assert!(matches!(err, SigError::InvalidBase64(_)));
    }

    #[test]
    fn verify_returns_false_on_random_signature() {
        let (_sk, vk) = test_keypair(0);
        let keyring = Keyring::single(vk);
        let random_sig = URL_SAFE_NO_PAD.encode([0x42u8; 64]);
        let ok = keyring
            .verify(TEST_HASH_HEX, TEST_DEPLOYMENT_ID, &random_sig, None)
            .expect("verify should not error on a well-formed but wrong signature");
        assert!(!ok);
    }

    #[test]
    fn verify_rejects_replay_across_deployment_ids() {
        let (sk, vk) = test_keypair(0);
        let keyring = Keyring::single(vk);
        let sig = sign_for_test(&sk, TEST_HASH_HEX, "d_original");
        let ok = keyring
            .verify(TEST_HASH_HEX, "d_replay", &sig, None)
            .expect("verify should not error on a well-formed but wrong-id signature");
        assert!(!ok);
    }

    #[test]
    fn verify_rejects_replay_across_hashes() {
        let (sk, vk) = test_keypair(0);
        let keyring = Keyring::single(vk);
        let sig = sign_for_test(&sk, TEST_HASH_HEX, TEST_DEPLOYMENT_ID);
        let other_hash = "0".repeat(64);
        let ok = keyring
            .verify(&other_hash, TEST_DEPLOYMENT_ID, &sig, None)
            .expect("verify should not error on a well-formed but wrong-hash signature");
        assert!(!ok);
    }

    // ---- PR1 (follow-up) cases: keyring kid resolution ----

    /// `kid = None` and a single-key keyring verifies successfully.
    #[test]
    fn verify_no_kid_single_key_keyring() {
        let (sk, vk) = test_keypair(0);
        let keyring = Keyring::single(vk);
        let sig = sign_for_test(&sk, TEST_HASH_HEX, TEST_DEPLOYMENT_ID);
        let ok = keyring
            .verify(TEST_HASH_HEX, TEST_DEPLOYMENT_ID, &sig, None)
            .expect("verify");
        assert!(ok);
    }

    /// `kid = Some("")` (legacy CP form) resolves to the default key.
    /// The most likely silent foot-gun: a pre-PR1-follow-up control
    /// plane emits an empty-string kid instead of null. The worker
    /// MUST treat it identically to `None`, not crash on the empty
    /// key lookup.
    #[test]
    fn verify_empty_kid_falls_back_to_default_key() {
        let (sk, vk) = test_keypair(0);
        let keyring = Keyring::single(vk);
        let sig = sign_for_test(&sk, TEST_HASH_HEX, TEST_DEPLOYMENT_ID);
        let ok = keyring
            .verify(TEST_HASH_HEX, TEST_DEPLOYMENT_ID, &sig, Some(""))
            .expect("empty-string kid must resolve to default key");
        assert!(ok);
    }

    /// `kid = Some("k1")` resolves to the matching key in a multi-key
    /// keyring. Two distinct test keypairs have separate sigs; the
    /// right key verifies.
    #[test]
    fn verify_explicit_kid_resolves_in_multi_key_keyring() {
        let (sk1, vk1) = test_keypair(1);
        let (_sk2, vk2) = test_keypair(2);
        let mut keys = BTreeMap::new();
        keys.insert("k1".to_string(), vk1);
        keys.insert("k2".to_string(), vk2);
        let keyring = Keyring { keys };

        let sig = sign_for_test(&sk1, TEST_HASH_HEX, TEST_DEPLOYMENT_ID);
        let ok = keyring
            .verify(TEST_HASH_HEX, TEST_DEPLOYMENT_ID, &sig, Some("k1"))
            .expect("explicit kid must verify against matching key");
        assert!(ok);
    }

    /// `kid = Some("unknown")` returns `UnknownKey`, not `Ok(false)`.
    /// Distinct error type so an operator can diagnose
    /// "keyring config drift" from "signature does not verify."
    #[test]
    fn verify_unknown_kid_returns_unknown_key_error() {
        let (_sk, vk) = test_keypair(0);
        let keyring = Keyring::single(vk);
        let err = keyring
            .verify(
                TEST_HASH_HEX,
                TEST_DEPLOYMENT_ID,
                "valid_b64_placeholder",
                Some("k_does_not_exist"),
            )
            .expect_err("unknown kid must produce UnknownKey");
        assert!(
            matches!(err, SigError::UnknownKey { ref kid } if kid == "k_does_not_exist"),
            "expected UnknownKey(k_does_not_exist), got: {err:?}"
        );
    }

    /// Wrong kid for a valid signature → `Ok(false)`, not `UnknownKey`.
    /// The signature was correctly formed for `vk2` but `k1` was
    /// requested; the lookup succeeds, the verify fails. This is the
    /// path operators hit when they rotate keys but forget to update
    /// the CP's `EDGE_SIGNING_KEY_ID`.
    #[test]
    fn verify_wrong_kid_for_valid_signature_returns_false() {
        let (sk1, vk1) = test_keypair(1);
        let (sk2, vk2) = test_keypair(2);
        let mut keys = BTreeMap::new();
        keys.insert("k1".to_string(), vk1);
        keys.insert("k2".to_string(), vk2);
        let keyring = Keyring { keys };

        // Sign with sk2 (kid k2's key).
        let sig = sign_for_test(&sk2, TEST_HASH_HEX, TEST_DEPLOYMENT_ID);
        // Verify claiming kid k1 — sig won't match.
        let ok = keyring
            .verify(TEST_HASH_HEX, TEST_DEPLOYMENT_ID, &sig, Some("k1"))
            .expect("verify should not error on well-formed sig + wrong key");
        assert!(!ok, "sig for k2 must not verify against k1");
        // Suppress unused warning on sk1 — it's only there to make
        // the keypair identities distinct.
        let _ = sk1;
    }

    // ---- from_inline / from_file ----

    #[test]
    fn from_inline_accepts_multiple_keys() {
        let (_sk1, vk1) = test_keypair(1);
        let (_sk2, vk2) = test_keypair(2);
        let raw = format!(
            "k1 = {}\nk2 = {}\n# comment line\n\nk3 = {}\n",
            pubkey_hex(&vk1),
            pubkey_hex(&vk2),
            pubkey_hex(&vk1), // duplicate vk1, different kid — both legit
        );
        let keyring = Keyring::from_inline(&raw).expect("parse multi-key keyring");
        assert_eq!(keyring.keys.len(), 3);
        assert!(keyring.keys.contains_key("k1"));
        assert!(keyring.keys.contains_key("k2"));
        assert!(keyring.keys.contains_key("k3"));
    }

    #[test]
    fn from_inline_rejects_duplicate_kid() {
        let (_sk, vk) = test_keypair(0);
        let raw = format!("k1 = {}\nk1 = {}\n", pubkey_hex(&vk), pubkey_hex(&vk),);
        let err = Keyring::from_inline(&raw).expect_err("duplicate kid must error");
        assert!(
            err.to_string().contains("duplicate kid"),
            "expected duplicate-kid error, got: {err}"
        );
    }

    #[test]
    fn from_inline_rejects_empty() {
        let err = Keyring::from_inline("").expect_err("empty keyring must error");
        assert!(
            err.to_string().contains("empty"),
            "expected empty error, got: {err}"
        );
    }

    #[test]
    fn from_inline_rejects_malformed_line() {
        let err = Keyring::from_inline("not a valid line").expect_err("malformed line must error");
        assert!(
            err.to_string().contains("expected `<kid> = <64-hex>`"),
            "expected malformed-line error, got: {err}"
        );
    }

    #[test]
    fn from_inline_rejects_invalid_pubkey_hex() {
        let err = Keyring::from_inline("k1 = zzzz_not_hex").expect_err("bad hex must error");
        assert!(
            err.to_string().contains("lowercase hex")
                || err.to_string().contains("non-hex")
                || err.to_string().contains("wrong"),
            "expected hex-shape error, got: {err}"
        );
    }

    #[test]
    fn from_file_reads_keyring_with_trailing_whitespace() {
        let (_sk, vk) = test_keypair(0);
        let raw = format!("k1 = {}\n", pubkey_hex(&vk));
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("keyring.txt");
        std::fs::write(&path, &raw).expect("write keyring");
        let keyring = Keyring::from_file(&path).expect("file load should succeed");
        assert_eq!(keyring.keys.len(), 1);
    }
}
