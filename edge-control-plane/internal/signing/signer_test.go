package signing

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

// 8 tests mirroring the worker's verify_hash style — one positive
// happy path + 7 negative / edge cases. Keep this set locked: a
// regression on any of them means the on-the-wire signature shape
// (or the signed message layout) drifted, which would silently
// invalidate every signature already issued.

const testHashHex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // sha256("")
// Deployment ID was chosen so the deterministic Ed25519 signature
// over (sha256("") || testDeploymentID) does not contain any
// English-looking base64 substrings ("HAVE", "HAS", etc.) — the
// typos run would otherwise flag the well-known fixture below as
// spelling mistakes. See TestSigner_DeterministicSignature.
const testDeploymentID = "d_00000000000000000000"

//  1. roundtrip: a freshly-generated key signs a real sha256("") and
//     the same key verifies the resulting signature. The signature
//     is non-empty and base64url-decodes to 64 bytes.
func TestSigner_RoundtripFreshKey(t *testing.T) {
	s := FreshTestKey(t)
	hashBytes := sha256.Sum256([]byte("hello"))
	hashHex := hex.EncodeToString(hashBytes[:])

	sig, err := s.Sign(hashHex, testDeploymentID)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if sig == "" {
		t.Fatal("Sign returned empty signature")
	}
	raw, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil {
		t.Fatalf("signature is not valid base64url: %v", err)
	}
	if len(raw) != 64 {
		t.Errorf("expected 64-byte Ed25519 sig, got %d", len(raw))
	}

	ok, err := s.Verify(hashHex, testDeploymentID, sig)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !ok {
		t.Error("Verify returned false on a signature the same key just produced")
	}
}

//  2. deterministic fixture: the test signer reproduces the same
//     signature byte-for-byte across runs. If the signed message
//     layout ever drifts (e.g. someone adds a length prefix, or
//     changes the hash format from raw-bytes to hex), this test
//     fails with a clear diff and the operator catches it before
//     shipping a backwards-incompatible wire format.
func TestSigner_DeterministicSignature(t *testing.T) {
	s := TestKey(t)
	sig1, err := s.Sign(testHashHex, testDeploymentID)
	if err != nil {
		t.Fatalf("Sign #1: %v", err)
	}
	sig2, err := s.Sign(testHashHex, testDeploymentID)
	if err != nil {
		t.Fatalf("Sign #2: %v", err)
	}
	if sig1 != sig2 {
		t.Errorf("deterministic signer produced two different signatures:\n  %s\n  %s", sig1, sig2)
	}
	// And the well-known value the test fixture commits to. If this
	// changes, every existing signature on a running deployment
	// becomes un-verifiable — intentional breakage, not an
	// accident. Computed once with the deterministic zero-seed
	// signer over (sha256("") ‖ testDeploymentID). The base64 below
	// is intentionally free of English-looking substrings so the
	// typo-checker doesn't flag it (see the testDeploymentID comment).
	const want = "5zf8l-yfPBjEZjt8_fNZ_1SnmczHywrcKWaUUDmNAAntz6uM4lVlmyC-5x7jWWTWH6WdZQ4hX9xY7siiMztdDA"
	if sig1 != want {
		t.Errorf("signature drift — the well-known test fixture no longer matches the implementation\n  got:  %s\n  want: %s", sig1, want)
	}
}

//  3. uppercase hex hash → reject. The deployment service's
//     `SaveAndHash` returns lowercase hex; if a future caller
//     passes uppercase, we want a clean error rather than
//     producing a signature over a different byte sequence.
func TestSigner_RejectsUppercaseHexHash(t *testing.T) {
	s := TestKey(t)
	upper := strings.ToUpper(testHashHex)
	_, err := s.Sign(upper, testDeploymentID)
	if !errors.Is(err, ErrInvalidHash) {
		t.Errorf("expected ErrInvalidHash for uppercase hash, got %v", err)
	}
}

//  4. wrong-length hash → reject. 63 chars or 65 chars both fail
//     with ErrInvalidHash; the message must mention the length.
func TestSigner_RejectsWrongLengthHash(t *testing.T) {
	s := TestKey(t)
	for _, n := range []int{0, 1, 63, 65, 128} {
		bad := strings.Repeat("a", n)
		_, err := s.Sign(bad, testDeploymentID)
		if !errors.Is(err, ErrInvalidHash) {
			t.Errorf("len=%d: expected ErrInvalidHash, got %v", n, err)
		}
	}
}

//  5. empty deployment_id → reject. Empty id would produce a
//     signature that the worker can never bind to a specific
//     deployment, so the signer refuses up front.
func TestSigner_RejectsEmptyDeploymentID(t *testing.T) {
	s := TestKey(t)
	_, err := s.Sign(testHashHex, "")
	if !errors.Is(err, ErrInvalidDeploymentID) {
		t.Errorf("expected ErrInvalidDeploymentID, got %v", err)
	}
}

//  6. signature over a different deployment_id does not verify
//     against the original. Pins the (hash, deployment_id) binding
//     — this is the whole reason the signing input includes the
//     id and not just the hash.
func TestSigner_RejectsReplayAcrossDeploymentIDs(t *testing.T) {
	s := TestKey(t)
	sig, err := s.Sign(testHashHex, "d_original")
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	ok, err := s.Verify(testHashHex, "d_replay", sig)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if ok {
		t.Error("signature over deployment A verified against deployment B — replay protection broken")
	}
}

//  7. standard base64 (with `+/=`) is rejected on Verify — only
//     base64url no-padding is the wire format. A worker receiving
//     a signature with `+` or `=` should reject it; the same is
//     true for the CP-side Verify used by tests and operator
//     tooling.
func TestSigner_VerifyRejectsStandardBase64(t *testing.T) {
	s := TestKey(t)
	// Build a fake "standard base64" signature by replacing the
	// base64url chars with base64 chars. The decode is the part
	// that needs to fail — we don't need a real sig here.
	rawSig := make([]byte, 64)
	for i := range rawSig {
		rawSig[i] = byte(i)
	}
	standard := base64.StdEncoding.EncodeToString(rawSig)
	if _, err := s.Verify(testHashHex, testDeploymentID, standard); err == nil {
		t.Error("expected base64 standard decode to fail, got nil error")
	}
}

//  8. tampered signature byte → verify returns false (not panic,
//     not a wrong-type error). This is the property the worker
//     relies on: a single-bit corruption of `deployments.signature`
//     in the DB must produce a clean verify-false, not a runtime
//     error.
func TestSigner_VerifyRejectsTamperedSignature(t *testing.T) {
	s := TestKey(t)
	sig, err := s.Sign(testHashHex, testDeploymentID)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	// Flip a bit in the middle of the signature.
	raw, _ := base64.RawURLEncoding.DecodeString(sig)
	raw[32] ^= 0x80
	tampered := base64.RawURLEncoding.EncodeToString(raw)

	ok, err := s.Verify(testHashHex, testDeploymentID, tampered)
	if err != nil {
		t.Fatalf("Verify returned error (should just be false): %v", err)
	}
	if ok {
		t.Error("Verify returned true for a tampered signature")
	}
}

// Regression: parsePrivateKey must not trim whitespace bytes out of
// raw input. A 32-byte seed is allowed to contain any byte —
// including 0x20 (space), 0x09 (tab), 0x0a (LF), 0x0d (CR). If the
// trailing byte of a raw seed happens to be one of those, an
// older version of trimASCII would strip it, leaving 31 bytes and
// producing a confusing "got 31" error. The fix detects hex-vs-raw
// before trimming, so raw input passes through untouched. (Found
// via `-shuffle on` + `-count=N` stress on issue #307 PR1's
// keyring tests; reproduced locally and pinned here.)
func TestParsePrivateKey_RawSeedWithTrailingWhitespaceByteIsNotTrimmed(t *testing.T) {
	for _, trailing := range []byte{' ', '\t', '\n', '\r'} {
		seed := make([]byte, ed25519.SeedSize)
		// Deterministic non-zero seed body so a successful parse
		// produces a real key, not all-zero garbage.
		for i := range seed[:31] {
			seed[i] = byte(i + 1)
		}
		seed[31] = trailing

		priv, err := parsePrivateKey(seed)
		if err != nil {
			t.Fatalf("trailing=%#x: parsePrivateKey returned error: %v", trailing, err)
		}
		if len(priv) != ed25519.PrivateKeySize {
			t.Fatalf("trailing=%#x: returned priv has len=%d, want %d", trailing, len(priv), ed25519.PrivateKeySize)
		}
		// Round-trip: the seed the parser saw must equal what we
		// passed in. If trimASCII stripped the trailing byte, the
		// first 31 bytes would be reinterpreted as a different
		// seed and produce a different public key.
		gotSeed := priv.Seed()
		for i := range gotSeed {
			if gotSeed[i] != seed[i] {
				t.Fatalf("trailing=%#x: seed byte %d differs: got %#x want %#x", trailing, i, gotSeed[i], seed[i])
			}
		}
	}
}
