package signing

import (
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
const testDeploymentID = "d_00000000-0000-0000-0000-000000000001"

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
	// signer over (sha256("") ‖ "d_00000000-0000-0000-0000-000000000001").
	const want = "ZqBP0mLys4PlNDuM2viHVEA13kcz8EbA4xOjIpjO4YPmPM5NWgFxzSuyrUsZVY6_ZcsI7zFGXXcjjI2stG9HAA"
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
