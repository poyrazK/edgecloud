package signing

import (
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

// Cross-language wire-contract test (issue #611). The committed
// fixture at testdata/well_known_signature.json is decoded by both
// this package and the Rust worker crate (see
// edge-worker/tests/signing_wire_contract.rs). A drift on either
// side — payload-layout reorder, hash-vs-hex swap, base64url vs
// std encoding, kid-mismatch — fails the test on whichever side
// introduced the change, plus the opposite side's acceptance test.
//
// The fixture is the deterministic cross-language fixture: a real
// Ed25519 signature over
// (sha256("") || "d_00000000000000000000") produced by the
// all-zero-seed test signer (TestKey()). Both the Go signer and
// the Rust verifier must reproduce / accept this signature
// byte-for-byte.

//go:embed testdata/well_known_signature.json
var wellKnownSignatureFixture []byte

// wellKnownFixtureJSON is the shape of the fixture file. Only the
// fields needed by the tests are declared; an unknown field in the
// JSON is silently ignored, so adding more metadata later is a
// non-breaking change.
type wellKnownFixtureJSON struct {
	Kid          string `json:"kid"`
	PubkeyHex    string `json:"pubkey_hex"`
	DeploymentID string `json:"deployment_id"`
	HashHex      string `json:"hash_hex"`
	Signature    string `json:"signature"`
}

func loadWellKnownFixture(t *testing.T) wellKnownFixtureJSON {
	t.Helper()
	var f wellKnownFixtureJSON
	if err := json.Unmarshal(wellKnownSignatureFixture, &f); err != nil {
		t.Fatalf("decode well_known_signature.json: %v", err)
	}
	// A fixture field that's been accidentally dropped would otherwise
	// produce a confusing "drift" message downstream (e.g. empty
	// Signature would fail `got != want` on GoProducesWellKnownSignature
	// instead of pointing at the dropped field). Surface a clear
	// diagnostic here so a future fixture edit goes red in the obvious
	// place, not inside an unrelated assertion.
	for _, c := range []struct {
		field, value string
	}{
		{"kid", f.Kid},
		{"pubkey_hex", f.PubkeyHex},
		{"deployment_id", f.DeploymentID},
		{"hash_hex", f.HashHex},
		{"signature", f.Signature},
	} {
		if c.value == "" {
			t.Fatalf("well_known_signature.json: required field %q is empty — fixture is malformed", c.field)
		}
	}
	return f
}

//  1. Positive: the Go signer reproduces the well-known signature
//     byte-for-byte. Drift message points at signer.go so the
//     failure diff makes the right files leap to mind.
func TestWireContract_GoProducesWellKnownSignature(t *testing.T) {
	fx := loadWellKnownFixture(t)
	ring, err := LoadKeyringFromInline(allZeroKeyringInline(fx.Kid), fx.Kid)
	if err != nil {
		t.Fatalf("LoadKeyringFromInline: %v", err)
	}
	got, _, err := ring.Sign(fx.HashHex, fx.DeploymentID)
	if err != nil {
		t.Fatalf("Keyring.Sign: %v", err)
	}
	if got != fx.Signature {
		t.Errorf("Go signer no longer produces the well-known fixture signature — check signer.go Sign(...) for layout/encoding drift\n  got:  %s\n  want: %s", got, fx.Signature)
	}
}

//  2. Fixture integrity: the pubkey in the fixture must be the one
//     the all-zero-seed test keypair derives, AND the fixture's kid
//     must match the kid that TestKey() stamps onto its Signer.
//     Pins "this fixture's pubkey/kid are the ones the Rust side
//     will trust", so a future fixture edit that swaps in a wrong
//     pubkey (or a future testutil.go rename that drifts the kid)
//     fails locally instead of misleading the Rust side.
func TestWireContract_FixturePubkeyMatchesAllZeroSeed(t *testing.T) {
	fx := loadWellKnownFixture(t)
	if got := TestKey(t).PublicKeyHex(); got != fx.PubkeyHex {
		t.Errorf("fixture pubkey does not match the all-zero-seed derived key\n  got:  %s\n  want: %s", got, fx.PubkeyHex)
	}
	if got := TestKey(t).KeyID(); got != fx.Kid {
		t.Errorf("fixture kid does not match the test signer's kid (testutil.go:46) — rename one or the other\n  got:  %s\n  want: %s", got, fx.Kid)
	}
}

//  3. Go-side round-trip: the same keyring that just produced the
//     well-known signature must also accept it on verify. Catches
//     the (hypothetical) bug where the signer uses a different key
//     or different payload than the verifier expects.
func TestWireContract_KeyringVerifyAcceptsWellKnown(t *testing.T) {
	fx := loadWellKnownFixture(t)
	ring, err := LoadKeyringFromInline(allZeroKeyringInline(fx.Kid), fx.Kid)
	if err != nil {
		t.Fatalf("LoadKeyringFromInline: %v", err)
	}
	ok, err := ring.Verify(fx.HashHex, fx.DeploymentID, fx.Signature, fx.Kid)
	if err != nil {
		t.Fatalf("Keyring.Verify: %v", err)
	}
	if !ok {
		t.Error("Keyring.Verify rejected the fixture's own signature — signer and verifier disagree")
	}
}

//  4. Tampered-byte negative. Flip one bit at position 32 of the
//     decoded sig, re-encode, and expect (false, nil). Verify must
//     not error (the bytes still parse cleanly) but must reject
//     cryptographically.
func TestWireContract_KeyringVerifyRejectsTamperedSignature(t *testing.T) {
	fx := loadWellKnownFixture(t)
	ring, err := LoadKeyringFromInline(allZeroKeyringInline(fx.Kid), fx.Kid)
	if err != nil {
		t.Fatalf("LoadKeyringFromInline: %v", err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(fx.Signature)
	if err != nil {
		t.Fatalf("decode fixture sig: %v", err)
	}
	raw[32] ^= 0x80
	tampered := base64.RawURLEncoding.EncodeToString(raw)

	ok, err := ring.Verify(fx.HashHex, fx.DeploymentID, tampered, fx.Kid)
	if err != nil {
		t.Fatalf("Verify(tampered) returned error (should be clean (false,nil)): %v", err)
	}
	if ok {
		t.Error("Verify accepted a tampered signature — single-bit drift is not detected")
	}
}

//  5. Replay-across-id negative. The same signature on a different
//     deployment_id must not verify. This is the property the
//     (hash || deployment_id) binding gives us.
func TestWireContract_KeyringVerifyRejectsReplayAcrossID(t *testing.T) {
	fx := loadWellKnownFixture(t)
	ring, err := LoadKeyringFromInline(allZeroKeyringInline(fx.Kid), fx.Kid)
	if err != nil {
		t.Fatalf("LoadKeyringFromInline: %v", err)
	}
	ok, err := ring.Verify(fx.HashHex, "d_replay", fx.Signature, fx.Kid)
	if err != nil {
		t.Fatalf("Verify(replay) returned error (should be clean (false,nil)): %v", err)
	}
	if ok {
		t.Error("Verify accepted a replay — signature bound to one deployment_id verified against another")
	}
}

//  6. Standard-base64 negative. Re-encode a fresh signature with
//     base64.StdEncoding (gains `=` padding) and expect Verify to
//     return an error — RawURLEncoding.DecodeString rejects the
//     `=` and `+` characters the standard alphabet introduces.
func TestWireContract_KeyringVerifyRejectsStandardBase64(t *testing.T) {
	fx := loadWellKnownFixture(t)
	ring, err := LoadKeyringFromInline(allZeroKeyringInline(fx.Kid), fx.Kid)
	if err != nil {
		t.Fatalf("LoadKeyringFromInline: %v", err)
	}
	fresh, err := TestKey(t).SignBytes([]byte("payload for fresh sig"))
	if err != nil {
		t.Fatalf("make fresh sig: %v", err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(fresh)
	if err != nil {
		t.Fatalf("decode fresh sig: %v", err)
	}
	standard := base64.StdEncoding.EncodeToString(raw)
	if !strings.Contains(standard, "=") {
		t.Fatalf("test setup invariant: standard-base64 of 64-byte sig should have padding; got %q", standard)
	}

	_, err = ring.VerifyBytes([]byte("payload for fresh sig"), standard, fx.Kid)
	if err == nil {
		t.Error("VerifyBytes accepted a standard-base64 signature — RawURLEncoding decode should reject `=`/`+`")
	}
}

// allZeroKeyringInline builds a `kid = 0000…0000` line using the
// 32-byte all-zero seed (hex of 32 zero bytes = 64 zero chars).
// The format mirrors the file format
// `LoadKeyringFromInline` accepts in production (`<kid> = <64
// hex>`), so the wire surface is the same.
func allZeroKeyringInline(kid string) string {
	zeroSeedHex := strings.Repeat("0", 64)
	return kid + " = " + zeroSeedHex + "\n"
}
