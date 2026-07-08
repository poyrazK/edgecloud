package provenance

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/signing"
)

// fixedTestSeed is the deterministic 32-byte seed used by every
// provenance unit test. Same shape as signing.testutil's all-zero
// seed (so any "I changed the test signer" diff in the signing
// package's signature fixtures surfaces here as a parallel diff).
var fixedTestSeed = [32]byte{}

// testKeyring returns the deterministic test key wrapped in a
// 1-entry Keyring with kid = "test-k1". Mirrors
// signing.TestKeyring but lives in this package so the tests can
// stay self-contained (no cross-package test helper import).
func testKeyring(t *testing.T) *signing.Keyring {
	t.Helper()
	priv := ed25519.NewKeyFromSeed(fixedTestSeed[:])
	pub := priv.Public().(ed25519.PublicKey)
	return keyringFromRawKey(t, priv, pub, "test-k1")
}

// testFreshKeyring returns a freshly-generated keyring. Used for
// the cross-keyring rejection test — proves the verifier doesn't
// accept an envelope signed by a key it doesn't hold.
func testFreshKeyring(t *testing.T) *signing.Keyring {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	return keyringFromRawKey(t, priv, pub, "test-fresh")
}

// keyringFromRawKey builds a 1-entry Keyring by serializing the
// raw private key through signing.LoadFromRaw (which accepts the
// 64-byte RFC 8032 §5.1.2 form). We can't construct a *Signer
// directly because its fields are unexported, but LoadFromRaw is
// the public seam that produces one.
func keyringFromRawKey(t *testing.T, priv ed25519.PrivateKey, pub ed25519.PublicKey, kid string) *signing.Keyring {
	t.Helper()
	_ = pub // priv already carries pub in its high 32 bytes; pub arg is for clarity
	signer, err := signing.LoadFromRaw(priv, kid)
	if err != nil {
		t.Fatalf("signing.LoadFromRaw: %v", err)
	}
	return signing.KeyringFromSigner(signer, kid)
}
