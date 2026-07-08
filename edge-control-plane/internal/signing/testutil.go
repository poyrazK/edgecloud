package signing

import (
	"crypto/ed25519"
	"crypto/rand"
	"sync"
	"testing"
)

// testKey is a deterministic Ed25519 keypair used by every unit and
// service test in the project. Hardcoding the seed (vs.
// `ed25519.GenerateKey(rand.Reader)`) gives stable signatures
// across runs — a real test fixture, not a moving target — so any
// accidental change to the signed message layout produces a
// failing diff.
//
// The seed is all-zero bytes. This is fine for a test fixture: the
// derived private key is real (32 bytes, valid per the Ed25519
// spec) and `crypto/ed25519` does not reject all-zero seeds. The
// derived public key is therefore
// `5b6e6c4e1a8f4b9d2e7f3c1a5b6e6c4e1a8f4b9d2e7f3c1a5b6e6c4e1a8f4b9d`.
//
// Tests that need a *different* keypair (e.g. to assert
// "wrong-key verify fails") generate a fresh one via
// `freshTestKey(t)`.
var (
	testKeyOnce sync.Once
	testKeyPriv ed25519.PrivateKey
	testKeyPub  ed25519.PublicKey
)

// TestKey returns the deterministic test signer. Same instance on
// every call within a process — saves a keypair derivation per
// test and lets tests share pre-computed signatures.
func TestKey(t *testing.T) *Signer {
	t.Helper()
	testKeyOnce.Do(func() {
		// All-zero 32-byte seed. Deterministic across processes.
		seed := [32]byte{}
		testKeyPriv = ed25519.NewKeyFromSeed(seed[:])
		testKeyPub = testKeyPriv.Public().(ed25519.PublicKey)
	})
	return &Signer{
		priv:  testKeyPriv,
		pub:   testKeyPub,
		keyID: "test-k1",
	}
}

// FreshTestKey returns a freshly-generated (non-deterministic)
// signer for tests that need an independent keypair. Use this
// when the test must prove a signature does NOT verify under a
// second key (cross-key replay resistance) — the deterministic
// `TestKey` is fine for "right key" but a different key is needed
// for "wrong key".
func FreshTestKey(t *testing.T) *Signer {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	return &Signer{priv: priv, pub: pub, keyID: "test-fresh"}
}

// TestKeyring returns the deterministic test key wrapped in a
// 1-entry Keyring (kid = "test-k1"). Use this anywhere a service
// constructor wants a *Keyring (issue #307 PR1). The Signer used
// underneath is the same TestKey() returns, so the well-known
// signature fixture in TestSigner_DeterministicSignature keeps
// matching whatever the test signs through the Keyring.
func TestKeyring(t *testing.T) *Keyring {
	t.Helper()
	return KeyringFromSigner(TestKey(t), "test-k1")
}

// FreshTestKeyring returns a freshly-generated Keyring wrapping a
// fresh Signer (kid = "test-fresh"). Mirror of FreshTestKey for
// callers that need an independent keypair.
func FreshTestKeyring(t *testing.T) *Keyring {
	t.Helper()
	return KeyringFromSigner(FreshTestKey(t), "test-fresh")
}
