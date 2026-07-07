package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/signing"
)

// runPrintpub is a tiny harness around the package-level behavior:
// write a fixture key, invoke the same logic the main function runs
// (we don't shell out to `go run` because that's a build-system
// concern, not a unit test), and capture the public key hex.
//
// We exercise LoadFromFile + PublicKeyHex directly via the
// signing package — that's the contract main() depends on. The
// argument-parsing helpers (flag.Name) are not re-tested here; flag
// parsing is stdlib-tested upstream and adds no value.
//
// If a future refactor replaces PublicKeyHex with a non-hex encoding
// (or accidentally swaps the seed into the public-key slot), the
// `ed25519.PublicKey(hex.DecodeString(...))` step below fails fast.
func runPrintpub(t *testing.T, keyPath, keyID string) string {
	t.Helper()
	signer, err := signing.LoadFromFile(keyPath, keyID)
	if err != nil {
		t.Fatalf("LoadFromFile(%q): %v", keyPath, err)
	}
	return signer.PublicKeyHex()
}

func TestPrintpub_PrintsMatchingPublicKey(t *testing.T) {
	// Generate a fresh 32-byte seed so we can verify the printed
	// hex decodes to a public key that has the seed as its
	// corresponding private key. This is the round-trip the doc
	// advertises: an operator generates a key, runs printpub, and
	// the worker can verify signatures made by the same key.
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test_signing.key")
	if err := os.WriteFile(keyPath, seed, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	gotHex := runPrintpub(t, keyPath, "k1")

	// 64 lowercase hex chars per PublicKeyHex contract.
	if len(gotHex) != 64 {
		t.Fatalf("PublicKeyHex length = %d, want 64; got %q", len(gotHex), gotHex)
	}
	for _, c := range gotHex {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Fatalf("PublicKeyHex must be lowercase hex; got char %q in %q", c, gotHex)
		}
	}

	// Round-trip: the printed hex must decode to the public key
	// that corresponds to the seed — both ends must agree, since
	// the worker verifies signatures using this exact byte string.
	priv := ed25519.NewKeyFromSeed(seed)
	wantPub := priv.Public().(ed25519.PublicKey)
	gotPub, err := hex.DecodeString(gotHex)
	if err != nil {
		t.Fatalf("hex.DecodeString(%q): %v", gotHex, err)
	}
	if string(gotPub) != string(wantPub) {
		t.Errorf("printed public key %x does not match the seed's actual public key %x", gotPub, wantPub)
	}

	// And verify the sign side: signing sha256("") || "d_unit"
	// with the seed's expanded private key must verify under the
	// printed public key. This is the precise contract the
	// issue #307 worker uses — if it ever fails, the worker will
	// reject every signature produced by the corresponding
	// control plane.
	msg := append([]byte("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"), []byte("d_unit")...)
	sig := ed25519.Sign(priv, msg)
	if !ed25519.Verify(ed25519.PublicKey(gotPub), msg, sig) {
		t.Errorf("signature over (sha256(\"\") || \"d_unit\") did not verify under printed pubkey")
	}
}

func TestPrintpub_AcceptsRaw64ByteKey(t *testing.T) {
	// The seed file is normally 32 bytes; a 64-byte raw key
	// (seed || public) is also accepted per parsePrivateKey.
	// printpub must work for both shapes — operators sometimes
	// copy the full RFC 8032 key into the file rather than the
	// bare seed.
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	full := ed25519.NewKeyFromSeed(seed) // 64 bytes

	dir := t.TempDir()
	keyPath := filepath.Join(dir, "full.key")
	if err := os.WriteFile(keyPath, full, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	gotHex := runPrintpub(t, keyPath, "")
	if len(strings.TrimSpace(gotHex)) != 64 {
		t.Fatalf("PublicKeyHex for raw-64 input: length %d, want 64; got %q", len(gotHex), gotHex)
	}
}

func TestPrintpub_RejectsEmptyFile(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "empty.key")
	if err := os.WriteFile(keyPath, []byte{}, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// We exercise the same path the main function does, so the
	// error has to surface from LoadFromFile / parsePrivateKey
	// rather than from flag parsing. An empty file fails
	// parsePrivateKey with ErrInvalidKey.
	if _, err := signing.LoadFromFile(keyPath, ""); err == nil {
		t.Fatal("LoadFromFile on empty file should fail; got nil")
	}
}
