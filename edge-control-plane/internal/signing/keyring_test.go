package signing

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

// Pin the on-disk keyring file format (issue #307 PR1). One
// positive happy path + 6 negative / edge cases. Same shape as
// signer_test.go: a regression on any of these means the wire
// format drifted, which would silently invalidate every signature
// issued with the old format.

// testHashHex2 is a separate fixture from signer_test.go's
// testHashHex so a hash-shape regression on the keyring side
// surfaces independently.
const testHashHex2 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
const testDeploymentID2 = "d_kr_00000000000000000000"

// genKey returns a freshly-generated Signer for tests that need
// an independent keypair (cross-key replay resistance). Same
// behavior as FreshTestKey but declared here to keep keyring
// tests self-contained.
func genKey(t *testing.T) *Signer {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	return &Signer{priv: priv, pub: pub, keyID: "test-fresh"}
}

//  1. roundtrip: a freshly-loaded keyring signs a real sha256("")
//     and the same keyring verifies the resulting signature. The
//     returned kid equals the active kid.
func TestKeyring_RoundtripFreshKey(t *testing.T) {
	k := TestKeyring(t)
	sig, kid, err := k.Sign(testHashHex2, testDeploymentID2)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if sig == "" {
		t.Fatal("Sign returned empty signature")
	}
	if kid != k.ActiveKeyID() {
		t.Errorf("Sign returned kid %q, want active kid %q", kid, k.ActiveKeyID())
	}

	ok, err := k.Verify(testHashHex2, testDeploymentID2, sig, kid)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !ok {
		t.Error("Verify returned false on a signature the same keyring just produced")
	}
}

//  2. multi-key keyring: the kid the signer picks matches the
//     signature it produces. Build a keyring with two keys, set
//     the active kid to the SECOND one, sign, then verify against
//     the same kid — must succeed. Verifying the signature under
//     the FIRST kid must fail (proves the keyring dispatches by
//     kid, not by some hidden "first key" shortcut).
func TestKeyring_SignPicksActiveKid(t *testing.T) {
	a := genKey(t)
	b := genKey(t)
	ring, err := LoadKeyringFromInline(
		"ka = "+hexEncode(t, a)+"\nkb = "+hexEncode(t, b)+"\n",
		"kb",
	)
	if err != nil {
		t.Fatalf("LoadKeyringFromInline: %v", err)
	}

	sig, kid, err := ring.Sign(testHashHex2, testDeploymentID2)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if kid != "kb" {
		t.Errorf("Sign returned kid %q, want active kid %q", kid, "kb")
	}
	// The signature was produced by b's priv; b's pub verifies it.
	ok, err := ring.Verify(testHashHex2, testDeploymentID2, sig, "kb")
	if err != nil {
		t.Fatalf("Verify(kb): %v", err)
	}
	if !ok {
		t.Error("Verify(kb) returned false on a signature kb just produced")
	}
	// The same signature MUST NOT verify under a — proves the
	// keyring picks the active key for signing (not "first key").
	ok, err = ring.Verify(testHashHex2, testDeploymentID2, sig, "ka")
	if err != nil {
		t.Fatalf("Verify(ka): %v", err)
	}
	if ok {
		t.Error("Verify(ka) returned true for a signature kb produced — kid dispatch broken")
	}
}

//  3. missing kid: an artifact signed with `k1` cannot be verified
//     by a keyring that doesn't contain k1. Returns ErrInvalidKey
//     (typed; operators see the failure mode in logs).
func TestKeyring_VerifyRejectsUnknownKid(t *testing.T) {
	ring, err := LoadKeyringFromInline(
		"ka = "+hexEncode(t, TestKey(t))+"\n",
		"ka",
	)
	if err != nil {
		t.Fatalf("LoadKeyringFromInline: %v", err)
	}

	// Pretend we received an artifact that claims kid="kb" but
	// the keyring doesn't have a kb entry. Verify must surface a
	// typed error rather than returning false (which would look
	// identical to a real cryptographic rejection).
	_, err = ring.Verify(testHashHex2, testDeploymentID2, "ignored", "kb")
	if err == nil {
		t.Fatal("expected error for unknown kid, got nil")
	}
	if !errIsInvalidKey(err) {
		t.Errorf("expected ErrInvalidKey, got %v", err)
	}
}

//  4. empty kid falls back to "default" — symmetric with the
//     worker side (`verifier::Keyring::verify` in edge-worker).
//     A legacy deployment row with an empty `signing_key_id`
//     column should still be verifiable.
func TestKeyring_VerifyEmptyKidFallsBackToDefault(t *testing.T) {
	ring, err := LoadKeyringFromInline(
		"default = "+hexEncode(t, TestKey(t))+"\n",
		"default",
	)
	if err != nil {
		t.Fatalf("LoadKeyringFromInline: %v", err)
	}

	sig, _, err := ring.Sign(testHashHex2, testDeploymentID2)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	ok, err := ring.Verify(testHashHex2, testDeploymentID2, sig, "")
	if err != nil {
		t.Fatalf("Verify with empty kid: %v", err)
	}
	if !ok {
		t.Error("Verify returned false on a signature under the default kid")
	}
}

//  5. active kid not in keyring: a misconfigured CP that sets
//     `EDGE_SIGNING_KEY_ID=k1` but the keyring file only has
//     `k2` must fail fast at startup. Catches the operator
//     footgun before the CP serves the first Deploy.
func TestKeyring_LoadKeyringFromInline_ActiveKidNotInKeyring(t *testing.T) {
	_, err := LoadKeyringFromInline(
		"k2 = "+hexEncode(t, TestKey(t))+"\n",
		"k1", // not in the keyring
	)
	if err == nil {
		t.Fatal("expected error for active kid not in keyring, got nil")
	}
	if !errIsInvalidKey(err) {
		t.Errorf("expected ErrInvalidKey, got %v", err)
	}
}

//  6. malformed keyring line: `<kid> = <not-hex>` returns an
//     error at load time, never silently dropping the entry.
func TestKeyring_LoadKeyringFromInline_RejectsMalformedLine(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"no_equals", "k1\n"},
		{"empty_kid", "= 5b6e6c4e1a8f4b9d2e7f3c1a5b6e6c4e1a8f4b9d2e7f3c1a5b6e6c4e1a8f4b9d\n"},
		{"non_hex_value", "k1 = zzzz\n"},
		{"duplicate_kid", "k1 = " + hexEncode(t, TestKey(t)) + "\nk1 = " + hexEncode(t, genKey(t)) + "\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := LoadKeyringFromInline(tc.body, ""); err == nil {
				t.Errorf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

//  7. empty keyring: no entries → error (caller must explicitly
//     configure at least one key).
func TestKeyring_LoadKeyringFromInline_RejectsEmpty(t *testing.T) {
	if _, err := LoadKeyringFromInline("# only comments\n\n# nothing useful\n", ""); err == nil {
		t.Error("expected error for empty keyring, got nil")
	}
}

//  8. default kid resolution: when `active` is "" and the
//     keyring has no "default" entry, the loader fails closed.
func TestKeyring_LoadKeyringFromInline_DefaultKidRequiredWhenUnset(t *testing.T) {
	_, err := LoadKeyringFromInline(
		"k1 = "+hexEncode(t, TestKey(t))+"\n",
		"", // active empty → defaults to DefaultKeyID
	)
	if err == nil {
		t.Error("expected error when keyring lacks default kid and active kid is unset")
	}
}

//  9. comments and blank lines are tolerated (operators paste
//     keyring files with `# k1 = ...` headings).
func TestKeyring_LoadKeyringFromInline_ToleratesComments(t *testing.T) {
	ring, err := LoadKeyringFromInline(
		"# header comment\n"+
			"\n"+
			"# k1 = old retired key (kept for historical artifacts)\n"+
			"k1 = "+hexEncode(t, TestKey(t))+"\n",
		"k1",
	)
	if err != nil {
		t.Fatalf("LoadKeyringFromInline: %v", err)
	}
	if got := ring.ActiveKeyID(); got != "k1" {
		t.Errorf("ActiveKeyID = %q, want k1", got)
	}
}

//  10. KeyringFromSigner round-trip: the legacy single-key shim
//     preserves the kid when given, inherits when empty.
func TestKeyringFromSigner_KidResolution(t *testing.T) {
	t.Run("explicit_kid_wins", func(t *testing.T) {
		s := &Signer{keyID: "from-signer"}
		r := KeyringFromSigner(s, "explicit")
		if got := r.ActiveKeyID(); got != "explicit" {
			t.Errorf("ActiveKeyID = %q, want explicit", got)
		}
	})
	t.Run("inherits_from_signer_when_empty", func(t *testing.T) {
		s := &Signer{keyID: "from-signer"}
		r := KeyringFromSigner(s, "")
		if got := r.ActiveKeyID(); got != "from-signer" {
			t.Errorf("ActiveKeyID = %q, want from-signer", got)
		}
	})
	t.Run("empty_falls_back_to_default", func(t *testing.T) {
		s := &Signer{keyID: ""}
		r := KeyringFromSigner(s, "")
		if got := r.ActiveKeyID(); got != DefaultKeyID {
			t.Errorf("ActiveKeyID = %q, want %q", got, DefaultKeyID)
		}
	})
}

//  11. sortedKids is stable. (Internal helper; no operator-facing
//     contract, but the sort must be deterministic so error
//     messages are reproducible across runs.)
func TestKeyring_KidsSorted(t *testing.T) {
	a := genKey(t)
	b := genKey(t)
	c := genKey(t)
	ring, err := LoadKeyringFromInline(
		"zb = "+hexEncode(t, a)+"\n"+
			"aa = "+hexEncode(t, b)+"\n"+
			"mn = "+hexEncode(t, c)+"\n",
		"aa",
	)
	if err != nil {
		t.Fatalf("LoadKeyringFromInline: %v", err)
	}
	got := ring.Kids()
	want := []string{"aa", "mn", "zb"}
	if len(got) != len(want) {
		t.Fatalf("Kids() = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("Kids()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// helpers

// hexEncode returns the deterministic test Signer's seed as
// 64-lowercase-hex (the on-disk keyring format). Uses TestKey's
// underlying priv so the well-known fixture round-trip stays
// meaningful.
func hexEncode(t *testing.T, s *Signer) string {
	t.Helper()
	return strings.ToLower(hex.EncodeToString(s.priv.Seed()))
}

func errIsInvalidKey(err error) bool {
	return errors.Is(err, ErrInvalidKey)
}
