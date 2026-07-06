package service

import (
	"strings"
	"testing"
)

// testMasterKey is a valid 32-byte hex key used by all tests in this file.
const testMasterKey = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"

// testKey2 is a different 32-byte hex key for rotation tests.
const testKey2 = "ffffffffffffffffffffffffffffffffeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

func TestNewEncryptFromConfig_WithKeys(t *testing.T) {
	sec, err := NewSecretEncryptorFromConfig("key1", map[string]string{
		"key1": testMasterKey,
	})
	if err != nil {
		t.Fatalf("NewSecretEncryptorFromConfig: %v", err)
	}
	if sec == nil {
		t.Fatal("expected non-nil encryptor")
	}
	if sec.ActiveKeyID() != "key1" {
		t.Errorf("ActiveKeyID = %q, want %q", sec.ActiveKeyID(), "key1")
	}
}

func TestNewEncryptFromConfig_EmptyKeysReturnsNil(t *testing.T) {
	sec, err := NewSecretEncryptorFromConfig("", nil)
	if err != nil {
		t.Fatalf("NewSecretEncryptorFromConfig: %v", err)
	}
	if sec != nil {
		t.Error("expected nil for empty keys")
	}

	sec, err = NewSecretEncryptorFromConfig("", map[string]string{})
	if err != nil {
		t.Fatalf("NewSecretEncryptorFromConfig: %v", err)
	}
	if sec != nil {
		t.Error("expected nil for empty key map")
	}
}

func TestNewEncryptFromConfig_UnknownActiveKey(t *testing.T) {
	_, err := NewSecretEncryptorFromConfig("nope", map[string]string{
		"key1": testMasterKey,
	})
	if err == nil {
		t.Fatal("expected error for unknown active_key_id")
	}
}

func TestNewEncryptFromConfig_InvalidHexKey(t *testing.T) {
	_, err := NewSecretEncryptorFromConfig("key1", map[string]string{
		"key1": "not-hex",
	})
	if err == nil {
		t.Fatal("expected error for non-hex key")
	}
}

func TestRoundTrip_NewFormat(t *testing.T) {
	sec, _ := NewSecretEncryptorFromConfig("key1", map[string]string{
		"key1": testMasterKey,
	})

	plaintext := "DATABASE_URL=postgres://user:pass@host:5432/db"
	enc, err := sec.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	// Must have three colon-separated parts.
	parts := strings.SplitN(enc, ":", 3)
	if len(parts) != 3 {
		t.Fatalf("expected 3-part format (key_id:nonce:ct+tag), got %q", enc)
	}
	if parts[0] != "key1" {
		t.Errorf("key_id = %q, want %q", parts[0], "key1")
	}

	dec, err := sec.Decrypt(enc)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if dec != plaintext {
		t.Errorf("Decrypt = %q, want %q", dec, plaintext)
	}
}

func TestRoundTrip_LegacyFormat(t *testing.T) {
	// Encrypt with legacy constructor, decrypt with keyring constructor.
	legacySec, _ := NewSecretEncryptorFromLegacy(testMasterKey)
	keyringSec, _ := NewSecretEncryptorFromConfig("key1", map[string]string{
		"key1": testMasterKey,
	})

	plaintext := "STRIPE_KEY=sk_live_abc123"
	encOld, err := legacySec.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Legacy Encrypt: %v", err)
	}

	// Strip the "legacy:" prefix that NewSecretEncryptorFromLegacy now adds
	// to simulate a true old-format 2-part ciphertext from before the migration.
	encOld = strings.TrimPrefix(encOld, "legacy:")
	dec, err := keyringSec.Decrypt(encOld)
	if err != nil {
		t.Fatalf("Keyring Decrypt (old format): %v", err)
	}
	if dec != plaintext {
		t.Errorf("Decrypt = %q, want %q", dec, plaintext)
	}
}

func TestRotation_NewKeyDecryptsOldValues(t *testing.T) {
	// Encrypt with key1, then switch to key2 (both in keyring).
	keyring := map[string]string{
		"key1": testMasterKey,
		"key2": testKey2,
	}

	sec1, _ := NewSecretEncryptorFromConfig("key1", keyring)
	plaintext := "sensitive-data"

	enc1, err := sec1.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt with key1: %v", err)
	}

	// Now "rotate": activate key2 but keep key1 in keyring.
	sec2, _ := NewSecretEncryptorFromConfig("key2", keyring)

	// New encrypts use key2.
	enc2, _ := sec2.Encrypt(plaintext)
	if !strings.HasPrefix(enc2, "key2:") {
		t.Errorf("new encrypt should use key2 prefix, got %q", enc2)
	}

	// Old value encrypted with key1 must still decrypt.
	dec, err := sec2.Decrypt(enc1)
	if err != nil {
		t.Fatalf("Decrypt old value with new keyring: %v", err)
	}
	if dec != plaintext {
		t.Errorf("Decrypt = %q, want %q", dec, plaintext)
	}
}

func TestDecrypt_UnknownKeyID_ReturnsPlaintext(t *testing.T) {
	sec, _ := NewSecretEncryptorFromConfig("key1", map[string]string{
		"key1": testMasterKey,
	})

	// Value that looks like new format but with unknown key_id.
	enc := "unknown_key:0102030405060708090a0b0c0d0e0f10:aabbccdd"

	dec, err := sec.Decrypt(enc)
	if err != nil {
		t.Fatalf("Decrypt with unknown key_id: %v", err)
	}
	if dec != enc {
		t.Errorf("expected pass-through for unknown key_id, got %q", dec)
	}
}

func TestDecrypt_OldFormatWithRemovedKey(t *testing.T) {
	// Encrypt with key1, then remove key1 from keyring.
	sec1, _ := NewSecretEncryptorFromConfig("key1", map[string]string{
		"key1": testMasterKey,
	})
	plaintext := "sensitive"
	enc, err := sec1.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	// New encryptor without key1.
	sec2, _ := NewSecretEncryptorFromConfig("key2", map[string]string{
		"key2": testKey2,
	})

	// Should return as-is (legacy plaintext passthrough).
	dec, err := sec2.Decrypt(enc)
	if err != nil {
		t.Fatalf("Decrypt after key removal: %v", err)
	}
	if dec != enc {
		t.Errorf("expected pass-through for value encrypted with removed key, got %q", dec)
	}
}

func TestDecrypt_LegacyPlaintext(t *testing.T) {
	sec, _ := NewSecretEncryptorFromConfig("key1", map[string]string{
		"key1": testMasterKey,
	})

	legacy := "DATABASE_URL=postgres://user:pass@host:5432/db"
	dec, err := sec.Decrypt(legacy)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if dec != legacy {
		t.Errorf("expected pass-through, got %q", dec)
	}
}

func TestDecrypt_PlaintextWithColon(t *testing.T) {
	sec, _ := NewSecretEncryptorFromConfig("key1", map[string]string{
		"key1": testMasterKey,
	})

	// Plaintext with colon must pass through.
	plaintext := "key:value:with:colons"
	dec, err := sec.Decrypt(plaintext)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if dec != plaintext {
		t.Errorf("expected pass-through, got %q", dec)
	}
}

func TestDecrypt_TamperedCiphertext(t *testing.T) {
	sec, _ := NewSecretEncryptorFromConfig("key1", map[string]string{
		"key1": testMasterKey,
	})

	enc, err := sec.Encrypt("sensitive")
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	// Flip a byte in the ciphertext portion.
	parts := strings.SplitN(enc, ":", 3)
	tampered := parts[0] + ":" + parts[1] + ":deadbeef" + parts[2][8:]

	dec, err := sec.Decrypt(tampered)
	if err != nil {
		t.Fatalf("Decrypt(tampered) should passthrough: %v", err)
	}
	if dec != tampered {
		t.Errorf("expected pass-through for tampered value, got %q", dec)
	}
}

func TestDecrypt_WrongKey(t *testing.T) {
	sec1, _ := NewSecretEncryptorFromConfig("key1", map[string]string{
		"key1": testMasterKey,
	})
	sec2, _ := NewSecretEncryptorFromConfig("key2", map[string]string{
		"key2": testKey2,
	})

	enc, err := sec1.Encrypt("my-api-key")
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	dec, err := sec2.Decrypt(enc)
	if err != nil {
		t.Fatalf("Decrypt with wrong key should passthrough: %v", err)
	}
	if dec != enc {
		t.Errorf("expected pass-through, got %q", dec)
	}
}

func TestEncrypt_NilEncryptor_Noop(t *testing.T) {
	var sec *SecretEncryptor
	out, err := sec.Encrypt("plaintext")
	if err != nil {
		t.Fatalf("Encrypt(nil): %v", err)
	}
	if out != "plaintext" {
		t.Errorf("nil encryptor must pass through, got %q", out)
	}
}

func TestDecrypt_NilEncryptor_Noop(t *testing.T) {
	var sec *SecretEncryptor
	out, err := sec.Decrypt("ciphertext")
	if err != nil {
		t.Fatalf("Decrypt(nil): %v", err)
	}
	if out != "ciphertext" {
		t.Errorf("nil encryptor must pass through, got %q", out)
	}
}

func TestNewSecretEncryptor_EmptyKey(t *testing.T) {
	sec, err := NewSecretEncryptor("")
	if err != nil {
		t.Fatalf("NewSecretEncryptor(''): %v", err)
	}
	if sec != nil {
		t.Error("empty key should return nil encryptor")
	}
}

func TestNewSecretEncryptor_ShortKey(t *testing.T) {
	_, err := NewSecretEncryptor("abcd1234")
	if err == nil {
		t.Fatal("expected error for short key, got nil")
	}
}

func TestKeyIDs_ReturnsSorted(t *testing.T) {
	sec, _ := NewSecretEncryptorFromConfig("b_key", map[string]string{
		"b_key": testKey2,
		"a_key": testMasterKey,
	})

	ids := sec.KeyIDs()
	if len(ids) != 2 {
		t.Fatalf("KeyIDs len = %d, want 2", len(ids))
	}
	if ids[0] != "a_key" || ids[1] != "b_key" {
		t.Errorf("KeyIDs = %v, want [a_key b_key] (sorted)", ids)
	}
}

func TestKeyIDs_NilEncryptor(t *testing.T) {
	var sec *SecretEncryptor
	if ids := sec.KeyIDs(); ids != nil {
		t.Errorf("nil encryptor KeyIDs = %v, want nil", ids)
	}
}

func TestActiveKeyID_NilEncryptor(t *testing.T) {
	var sec *SecretEncryptor
	if id := sec.ActiveKeyID(); id != "" {
		t.Errorf("nil encryptor ActiveKeyID = %q, want \"\"", id)
	}
}

func TestEncryptDecrypt_MultipleValues(t *testing.T) {
	sec, _ := NewSecretEncryptorFromConfig("key1", map[string]string{
		"key1": testMasterKey,
	})

	values := []string{
		"a",
		"hello world",
		"STRIPE_KEY=sk_live_abcdef123456",
		`{"nested": {"json": true}}`,
		"",
	}
	for _, v := range values {
		enc, err := sec.Encrypt(v)
		if err != nil {
			t.Fatalf("Encrypt(%q): %v", v, err)
		}
		dec, err := sec.Decrypt(enc)
		if err != nil {
			t.Fatalf("Decrypt(%q): %v", enc, err)
		}
		if dec != v {
			t.Errorf("round-trip mismatch: got %q, want %q", dec, v)
		}
	}
}

func TestLegacyConstructor_BackwardCompat(t *testing.T) {
	// Old-format encrypt (no key_id).
	legacy, _ := NewSecretEncryptor(testMasterKey)
	plaintext := "legacy-value"

	encOld, err := legacy.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Legacy Encrypt: %v", err)
	}

	// Should start with "legacy:" (the auto-assigned key ID).
	if !strings.HasPrefix(encOld, "legacy:") {
		t.Errorf("legacy Encrypt should produce new format with 'legacy' key_id, got %q", encOld)
	}

	// Decrypt with same legacy encryptor.
	dec, err := legacy.Decrypt(encOld)
	if err != nil {
		t.Fatalf("Legacy Decrypt: %v", err)
	}
	if dec != plaintext {
		t.Errorf("Decrypt = %q, want %q", dec, plaintext)
	}
}

func TestDecrypt_OldFormatWithoutKeyID(t *testing.T) {
	// Simulate an old-format value that's already stored in DB
	// (nonce:ct+tag, no key_id prefix). Encrypt with legacy,
	// manually strip the "legacy:" prefix.
	legacy, _ := NewSecretEncryptor(testMasterKey)
	plaintext := "old-format-value"

	encWithPrefix, _ := legacy.Encrypt(plaintext)
	// Strip "legacy:" prefix to simulate old format.
	encOld := encWithPrefix[strings.Index(encWithPrefix, ":")+1:]

	if strings.Contains(encOld, ":") != true {
		t.Fatalf("old format should have at least one colon: %q", encOld)
	}

	// Decrypt with new keyring encryptor.
	sec, _ := NewSecretEncryptorFromConfig("key1", map[string]string{
		"key1": testMasterKey,
	})

	dec, err := sec.Decrypt(encOld)
	if err != nil {
		t.Fatalf("Decrypt old format: %v", err)
	}
	if dec != plaintext {
		t.Errorf("Decrypt = %q, want %q", dec, plaintext)
	}
}
