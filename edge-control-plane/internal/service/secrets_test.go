package service

import (
	"strings"
	"testing"
)

func TestEncryptDecrypt_RoundTrip(t *testing.T) {
	sec, err := NewSecretEncryptor(testMasterKey)
	if err != nil {
		t.Fatalf("NewSecretEncryptor: %v", err)
	}
	if sec == nil {
		t.Fatal("expected non-nil encryptor")
	}

	plaintext := "DATABASE_URL=postgres://user:pass@host:5432/db"
	encrypted, err := sec.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	if encrypted == plaintext {
		t.Error("encrypted value must differ from plaintext")
	}
	if !strings.Contains(encrypted, ":") {
		t.Error("encrypted value must contain ':' separator")
	}

	decrypted, err := sec.Decrypt(encrypted)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if decrypted != plaintext {
		t.Errorf("Decrypt = %q, want %q", decrypted, plaintext)
	}
}

func TestEncryptDecrypt_MultipleValues(t *testing.T) {
	sec, err := NewSecretEncryptor(testMasterKey)
	if err != nil || sec == nil {
		t.Fatalf("NewSecretEncryptor: %v", err)
	}

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

func TestDecrypt_LegacyPlaintext(t *testing.T) {
	sec, err := NewSecretEncryptor(testMasterKey)
	if err != nil || sec == nil {
		t.Fatalf("NewSecretEncryptor: %v", err)
	}

	// Legacy DB value — no colon prefix
	legacy := "DATABASE_URL=postgres://user:pass@host:5432/db"
	dec, err := sec.Decrypt(legacy)
	if err != nil {
		t.Fatalf("Decrypt(legacy): %v", err)
	}
	if dec != legacy {
		t.Errorf("legacy value must pass through unchanged: got %q", dec)
	}
}

func TestDecrypt_PlaintextWithColon(t *testing.T) {
	sec, err := NewSecretEncryptor(testMasterKey)
	if err != nil || sec == nil {
		t.Fatalf("NewSecretEncryptor: %v", err)
	}

	// A plaintext that happens to contain ":" should still pass through
	// because the nonce prefix won't be valid hex.
	plaintext := "key:value:with:colons"
	dec, err := sec.Decrypt(plaintext)
	if err != nil {
		t.Fatalf("Decrypt(plaintextWithColon): %v", err)
	}
	if dec != plaintext {
		t.Errorf("expected pass-through, got %q", dec)
	}
}

func TestDecrypt_TamperedCiphertext(t *testing.T) {
	sec, err := NewSecretEncryptor(testMasterKey)
	if err != nil || sec == nil {
		t.Fatalf("NewSecretEncryptor: %v", err)
	}

	enc, err := sec.Encrypt("sensitive")
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	// Flip a byte in the ciphertext portion
	parts := strings.SplitN(enc, ":", 2)
	tampered := parts[0] + ":deadbeef" + parts[1][8:]

	_, err = sec.Decrypt(tampered)
	if err == nil {
		t.Fatal("expected error for tampered ciphertext, got nil")
	}
}

func TestDecrypt_WrongKey(t *testing.T) {
	sec1, _ := NewSecretEncryptor(testMasterKey)
	sec2, err := NewSecretEncryptor("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil || sec2 == nil {
		t.Fatalf("NewSecretEncryptor(sec2): %v", err)
	}

	enc, err := sec1.Encrypt("my-api-key")
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	_, err = sec2.Decrypt(enc)
	if err == nil {
		t.Fatal("expected error for wrong key, got nil")
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

func TestNewSecretEncryptor_NonHexKey(t *testing.T) {
	_, err := NewSecretEncryptor(strings.Repeat("z", 64))
	if err == nil {
		t.Fatal("expected error for non-hex key, got nil")
	}
}

// testMasterKey is a valid 32-byte hex key used by all tests in this file.
const testMasterKey = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
