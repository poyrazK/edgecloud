package service

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

// SecretEncryptor encrypts and decrypts secret values using AES-256-GCM.
// A nil or zero-value SecretEncryptor is a no-op: Encrypt/Decrypt return
// the value unchanged. This lets development setups run without a master key
// while production enforces one.
type SecretEncryptor struct {
	key []byte // 32 bytes for AES-256
}

// NewSecretEncryptor creates an encryptor from a hex-encoded 32-byte key.
// Returns nil (no-op) when keyHex is empty.
func NewSecretEncryptor(keyHex string) (*SecretEncryptor, error) {
	if keyHex == "" {
		return nil, nil
	}
	key, err := hex.DecodeString(keyHex)
	if err != nil {
		return nil, fmt.Errorf("secrets key must be hex: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("secrets key must be 32 bytes (64 hex chars), got %d bytes", len(key))
	}
	return &SecretEncryptor{key: key}, nil
}

// Encrypt encrypts plaintext with AES-256-GCM and returns it as
// "<hex-nonce>:<hex-ciphertext+tag>". Returns plaintext unchanged when sec
// is nil or empty (encryption disabled — development mode).
func (sec *SecretEncryptor) Encrypt(plaintext string) (string, error) {
	if sec == nil || len(sec.key) == 0 {
		return plaintext, nil
	}
	block, err := aes.NewCipher(sec.key)
	if err != nil {
		return "", fmt.Errorf("aes: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("gcm: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("nonce: %w", err)
	}
	ct := gcm.Seal(nil, nonce, []byte(plaintext), nil)
	return hex.EncodeToString(nonce) + ":" + hex.EncodeToString(ct), nil
}

// Decrypt reverses Encrypt. Values without the "<hex>:" prefix (legacy
// plaintext) are returned as-is. An empty or nil sec returns value unchanged.
func (sec *SecretEncryptor) Decrypt(value string) (string, error) {
	if sec == nil || len(sec.key) == 0 {
		return value, nil
	}
	// Legacy plaintext: no ":" separator, or nonce part is not valid hex.
	parts := strings.SplitN(value, ":", 2)
	if len(parts) != 2 {
		return value, nil
	}
	nonce, err := hex.DecodeString(parts[0])
	if err != nil || len(nonce) == 0 {
		return value, nil
	}
	ct, err := hex.DecodeString(parts[1])
	if err != nil {
		return value, nil
	}

	block, err := aes.NewCipher(sec.key)
	if err != nil {
		return "", fmt.Errorf("aes: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("gcm: %w", err)
	}
	plaintext, err := gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return "", fmt.Errorf("env value tampered or wrong key: %w", err)
	}
	return string(plaintext), nil
}


