package service

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// SecretEncryptor encrypts and decrypts secret values using AES-256-GCM
// with a keyring that supports zero-downtime key rotation.
//
// The active key (ActiveKeyID) encrypts new values; all keys in the
// keyring are attempted for decryption. Old-format values (without a
// key_id prefix) are handled via backward-compatible fallback.
//
// Ciphertext format:
//   - New:  <key_id>:<hex-nonce>:<hex-ciphertext+tag>
//   - Old:  <hex-nonce>:<hex-ciphertext+tag>
//   - Legacy plaintext: any value without two colons
//
// A nil SecretEncryptor is a no-op: Encrypt/Decrypt return the value
// unchanged. This lets development setups run without encryption keys.
type SecretEncryptor struct {
	keyring     map[string][]byte // all known keys for decryption
	activeKeyID string            // which key encrypts new values
	activeKey   []byte            // cached reference for Encrypt fast path
}

// NewSecretEncryptorFromConfig creates an encryptor from SecretsConfig.
// Returns nil (no-op) when config has no keys.
func NewSecretEncryptorFromConfig(activeKeyID string, keys map[string]string) (*SecretEncryptor, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	keyring := make(map[string][]byte, len(keys))
	for id, hexKey := range keys {
		key, err := hex.DecodeString(hexKey)
		if err != nil {
			return nil, fmt.Errorf("secrets.keys[%q] must be hex: %w", id, err)
		}
		if len(key) != 32 {
			return nil, fmt.Errorf("secrets.keys[%q] must be 32 bytes (64 hex chars), got %d bytes", id, len(key))
		}
		keyring[id] = key
	}
	activeKey, ok := keyring[activeKeyID]
	if !ok {
		return nil, fmt.Errorf("active_key_id %q not found in keyring", activeKeyID)
	}
	return &SecretEncryptor{
		keyring:     keyring,
		activeKeyID: activeKeyID,
		activeKey:   activeKey,
	}, nil
}

// NewSecretEncryptorFromLegacy creates an encryptor from a single hex key,
// assigning it key ID "legacy". This path is used when the old
// secrets_master_key field is set.
func NewSecretEncryptorFromLegacy(keyHex string) (*SecretEncryptor, error) {
	key, err := hex.DecodeString(keyHex)
	if err != nil {
		return nil, fmt.Errorf("secrets key must be hex: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("secrets key must be 32 bytes (64 hex chars), got %d bytes", len(key))
	}
	return &SecretEncryptor{
		keyring:     map[string][]byte{"legacy": key},
		activeKeyID: "legacy",
		activeKey:   key,
	}, nil
}

// NewSecretEncryptor creates an encryptor from a hex-encoded 32-byte key.
// Returns nil (no-op) when keyHex is empty.
//
// Deprecated: use NewSecretEncryptorFromConfig or NewSecretEncryptorFromLegacy.
func NewSecretEncryptor(keyHex string) (*SecretEncryptor, error) {
	if keyHex == "" {
		return nil, nil
	}
	return NewSecretEncryptorFromLegacy(keyHex)
}

// Encrypt encrypts plaintext with AES-256-GCM using the active key and
// returns it as "<activeKeyID>:<hex-nonce>:<hex-ciphertext+tag>".
// Returns plaintext unchanged when sec is nil (development mode).
func (sec *SecretEncryptor) Encrypt(plaintext string) (string, error) {
	if sec == nil || len(sec.activeKey) == 0 {
		return plaintext, nil
	}
	block, err := aes.NewCipher(sec.activeKey)
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
	return sec.activeKeyID + ":" + hex.EncodeToString(nonce) + ":" + hex.EncodeToString(ct), nil
}

// Decrypt reverses Encrypt. Supports both old format (<nonce>:<ct>) and
// new format (<key_id>:<nonce>:<ct>). Values that don't match either format
// (legacy plaintext) are returned as-is.
//
// Backward compatibility:
//   - New-format values (3 parts) are decrypted with the key identified by key_id.
//     If the key_id is unknown, the value is treated as legacy plaintext (passthrough).
//   - Old-format values (2 parts) are tried against every key in the keyring.
//     If none succeeds, the value is treated as legacy plaintext (passthrough).
//   - Legacy plaintext (0-1 parts, or unrecognized) is returned unchanged.
//
// A nil SecretEncryptor returns value unchanged.
func (sec *SecretEncryptor) Decrypt(value string) (string, error) {
	if sec == nil || len(sec.keyring) == 0 {
		return value, nil
	}

	parts := strings.SplitN(value, ":", 3)

	switch len(parts) {
	case 3:
		// New format or plaintext-with-two-colons.
		keyID := parts[0]
		if key, ok := sec.keyring[keyID]; ok {
			result, err := sec.decryptWithKey(key, parts[1], parts[2])
			if err == nil {
				return result, nil
			}
			// Invalid ciphertext for this key. Could be plaintext that happens
			// to have two colons. Fall through to passthrough.
		}
		// Unknown key_id: the value might be plaintext containing colons,
		// or was encrypted with a now-removed key. Return as-is.
		return value, nil
	case 2:
		// Old format or legacy plaintext with colon(s).
		// Try each key in the keyring. Order doesn't matter for correctness,
		// but try the active key first for speed.
		ordered := make([][]byte, 0, len(sec.keyring))
		if sec.activeKey != nil {
			ordered = append(ordered, sec.activeKey)
		}
		for _, key := range sec.keyring {
			if len(key) > 0 && (sec.activeKey == nil || &key[0] != &sec.activeKey[0]) {
				ordered = append(ordered, key)
			}
		}
		for _, key := range ordered {
			nonceHex, ctHex := parts[0], parts[1]
			if len(parts) == 3 {
				nonceHex, ctHex = parts[1], parts[2]
			}
			result, err := sec.decryptWithKey(key, nonceHex, ctHex)
			if err == nil {
				return result, nil
			}
		}
		// Couldn't decrypt with any key — legacy plaintext passthrough.
		return value, nil
	default:
		// 0 or 1 parts: definitely legacy plaintext.
		return value, nil
	}
}

// decryptWithKey is the inner AES-256-GCM decrypt given a key, nonce hex, and ciphertext hex.
func (sec *SecretEncryptor) decryptWithKey(key []byte, nonceHex, ctHex string) (string, error) {
	nonce, err := hex.DecodeString(nonceHex)
	if err != nil || len(nonce) == 0 {
		return "", fmt.Errorf("invalid nonce")
	}
	ct, err := hex.DecodeString(ctHex)
	if err != nil {
		return "", fmt.Errorf("invalid ciphertext")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("aes: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("gcm: %w", err)
	}
	plaintext, err := gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return "", fmt.Errorf("decrypt failed: %w", err)
	}
	return string(plaintext), nil
}

// KeyIDs returns the sorted list of all key IDs in the keyring.
// Returns nil when sec is nil.
func (sec *SecretEncryptor) KeyIDs() []string {
	if sec == nil || sec.keyring == nil {
		return nil
	}
	ids := make([]string, 0, len(sec.keyring))
	for id := range sec.keyring {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// ActiveKeyID returns the ID of the key used for new encryptions.
// Returns "" when sec is nil.
func (sec *SecretEncryptor) ActiveKeyID() string {
	if sec == nil {
		return ""
	}
	return sec.activeKeyID
}
