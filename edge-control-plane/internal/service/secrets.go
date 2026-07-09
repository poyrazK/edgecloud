package service

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// SecretEncryptor encrypts and decrypts secret values using AES-256-GCM
// with a keyring that supports zero-downtime key rotation.
//
// The active key (ActiveKeyID) encrypts new values; all keys in the
// keyring are attempted for decryption.
//
// Ciphertext formats:
//   - New:  <key_id>:<hex-nonce>:<hex-ciphertext+tag>
//   - Old:  <hex-nonce>:<hex-ciphertext+tag>  (no kid; tried against every key)
//
// Decrypt never returns plaintext. Stored values that don't match the
// ciphertext shape return ErrPlaintextEnvNotAllowed — issue #441 closes
// the silent-passthrough path that previously let operators (or
// attackers with DB write) seed plaintext env rows that decrypted
// cleanly. Cipher rows whose kid IS in the keyring but whose GCM tag
// fails (tamper, wrong-key, corruption) return ErrCiphertextMismatch so
// operators can distinguish "run re-encrypt" from "page a human".
//
// A nil SecretEncryptor is a no-op: Encrypt/Decrypt return the value
// unchanged. This lets development setups run without encryption keys.

// ErrPlaintextEnvNotAllowed is returned by Decrypt when the stored
// value is not in any recognized ciphertext shape. Issue #441: the CP
// must refuse to publish plaintext env values; run
// POST /api/v1/admin/secrets/re-encrypt to migrate legacy rows, or
// set EDGE_ALLOW_LEGACY_PLAINTEXT_ENV=true at startup as a temporary
// migration escape (warning logged, not enforced).
var ErrPlaintextEnvNotAllowed = errors.New("plaintext env values are not allowed; re-encrypt via POST /api/v1/admin/secrets/re-encrypt or set EDGE_ALLOW_LEGACY_PLAINTEXT_ENV=true")

// ErrCiphertextMismatch is returned by Decrypt when the value has a
// recognized kid prefix but AES-GCM authentication fails. This signals
// either row tampering or that the keyring lost a key that the row
// was encrypted with. Re-encrypt will not help; investigate.
var ErrCiphertextMismatch = errors.New("env ciphertext failed authentication: keyring mismatch or row tampering")
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
// return ErrPlaintextEnvNotAllowed (issue #441). Cipher rows whose kid is
// in the keyring but whose GCM tag fails return ErrCiphertextMismatch.
//
// A nil SecretEncryptor returns value unchanged (dev mode).
func (sec *SecretEncryptor) Decrypt(value string) (string, error) {
	if sec == nil || len(sec.keyring) == 0 {
		return value, nil
	}

	parts := strings.SplitN(value, ":", 3)

	switch len(parts) {
	case 3:
		// New format: kid:nonce:ct.
		keyID := parts[0]
		if key, ok := sec.keyring[keyID]; ok {
			result, err := sec.decryptWithKey(key, parts[1], parts[2])
			if err == nil {
				return result, nil
			}
			// Kid IS in the keyring but GCM auth failed: row tampering,
			// wrong-key, or corruption. NOT plaintext — return the
			// distinct sentinel so operators can tell this apart from
			// "just re-encrypt me".
			return value, fmt.Errorf("%w: kid=%q", ErrCiphertextMismatch, keyID)
		}
		// Unknown kid: this is plaintext that happens to contain two
		// colons (a connection string, a credential, etc.). Reject
		// rather than silently pass through.
		return value, fmt.Errorf("%w: 3-part value with unknown kid %q", ErrPlaintextEnvNotAllowed, keyID)
	case 2:
		// Old format (no kid): try every key in the keyring.
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
			result, err := sec.decryptWithKey(key, nonceHex, ctHex)
			if err == nil {
				return result, nil
			}
		}
		// No key matched — either plaintext containing a colon, or a
		// row encrypted with a key that has been removed from the
		// keyring. Issue #441: in the latter case the operator needs
		// to re-add the key and re-encrypt, not be silently handed
		// the plaintext. We surface this as plaintext-rejected (it
		// is observably not a ciphertext this keyring can read) —
		// the failure mode is the same: the row cannot be decrypted.
		return value, fmt.Errorf("%w: 2-part value with no keyring match", ErrPlaintextEnvNotAllowed)
	default:
		// 0 or 1 parts: definitely plaintext.
		return value, fmt.Errorf("%w: %d-part value", ErrPlaintextEnvNotAllowed, len(parts))
	}
}

// decryptWithKey is the inner AES-256-GCM decrypt given a key, nonce hex, and ciphertext hex.
func (sec *SecretEncryptor) decryptWithKey(key []byte, nonceHex, ctHex string) (string, error) {
	nonce, err := hex.DecodeString(nonceHex)
	if err != nil || len(nonce) == 0 {
		return "", fmt.Errorf("invalid nonce hex: %w", err)
	}
	ct, err := hex.DecodeString(ctHex)
	if err != nil {
		return "", fmt.Errorf("invalid ciphertext hex: %w", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("aes: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("gcm: %w", err)
	}
	// GCM panics on incorrect nonce length. Guard it.
	if len(nonce) != gcm.NonceSize() {
		return "", fmt.Errorf("invalid nonce length: got %d, want %d", len(nonce), gcm.NonceSize())
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
