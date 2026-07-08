// Package signing issues and verifies Ed25519 signatures over
// `(sha256(artifact) || deployment_id)` for issue #307.
//
// The control plane signs each artifact once at upload time
// (`Deploy` / `Migrate` / `MigrateTree`) and persists the signature
// on the `deployments` row. Workers verify before instantiation. A
// successful verification proves the artifact was produced (or
// stored) by a control plane in possession of the corresponding
// private key — closing the gap where an attacker who compromises
// both the artifact store AND the deployments.hash column could
// otherwise substitute a malicious artifact with a matching SHA-256.
//
// Binding the signature to `deployment_id` (not just the hash) is
// what prevents DB-replay: an attacker who can rewrite a signature
// column on a different row cannot lift a valid signature off
// deployment A and paste it onto deployment B.
package signing

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
)

// ErrInvalidHash indicates the SHA-256 hash argument was not a
// well-formed 64-char lowercase hex string. Distinguishing this from
// ErrInvalidDeploymentID / ErrInvalidSignature lets the operator see
// the exact failure mode in logs (a 422 from the API surfaces one
// of these typed errors verbatim).
var (
	ErrInvalidHash         = errors.New("invalid hash")
	ErrInvalidDeploymentID = errors.New("invalid deployment id")
	ErrInvalidKey          = errors.New("invalid signing key")
)

// Signer is the control-plane-side Ed25519 signer. One instance per
// process, constructed at startup from a private key file (see
// LoadFromFile / LoadFromEnv). The `keyID` is a logical identifier
// (operator-chosen, e.g. "k1") stamped onto each row at sign time
// so future rotation work can verify freshness without touching
// the cryptographic code.
type Signer struct {
	priv  ed25519.PrivateKey
	pub   ed25519.PublicKey
	keyID string
}

// LoadFromFile reads an Ed25519 private key from `path`. Two formats
// are accepted, picked by file size:
//
//   - 32 raw bytes (seed form) — expanded via ed25519.NewKeyFromSeed
//   - 64 raw bytes (the full private key per RFC 8032 §5.1.2) — used
//     directly via ed25519.PrivateKey(bytes)
//
// hex-encoded (64 or 128 hex chars) is also accepted for tooling
// that prefers ASCII key files. Any other size or encoding is
// rejected with ErrInvalidKey.
func LoadFromFile(path, keyID string) (*Signer, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading signing key %q: %w", path, err)
	}
	return LoadFromRaw(data, keyID)
}

// LoadFromRaw is the in-memory companion to LoadFromFile: takes
// the key bytes directly (32/64 raw or 64/128 hex) and constructs
// a Signer. Used by app.New when the key is provided inline via
// EDGE_SIGNING_KEY rather than a file path.
func LoadFromRaw(data []byte, keyID string) (*Signer, error) {
	priv, err := parsePrivateKey(data)
	if err != nil {
		return nil, fmt.Errorf("parsing signing key: %w", err)
	}
	return newSigner(priv, keyID), nil
}

// LoadFromEnv on this type was removed in issue #307 follow-up PR1.
// Use `signing.LoadFromEnv` (keyring.go) which returns a `*Keyring`
// and supports both the new `EDGE_SIGNING_KEYRING[_PATH]` format
// and the legacy `EDGE_SIGNING_KEY[_PATH]` env vars (one-key shim,
// logs a deprecation warning).

// parsePrivateKey accepts the byte, hex-32, hex-64, and raw-64 forms
// documented on LoadFromFile.
//
// Trimming: only the hex (ASCII) path gets surrounding-whitespace
// stripping. The raw-byte path MUST NOT trim because a random
// 32- or 64-byte key can legitimately contain bytes that look like
// ASCII whitespace (0x09/0x0a/0x0d/0x20); trimming those would
// corrupt the key and surface as a confusing "got N" length error.
// Detecting raw-vs-hex first, then trimming only the hex branch,
// keeps the two paths independent.
func parsePrivateKey(data []byte) (ed25519.PrivateKey, error) {
	// First, look for the hex fingerprint on the ORIGINAL input
	// (before any trimming). All-hex + even-length + length in the
	// accepted hex set (64 or 128) is unambiguous evidence of a hex
	// payload; everything else is treated as raw bytes.
	if isLikelyHex(data) && (len(data) == 64 || len(data) == 128) {
		hexStr := string(trimASCII(data))
		raw, err := hex.DecodeString(hexStr)
		if err != nil {
			return nil, fmt.Errorf("%w: hex decode: %v", ErrInvalidKey, err)
		}
		switch len(raw) {
		case 32:
			return ed25519.NewKeyFromSeed(raw), nil
		case 64:
			return ed25519.PrivateKey(raw), nil
		default:
			return nil, fmt.Errorf("%w: hex decoded to %d raw bytes, want 32 or 64", ErrInvalidKey, len(raw))
		}
	}
	switch len(data) {
	case 32:
		// Raw seed.
		return ed25519.NewKeyFromSeed(data), nil
	case 64:
		// Raw private key (seed || public, RFC 8032 §5.1.2).
		return ed25519.PrivateKey(data), nil
	default:
		return nil, fmt.Errorf("%w: expected 32 or 64 raw bytes (or 64/128 hex), got %d", ErrInvalidKey, len(data))
	}
}

// isLikelyHex returns true if every byte is an ASCII hex digit. Used
// to disambiguate 64 raw bytes from 64-byte hex-encoded key material;
// we can't tell them apart by length alone.
func isLikelyHex(b []byte) bool {
	if len(b) == 0 || len(b)%2 != 0 {
		return false
	}
	for _, c := range b {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		case c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}

// trimASCII strips surrounding whitespace and newlines that
// shell-pasted key material often picks up. It's a no-op for raw
// binary input.
func trimASCII(b []byte) []byte {
	start, end := 0, len(b)
	for start < end && isSpace(b[start]) {
		start++
	}
	for end > start && isSpace(b[end-1]) {
		end--
	}
	return b[start:end]
}

func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

func newSigner(priv ed25519.PrivateKey, keyID string) *Signer {
	return &Signer{
		priv:  priv,
		pub:   priv.Public().(ed25519.PublicKey),
		keyID: keyID,
	}
}

// PublicKey returns the raw 32-byte Ed25519 public key. Operators
// emit this to a hex string (PublicKeyHex) and pass it to the
// workers as EDGE_SIGNING_PUBKEY.
func (s *Signer) PublicKey() ed25519.PublicKey { return s.pub }

// PublicKeyHex returns the public key as 64 lowercase hex chars.
func (s *Signer) PublicKeyHex() string {
	return hex.EncodeToString(s.pub)
}

// KeyID returns the logical key identifier stamped onto each
// deployment row at sign time. Empty if the CP was started without
// EDGE_SIGNING_KEY_ID.
func (s *Signer) KeyID() string { return s.keyID }

// Sign returns the base64url(no-pad) Ed25519 signature over
// `sha256(artifact_bytes) || deployment_id`.
//
// `hashHex` MUST be exactly 64 lowercase hex chars (the shape
// `SaveAndHash` returns via hex.EncodeToString). `deploymentID` is
// the canonical UUID-shaped deployment_id string ("d_<uuid>"). Both
// are pre-validated; the caller is expected to pass a real
// deployment_id (not user input that could carry newlines or
// control bytes — the verification input is bytes(deploymentID) so
// any byte sequence is technically valid, but we reject empty for
// sanity).
func (s *Signer) Sign(hashHex, deploymentID string) (string, error) {
	hashBytes, err := decodeHashHex(hashHex)
	if err != nil {
		return "", err
	}
	if deploymentID == "" {
		return "", fmt.Errorf("%w: empty", ErrInvalidDeploymentID)
	}

	// Build the signed message: hash bytes followed by the raw
	// deployment_id bytes. No separator / length prefix because the
	// hash is fixed-width (32 bytes) and the verifier knows the
	// layout.
	msg := make([]byte, 0, len(hashBytes)+len(deploymentID))
	msg = append(msg, hashBytes...)
	msg = append(msg, []byte(deploymentID)...)

	sig := ed25519.Sign(s.priv, msg)
	return base64.RawURLEncoding.EncodeToString(sig), nil
}

// Verify is a helper for tests and operator tooling (e.g. an
// out-of-band check of a signature column). Production verification
// happens in the worker; this method exists for the integration
// tests in `internal/signing/signer_test.go` and the deployment
// service test that asserts `dep.Signature` matches
// `verify(pub, hash, id, dep.Signature) == true`.
func (s *Signer) Verify(hashHex, deploymentID, signatureB64 string) (bool, error) {
	hashBytes, err := decodeHashHex(hashHex)
	if err != nil {
		return false, err
	}
	if deploymentID == "" {
		return false, fmt.Errorf("%w: empty", ErrInvalidDeploymentID)
	}
	sig, err := base64.RawURLEncoding.DecodeString(signatureB64)
	if err != nil {
		return false, fmt.Errorf("base64url decode signature: %w", err)
	}
	msg := make([]byte, 0, len(hashBytes)+len(deploymentID))
	msg = append(msg, hashBytes...)
	msg = append(msg, []byte(deploymentID)...)
	return ed25519.Verify(s.pub, msg, sig), nil
}

// decodeHashHex enforces the strict shape SaveAndHash produces:
// exactly 64 lowercase hex characters. The worker's verify_hash
// enforces the same shape; mirroring it here means a hash that
// verifies on the worker is the same hash that signs cleanly here.
func decodeHashHex(s string) ([]byte, error) {
	if len(s) != 64 {
		return nil, fmt.Errorf("%w: expected 64 chars, got %d", ErrInvalidHash, len(s))
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return nil, fmt.Errorf("%w: non-lowercase-hex char at position %d", ErrInvalidHash, i)
		}
	}
	return hex.DecodeString(s)
}

// HashBytes is a tiny convenience used by the deployment service
// after `SaveAndHash` returns the raw SHA-256 bytes — it wraps
// `hex.EncodeToString` so the call site doesn't need to import
// `encoding/hex` just to format the hash for `Sign`.
func HashBytes(rawHash []byte) string {
	return hex.EncodeToString(rawHash)
}
