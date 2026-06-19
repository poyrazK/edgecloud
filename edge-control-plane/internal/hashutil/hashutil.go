// Package hashutil provides shared hash helpers used across packages.
// In particular, SHA256Hex is the canonical lookup key for API key auth
// (see internal/middleware/auth.go and internal/service/api_key.go) and
// must produce identical bytes for the same input in every caller.
package hashutil

import (
	"crypto/sha256"
	"encoding/hex"
)

// SHA256Hex returns the lowercase hex-encoded SHA-256 of the input.
// Stable across processes — the canonical form AuthMiddleware and the
// API key service both query against.
func SHA256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}
