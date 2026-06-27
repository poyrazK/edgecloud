package handler

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// hmac256 is a test-only helper that matches the Rust
// `bootstrap::sign_with_psk` format: HMAC-SHA256(psk, "{worker_id}:{region}").
// Kept in a separate file so production code (bootstrap.go) doesn't
// accidentally import crypto primitives it doesn't need.
func hmac256(psk, message string) string {
	mac := hmac.New(sha256.New, []byte(psk))
	mac.Write([]byte(message))
	return hex.EncodeToString(mac.Sum(nil))
}
