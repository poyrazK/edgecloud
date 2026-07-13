package service

import (
	"crypto/sha256"
)

// sha256Sum is a thin convenience wrapper around crypto/sha256.
// Kept package-private because the only call site is the
// `migration_deny.go` pre-pass (issue #622 commit 2). Centralizing
// the hex encoding here avoids copying the
// fmt.Sprintf("%x", ...) idiom across future helpers.
func sha256Sum(b []byte) [32]byte {
	return sha256.Sum256(b)
}
