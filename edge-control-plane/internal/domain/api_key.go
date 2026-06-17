package domain

import (
	"time"
)

// APIKey represents a developer's API key for authentication.
type APIKey struct {
	ID        string     `db:"id"`
	TenantID  string     `db:"tenant_id"`
	Name      string     `db:"name"`
	KeyHash   string     `db:"key_hash"`  // hex SHA-256 OR PHC-formatted argon2id string
	Role      string     `db:"role"`      // owner, developer, viewer
	CreatedAt time.Time  `db:"created_at"`
	LastUsed  *time.Time `db:"last_used"`  // nil means never used
	ExpiresAt *time.Time `db:"expires_at"` // nil means never expires
	// HashAlgorithm is the algorithm used to produce KeyHash. Possible values:
	//   "sha256"    — legacy hex SHA-256; only present for keys created before
	//                 migration 005. AuthMiddleware transparently verifies these
	//                 and lazily upgrades them to "argon2id" on next use.
	//   "argon2id"  — PHC-formatted string (golang.org/x/crypto/argon2).
	HashAlgorithm string `db:"hash_algorithm"`
}

// HashAlgorithm values stored in DB.
const (
	HashAlgorithmSHA256   = "sha256"
	HashAlgorithmArgon2ID = "argon2id"
)

// Role constants.
const (
	RoleOwner    = "owner"
	RoleDeveloper = "developer"
	RoleViewer   = "viewer"
)

// IsValidRole checks if a role string is valid.
func IsValidRole(r string) bool {
	return r == RoleOwner || r == RoleDeveloper || r == RoleViewer
}
