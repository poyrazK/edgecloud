package domain

import (
	"time"
)

// APIKey represents a developer's API key for authentication.
type APIKey struct {
	ID       string `db:"id"`
	TenantID string `db:"tenant_id"`
	Name     string `db:"name"`
	// KeyHash is the algorithm-specific encoded hash. Its format depends on
	// HashAlgorithm:
	//   "sha256"   — hex SHA-256 of the raw key (legacy; pre-migration 005).
	//   "argon2id" — PHC-formatted string (golang.org/x/crypto/argon2).
	KeyHash   string     `db:"key_hash"`
	Role      string     `db:"role"` // owner, developer, viewer
	CreatedAt time.Time  `db:"created_at"`
	LastUsed  *time.Time `db:"last_used"`  // nil means never used
	ExpiresAt *time.Time `db:"expires_at"` // nil means never expires
	// HashAlgorithm is the algorithm used to produce KeyHash. Possible values:
	//   "sha256"    — legacy hex SHA-256; only present for keys created before
	//                 migration 005. AuthMiddleware transparently verifies these
	//                 and lazily upgrades them to "argon2id" on next use.
	//   "argon2id"  — PHC-formatted string (golang.org/x/crypto/argon2).
	HashAlgorithm string `db:"hash_algorithm"`
	// LookupHash is the SHA-256 hex of the raw API key. It is the stable
	// lookup key for AuthenticateRawKey, independent of the algorithm-
	// specific KeyHash. Migration 006 added this column; new rows are
	// populated on insert, SHA-256 legacy rows are backfilled from key_hash.
	LookupHash string `db:"lookup_hash"`
}

// HashAlgorithm values stored in DB.
const (
	HashAlgorithmSHA256   = "sha256"
	HashAlgorithmArgon2ID = "argon2id"
)

// Role constants.
const (
	RoleOwner     = "owner"
	RoleDeveloper = "developer"
	RoleViewer    = "viewer"
)

// IsValidRole checks if a role string is valid.
func IsValidRole(r string) bool {
	return r == RoleOwner || r == RoleDeveloper || r == RoleViewer
}
