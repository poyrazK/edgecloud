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

// UpdateAPIKeyRequest is sent when updating an existing API key.
// nil pointer fields mean "don't change"; non-nil means "set to this value".
type UpdateAPIKeyRequest struct {
	Name *string `json:"name"` // nil = no change
	Role *string `json:"role"` // nil = no change
}

// SafeAPIKeyResponse is the tenant-facing API key representation.
// It deliberately omits KeyHash, LookupHash, and HashAlgorithm.
type SafeAPIKeyResponse struct {
	ID        string  `json:"id"`
	Name      string  `json:"name"`
	Role      string  `json:"role"`
	CreatedAt string  `json:"created_at"`
	LastUsed  *string `json:"last_used,omitempty"`
	ExpiresAt *string `json:"expires_at,omitempty"`
}

// ToSafeResponse converts a domain.APIKey to a SafeAPIKeyResponse.
func (k *APIKey) ToSafeResponse() SafeAPIKeyResponse {
	r := SafeAPIKeyResponse{
		ID:        k.ID,
		Name:      k.Name,
		Role:      k.Role,
		CreatedAt: k.CreatedAt.Format("2006-01-02T15:04:05Z"),
	}
	if k.LastUsed != nil {
		s := k.LastUsed.Format("2006-01-02T15:04:05Z")
		r.LastUsed = &s
	}
	if k.ExpiresAt != nil {
		s := k.ExpiresAt.Format("2006-01-02T15:04:05Z")
		r.ExpiresAt = &s
	}
	return r
}
