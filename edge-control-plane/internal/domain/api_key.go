package domain

import (
	"time"
)

// APIKey represents a developer's API key for authentication.
type APIKey struct {
	ID        string     `db:"id"`
	TenantID  string     `db:"tenant_id"`
	Name      string     `db:"name"`
	KeyHash   string     `db:"key_hash"`  // SHA-256 of raw key
	Role      string     `db:"role"`      // owner, developer, viewer
	CreatedAt time.Time  `db:"created_at"`
	LastUsed  *time.Time `db:"last_used"`  // nil means never used
	ExpiresAt *time.Time `db:"expires_at"` // nil means never expires
}

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
