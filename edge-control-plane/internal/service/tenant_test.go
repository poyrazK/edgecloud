package service

import (
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/hashutil"
)

// TestMintAPIKey_PopulatesAllFields pins the contract shared by
// CreateAPIKey and BootstrapTenant: every APIKey minted through this
// helper must have HashAlgorithm, LookupHash, a PHC-formatted KeyHash,
// and a TenantID/Role/Name. Without these fields, the repository's loud
// guards (migrations 005/007) reject the row, and AuthenticateRawKey
// cannot locate it.
//
// A regression here would silently reintroduce the original
// BootstrapTenant bug — a key with no lookup hash that's invisible to
// every auth request.
func TestMintAPIKey_PopulatesAllFields(t *testing.T) {
	raw, k, err := mintAPIKey("t_test", "my-key", domain.RoleDeveloper)
	if err != nil {
		t.Fatalf("mintAPIKey: %v", err)
	}
	if raw == "" {
		t.Error("rawKey is empty")
	}
	if len(raw) != 64 {
		t.Errorf("rawKey length = %d, want 64 (32 bytes hex-encoded)", len(raw))
	}
	if !isLowerHex(raw) {
		t.Errorf("rawKey %q is not lowercase hex", raw)
	}
	if !strings.HasPrefix(k.ID, "k_") {
		t.Errorf("ID = %q, want prefix 'k_'", k.ID)
	}
	if k.HashAlgorithm != domain.HashAlgorithmArgon2ID {
		t.Errorf("HashAlgorithm = %q, want %q", k.HashAlgorithm, domain.HashAlgorithmArgon2ID)
	}
	if !strings.HasPrefix(k.KeyHash, "$argon2id$") {
		t.Errorf("KeyHash = %q, want PHC prefix '$argon2id$'", k.KeyHash)
	}
	if k.LookupHash == "" {
		t.Error("LookupHash is empty")
	}
	if k.LookupHash != hashutil.SHA256Hex(raw) {
		t.Errorf("LookupHash = %q, want SHA256Hex(rawKey) = %q", k.LookupHash, hashutil.SHA256Hex(raw))
	}
	if k.TenantID != "t_test" {
		t.Errorf("TenantID = %q, want t_test", k.TenantID)
	}
	if k.Name != "my-key" {
		t.Errorf("Name = %q, want my-key", k.Name)
	}
	if k.Role != domain.RoleDeveloper {
		t.Errorf("Role = %q, want %q", k.Role, domain.RoleDeveloper)
	}
}

// TestMintAPIKey_UniqueRawKeys asserts that successive mints produce
// different raw keys. argon2id salting protects against rainbow tables
// only if the underlying secret is unique.
func TestMintAPIKey_UniqueRawKeys(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 8; i++ {
		raw, _, err := mintAPIKey("t_test", "k", domain.RoleDeveloper)
		if err != nil {
			t.Fatalf("mintAPIKey: %v", err)
		}
		if seen[raw] {
			t.Fatalf("rawKey %q minted twice — randomness failure", raw)
		}
		seen[raw] = true
	}
}

// TestMintAPIKey_OwnerRoleUsedByBootstrap pins the role that
// BootstrapTenant's caller relies on. The bootstrap path passes
// domain.RoleOwner as a constant; if a future refactor changes the
// helper to require the role to be valid (CreateAPIKey does), this
// test surfaces the regression before it can ship.
func TestMintAPIKey_OwnerRoleUsedByBootstrap(t *testing.T) {
	_, k, err := mintAPIKey("t_test", "default", domain.RoleOwner)
	if err != nil {
		t.Fatalf("mintAPIKey: %v", err)
	}
	if k.Role != domain.RoleOwner {
		t.Errorf("Role = %q, want %q (BootstrapTenant relies on this role)", k.Role, domain.RoleOwner)
	}
	if !domain.IsValidRole(k.Role) {
		t.Errorf("Role %q is not valid per domain.IsValidRole", k.Role)
	}
}

func isLowerHex(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}
