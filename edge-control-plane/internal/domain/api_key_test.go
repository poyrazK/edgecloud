package domain

import "testing"

func TestIsValidRole(t *testing.T) {
	valid := []string{RoleOwner, RoleDeveloper, RoleViewer}
	for _, r := range valid {
		if !IsValidRole(r) {
			t.Errorf("IsValidRole(%q) = false, want true", r)
		}
	}
	invalid := []string{"admin", "readonly", "", "OWNER"}
	for _, r := range invalid {
		if IsValidRole(r) {
			t.Errorf("IsValidRole(%q) = true, want false", r)
		}
	}
}

func TestAPIKey_HashAlgorithmConstants(t *testing.T) {
	if HashAlgorithmSHA256 != "sha256" {
		t.Errorf("HashAlgorithmSHA256 = %q, want 'sha256'", HashAlgorithmSHA256)
	}
	if HashAlgorithmArgon2ID != "argon2id" {
		t.Errorf("HashAlgorithmArgon2ID = %q, want 'argon2id'", HashAlgorithmArgon2ID)
	}
}
