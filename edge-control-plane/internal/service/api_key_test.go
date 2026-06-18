package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/hashutil"
)

// mockAPIKeyRepo implements APIKeyRepo for testing.
type mockAPIKeyRepo struct {
	createFn                func(ctx context.Context, k *domain.APIKey) error
	getByLookupHashFn       func(ctx context.Context, lookupHash string) (*domain.APIKey, error)
	listByTenantFn          func(ctx context.Context, tenantID string) ([]domain.APIKey, error)
	deleteFn                func(ctx context.Context, id string) error
	updateLastUsedFn        func(ctx context.Context, id string) error
	updateHashIfAlgorithmFn func(ctx context.Context, id, currentAlgo, newHash, newAlgo string) (int64, error)
}

func (m *mockAPIKeyRepo) Create(ctx context.Context, k *domain.APIKey) error {
	if m.createFn == nil {
		return nil
	}
	return m.createFn(ctx, k)
}
func (m *mockAPIKeyRepo) GetByLookupHash(ctx context.Context, lookupHash string) (*domain.APIKey, error) {
	if m.getByLookupHashFn == nil {
		return nil, nil
	}
	return m.getByLookupHashFn(ctx, lookupHash)
}
func (m *mockAPIKeyRepo) ListByTenant(ctx context.Context, tenantID string) ([]domain.APIKey, error) {
	if m.listByTenantFn == nil {
		return nil, nil
	}
	return m.listByTenantFn(ctx, tenantID)
}
func (m *mockAPIKeyRepo) Delete(ctx context.Context, id string) error {
	if m.deleteFn == nil {
		return nil
	}
	return m.deleteFn(ctx, id)
}
func (m *mockAPIKeyRepo) UpdateLastUsed(ctx context.Context, id string) error {
	if m.updateLastUsedFn == nil {
		return nil
	}
	return m.updateLastUsedFn(ctx, id)
}
func (m *mockAPIKeyRepo) UpdateHashIfAlgorithm(ctx context.Context, id, currentAlgo, newHash, newAlgo string) (int64, error) {
	if m.updateHashIfAlgorithmFn == nil {
		return 0, nil
	}
	return m.updateHashIfAlgorithmFn(ctx, id, currentAlgo, newHash, newAlgo)
}

func TestAPIKeyService_AuthenticateRawKey_Argon2_HappyPath(t *testing.T) {
	raw := "the-real-secret-key"
	hash, err := HashAPIKey(raw)
	if err != nil {
		t.Fatalf("HashAPIKey: %v", err)
	}
	shaHex := hashutil.SHA256Hex(raw)

	lastUsedCalled := false
	repo := &mockAPIKeyRepo{
		getByLookupHashFn: func(ctx context.Context, h string) (*domain.APIKey, error) {
			if h != shaHex {
				t.Errorf("repo queried with wrong lookup hash: got %q want %q", h, shaHex)
			}
			return &domain.APIKey{
				ID:            "k_test",
				TenantID:      "t_test",
				KeyHash:       hash,
				LookupHash:    shaHex,
				HashAlgorithm: domain.HashAlgorithmArgon2ID,
				Role:          domain.RoleDeveloper,
			}, nil
		},
		updateLastUsedFn: func(ctx context.Context, id string) error {
			lastUsedCalled = true
			return nil
		},
	}
	svc := NewAPIKeyService(nil)
	svc.apiKeyRepo = repo

	got, err := svc.AuthenticateRawKey(context.Background(), raw)
	if err != nil {
		t.Fatalf("AuthenticateRawKey: %v", err)
	}
	if got.ID != "k_test" {
		t.Errorf("ID = %q, want k_test", got.ID)
	}
	if !lastUsedCalled {
		t.Error("UpdateLastUsed was not called on successful auth")
	}
}

func TestAPIKeyService_AuthenticateRawKey_Argon2_WrongKey(t *testing.T) {
	hash, _ := HashAPIKey("the-real-key")
	repo := &mockAPIKeyRepo{
		getByLookupHashFn: func(ctx context.Context, h string) (*domain.APIKey, error) {
			return &domain.APIKey{ID: "k_test", KeyHash: hash, LookupHash: h, HashAlgorithm: domain.HashAlgorithmArgon2ID}, nil
		},
	}
	svc := NewAPIKeyService(nil)
	svc.apiKeyRepo = repo

	_, err := svc.AuthenticateRawKey(context.Background(), "the-wrong-key")
	if !errors.Is(err, ErrInvalidAPIKey) {
		t.Errorf("error = %v, want ErrInvalidAPIKey", err)
	}
}

func TestAPIKeyService_AuthenticateRawKey_LegacySHA256_LazyRehash(t *testing.T) {
	// Simulate a pre-migration row: stored as hex SHA-256.
	raw := "legacy-raw-key"
	legacyHash := hashutil.SHA256Hex(raw)

	var rehashCalled bool
	var rehashToArgon bool
	var rehashID string
	var casAlgo string
	repo := &mockAPIKeyRepo{
		getByLookupHashFn: func(ctx context.Context, h string) (*domain.APIKey, error) {
			return &domain.APIKey{
				ID:            "k_legacy",
				TenantID:      "t_test",
				KeyHash:       legacyHash,
				LookupHash:    h,
				HashAlgorithm: domain.HashAlgorithmSHA256, // legacy
				Role:          domain.RoleDeveloper,
			}, nil
		},
		updateHashIfAlgorithmFn: func(ctx context.Context, id, currentAlgo, newHash, newAlgo string) (int64, error) {
			rehashCalled = true
			rehashID = id
			casAlgo = currentAlgo
			if newAlgo == domain.HashAlgorithmArgon2ID && strings.HasPrefix(newHash, "$argon2id$") {
				rehashToArgon = true
			}
			return 1, nil
		},
	}
	svc := NewAPIKeyService(nil)
	svc.apiKeyRepo = repo

	got, err := svc.AuthenticateRawKey(context.Background(), raw)
	if err != nil {
		t.Fatalf("AuthenticateRawKey: %v", err)
	}
	if got.ID != "k_legacy" {
		t.Errorf("ID = %q, want k_legacy", got.ID)
	}
	if !rehashCalled {
		t.Error("UpdateHashIfAlgorithm was not called — legacy key was not lazily upgraded")
	}
	if !rehashToArgon {
		t.Error("rehash did not write argon2id format")
	}
	if rehashID != "k_legacy" {
		t.Errorf("rehash ID = %q, want k_legacy", rehashID)
	}
	if casAlgo != domain.HashAlgorithmSHA256 {
		t.Errorf("CAS guard algo = %q, want %q", casAlgo, domain.HashAlgorithmSHA256)
	}
}

// TestAPIKeyService_AuthenticateRawKey_ConcurrentLazyRehash exercises the
// atomic-CAS path against a mock that models the production race.
//
// Scenario: 4 goroutines authenticate the same legacy SHA-256 row
// concurrently. The mock is a tiny in-memory table — once the first CAS
// "wins", subsequent lookups return the row in its upgraded argon2id
// format and bypass the CAS path entirely (the production behavior the
// CAS guard exists to enable).
//
// Invariants verified:
//   - every goroutine's auth succeeds (no panic, no spurious error),
//   - exactly one CAS guard matches (rowsAffected == 1),
//   - subsequent lookups see an argon2id row and skip the CAS path.
func TestAPIKeyService_AuthenticateRawKey_ConcurrentLazyRehash(t *testing.T) {
	raw := "concurrent-legacy-key"
	legacyHash := hashutil.SHA256Hex(raw)
	argonHash, err := HashAPIKey(raw)
	if err != nil {
		t.Fatalf("HashAPIKey: %v", err)
	}

	var (
		mu          sync.Mutex
		casAttempts int
		casWins     int
		upgraded    bool
	)

	updateHashIfAlgorithmFn := func(ctx context.Context, id, currentAlgo, newHash, newAlgo string) (int64, error) {
		mu.Lock()
		defer mu.Unlock()
		casAttempts++
		var affected int64
		// Only the first CAS guard sees algorithm=sha256 — every later
		// attempt would see argon2id and skip the CAS path entirely.
		if !upgraded {
			affected = 1
			casWins++
			upgraded = true
		}
		return affected, nil
	}

	repo := &mockAPIKeyRepo{
		getByLookupHashFn: func(ctx context.Context, h string) (*domain.APIKey, error) {
			mu.Lock()
			defer mu.Unlock()
			if upgraded {
				// Production: once the row is upgraded, subsequent lookups
				// see HashAlgorithm=argon2id. The auth path takes the
				// VerifyAPIKey branch, never re-entering the CAS path.
				return &domain.APIKey{
					ID:            "k_legacy",
					TenantID:      "t_test",
					KeyHash:       argonHash,
					LookupHash:    h,
					HashAlgorithm: domain.HashAlgorithmArgon2ID,
					Role:          domain.RoleDeveloper,
				}, nil
			}
			return &domain.APIKey{
				ID:            "k_legacy",
				TenantID:      "t_test",
				KeyHash:       legacyHash,
				LookupHash:    h,
				HashAlgorithm: domain.HashAlgorithmSHA256,
				Role:          domain.RoleDeveloper,
			}, nil
		},
		updateHashIfAlgorithmFn: updateHashIfAlgorithmFn,
	}
	svc := NewAPIKeyService(nil)
	svc.apiKeyRepo = repo

	const goroutines = 4
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := svc.AuthenticateRawKey(context.Background(), raw)
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent auth returned error: %v", err)
	}
	// Exactly one CAS guard matches: the first auth's. All later auths
	// hit the upgraded argon2id row and bypass the CAS path entirely.
	// (In a contended test the CAS count is 1 — the mock serializes
	// upgrades atomically. In a more relaxed schedule some lookups could
	// happen after the upgrade, lowering CAS attempts below goroutines;
	// the strict invariant is casWins == 1, not casAttempts == N.)
	if casWins != 1 {
		t.Errorf("CAS wins = %d, want exactly 1", casWins)
	}
	if casAttempts < 1 {
		t.Errorf("CAS attempts = %d, want at least 1", casAttempts)
	}
	if casAttempts > goroutines {
		t.Errorf("CAS attempts = %d, want at most %d (one per goroutine)", casAttempts, goroutines)
	}
}

func TestAPIKeyService_AuthenticateRawKey_NoSuchKey(t *testing.T) {
	repo := &mockAPIKeyRepo{
		getByLookupHashFn: func(ctx context.Context, h string) (*domain.APIKey, error) {
			return nil, nil // not found
		},
	}
	svc := NewAPIKeyService(nil)
	svc.apiKeyRepo = repo

	_, err := svc.AuthenticateRawKey(context.Background(), "any-key")
	if !errors.Is(err, ErrInvalidAPIKey) {
		t.Errorf("error = %v, want ErrInvalidAPIKey", err)
	}
}

func TestAPIKeyService_AuthenticateRawKey_Empty(t *testing.T) {
	svc := NewAPIKeyService(nil)
	_, err := svc.AuthenticateRawKey(context.Background(), "")
	if !errors.Is(err, ErrInvalidAPIKey) {
		t.Errorf("error = %v, want ErrInvalidAPIKey", err)
	}
}

// TestAPIKeyService_AuthenticateRawKey_RejectsExpiredKey asserts that an
// API key whose ExpiresAt is in the past is rejected with ErrInvalidAPIKey,
// even when the underlying hash verifies correctly. The check runs after
// the verify so a bug here cannot be exploited to enumerate valid IDs.
func TestAPIKeyService_AuthenticateRawKey_RejectsExpiredKey(t *testing.T) {
	raw := "expired-but-correctly-hashed-key"
	hash, err := HashAPIKey(raw)
	if err != nil {
		t.Fatalf("HashAPIKey: %v", err)
	}
	past := time.Now().Add(-1 * time.Hour)

	repo := &mockAPIKeyRepo{
		getByLookupHashFn: func(ctx context.Context, h string) (*domain.APIKey, error) {
			return &domain.APIKey{
				ID:            "k_expired",
				TenantID:      "t_test",
				KeyHash:       hash,
				LookupHash:    h,
				HashAlgorithm: domain.HashAlgorithmArgon2ID,
				Role:          domain.RoleDeveloper,
				ExpiresAt:     &past,
			}, nil
		},
	}
	svc := NewAPIKeyService(nil)
	svc.apiKeyRepo = repo

	_, err = svc.AuthenticateRawKey(context.Background(), raw)
	if !errors.Is(err, ErrInvalidAPIKey) {
		t.Errorf("error = %v, want ErrInvalidAPIKey for expired key", err)
	}
}

// TestAPIKeyService_AuthenticateRawKey_AcceptsUnsetExpiry asserts that a
// key with a nil ExpiresAt (never expires) is still accepted after the
// new expiry check is in place.
func TestAPIKeyService_AuthenticateRawKey_AcceptsUnsetExpiry(t *testing.T) {
	raw := "key-with-no-expiry"
	hash, err := HashAPIKey(raw)
	if err != nil {
		t.Fatalf("HashAPIKey: %v", err)
	}

	repo := &mockAPIKeyRepo{
		getByLookupHashFn: func(ctx context.Context, h string) (*domain.APIKey, error) {
			return &domain.APIKey{
				ID:            "k_noexpiry",
				TenantID:      "t_test",
				KeyHash:       hash,
				LookupHash:    h,
				HashAlgorithm: domain.HashAlgorithmArgon2ID,
				Role:          domain.RoleDeveloper,
				ExpiresAt:     nil, // never expires
			}, nil
		},
	}
	svc := NewAPIKeyService(nil)
	svc.apiKeyRepo = repo

	got, err := svc.AuthenticateRawKey(context.Background(), raw)
	if err != nil {
		t.Fatalf("AuthenticateRawKey: %v", err)
	}
	if got.ID != "k_noexpiry" {
		t.Errorf("ID = %q, want k_noexpiry", got.ID)
	}
}

// TestAPIKeyService_AuthenticateRawKey_AcceptsFutureExpiry asserts that a
// key whose ExpiresAt is in the future is accepted.
func TestAPIKeyService_AuthenticateRawKey_AcceptsFutureExpiry(t *testing.T) {
	raw := "key-future-expiry"
	hash, err := HashAPIKey(raw)
	if err != nil {
		t.Fatalf("HashAPIKey: %v", err)
	}
	future := time.Now().Add(24 * time.Hour)

	repo := &mockAPIKeyRepo{
		getByLookupHashFn: func(ctx context.Context, h string) (*domain.APIKey, error) {
			return &domain.APIKey{
				ID:            "k_future",
				TenantID:      "t_test",
				KeyHash:       hash,
				LookupHash:    h,
				HashAlgorithm: domain.HashAlgorithmArgon2ID,
				Role:          domain.RoleDeveloper,
				ExpiresAt:     &future,
			}, nil
		},
	}
	svc := NewAPIKeyService(nil)
	svc.apiKeyRepo = repo

	got, err := svc.AuthenticateRawKey(context.Background(), raw)
	if err != nil {
		t.Fatalf("AuthenticateRawKey: %v", err)
	}
	if got.ID != "k_future" {
		t.Errorf("ID = %q, want k_future", got.ID)
	}
}

// TestAPIKeyService_AuthenticateRawKey_ExpiredLegacyKeySkipsRehash pins
// the ExpiresAt ordering fix from the PR #64 review. The previous
// implementation ran the expiry check AFTER the algorithm switch, so the
// legacy SHA-256 → argon2id rehash write to the DB fired for an expired
// key. An attacker who knew the legacy raw for an expired account could
// thus cause a successful argon2id row replacement via the lazy-rehash
// path. The CAS guard does not protect against expiry.
//
// The test wires a mock with a counter on UpdateHashIfAlgorithm. If the
// rehash fires, the counter goes to 1. We assert it stays 0.
func TestAPIKeyService_AuthenticateRawKey_ExpiredLegacyKeySkipsRehash(t *testing.T) {
	raw := "expired-legacy-raw"
	legacyHash := hashutil.SHA256Hex(raw)
	past := time.Now().Add(-1 * time.Hour)

	var rehashCalls int
	repo := &mockAPIKeyRepo{
		getByLookupHashFn: func(ctx context.Context, h string) (*domain.APIKey, error) {
			return &domain.APIKey{
				ID:            "k_legacy_expired",
				TenantID:      "t_test",
				KeyHash:       legacyHash, // legacy SHA-256
				LookupHash:    h,
				HashAlgorithm: domain.HashAlgorithmSHA256,
				Role:          domain.RoleDeveloper,
				ExpiresAt:     &past,
			}, nil
		},
		updateHashIfAlgorithmFn: func(ctx context.Context, id, currentAlgo, newHash, newAlgo string) (int64, error) {
			rehashCalls++
			return 1, nil
		},
		updateLastUsedFn: func(ctx context.Context, id string) error {
			t.Errorf("UpdateLastUsed should not be called for an expired key, got id=%s", id)
			return nil
		},
	}
	svc := NewAPIKeyService(nil)
	svc.apiKeyRepo = repo

	_, err := svc.AuthenticateRawKey(context.Background(), raw)
	if !errors.Is(err, ErrInvalidAPIKey) {
		t.Errorf("error = %v, want ErrInvalidAPIKey for expired legacy key", err)
	}
	if rehashCalls != 0 {
		t.Errorf("rehash called %d times for expired legacy key; want 0 (expiry must gate before rehash)", rehashCalls)
	}
}
