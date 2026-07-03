package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/hashutil"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/google/uuid"
)

// ErrInvalidAPIKey is returned by AuthenticateRawKey when the raw key does not
// match any row. Callers should map this to 401 Unauthorized.
var ErrInvalidAPIKey = errors.New("invalid api key")

// APIKeyRepo is the subset of *repository.APIKeyRepository used by
// APIKeyService. It is exported so tests outside this package (notably the
// middleware layer) can substitute a stub without depending on the concrete
// repository type.
type APIKeyRepo interface {
	Create(ctx context.Context, k *domain.APIKey) error
	GetByID(ctx context.Context, id string) (*domain.APIKey, error)
	GetByLookupHash(ctx context.Context, lookupHash string) (*domain.APIKey, error)
	ListByTenant(ctx context.Context, tenantID string) ([]domain.APIKey, error)
	Delete(ctx context.Context, id string) error
	Update(ctx context.Context, k *domain.APIKey) error
	UpdateLastUsed(ctx context.Context, id string) error
	UpdateHashIfAlgorithm(ctx context.Context, id, currentAlgo, newHash, newAlgo string) (int64, error)
}

// APIKeyService handles API key business logic.
type APIKeyService struct {
	apiKeyRepo APIKeyRepo
}

func NewAPIKeyService(apiKeyRepo *repository.APIKeyRepository) *APIKeyService {
	return &APIKeyService{apiKeyRepo: apiKeyRepo}
}

// SetAPIKeyRepo replaces the repository used by this service. It exists
// for tests outside the package (e.g. the middleware suite) that need to
// inject a stub repository; production code should rely on the
// NewAPIKeyService constructor instead. Returns the receiver so it can be
// chained immediately after construction.
func (s *APIKeyService) SetAPIKeyRepo(r APIKeyRepo) *APIKeyService {
	s.apiKeyRepo = r
	return s
}

// CreateAPIKey creates a new API key and returns the raw key (shown only once).
//
// Keys are stored as argon2id hashes (PHC-formatted). The raw key is returned
// to the caller exactly once and is never persisted.
func (s *APIKeyService) CreateAPIKey(ctx context.Context, tenantID, name, role string) (*domain.APIKey, string, error) {
	if !domain.IsValidRole(role) {
		return nil, "", fmt.Errorf("invalid role: %s", role)
	}

	rawKey, apiKey, err := mintAPIKey(tenantID, name, role)
	if err != nil {
		return nil, "", err
	}

	if err := s.apiKeyRepo.Create(ctx, apiKey); err != nil {
		return nil, "", fmt.Errorf("creating api key: %w", err)
	}

	return apiKey, rawKey, nil
}

// mintAPIKey generates a 32-byte random key, hashes it with argon2id, and
// returns both the raw key (shown to the caller exactly once) and a fully
// populated *domain.APIKey ready to persist. Shared by CreateAPIKey and
// BootstrapTenant so both code paths produce identical APIKey structures.
//
// Caller responsibilities:
//   - validate the role before calling (CreateAPIKey does this; BootstrapTenant
//     passes the trusted RoleOwner constant);
//   - call apiKeyRepo.Create(ctx, apiKey) within the same transaction.
func mintAPIKey(tenantID, name, role string) (string, *domain.APIKey, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, fmt.Errorf("generating key: %w", err)
	}
	rawKey := hex.EncodeToString(raw)

	keyHash, err := HashAPIKey(rawKey)
	if err != nil {
		return "", nil, fmt.Errorf("hashing key: %w", err)
	}

	// lookupHash is the stable SHA-256 hex of the raw key. AuthenticateRawKey
	// queries this column to locate a candidate row before dispatching to the
	// algorithm-specific verifier. Independent of the encoded KeyHash so it
	// stays a fixed-length, fixed-format lookup key even after algorithm
	// changes. (Migration 006.)
	lookupHash := hashutil.SHA256Hex(rawKey)

	apiKey := &domain.APIKey{
		ID:            "k_" + uuid.New().String(),
		TenantID:      tenantID,
		Name:          name,
		KeyHash:       keyHash,
		LookupHash:    lookupHash,
		Role:          role,
		HashAlgorithm: domain.HashAlgorithmArgon2ID,
	}
	return rawKey, apiKey, nil
}

// ErrAPIKeyNotFound is returned by UpdateAPIKey when the key does not exist
// or does not belong to the requesting tenant.
var ErrAPIKeyNotFound = fmt.Errorf("api key not found")

// UpdateAPIKey updates mutable fields (name, role) of an existing API key.
// Returns ErrAPIKeyNotFound if the key is missing or belongs to a different tenant.
func (s *APIKeyService) UpdateAPIKey(ctx context.Context, id, tenantID string, req *domain.UpdateAPIKeyRequest) (*domain.APIKey, error) {
	key, err := s.apiKeyRepo.GetByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("getting api key: %w", err)
	}
	if key == nil || key.TenantID != tenantID {
		return nil, ErrAPIKeyNotFound
	}

	if req.Name != nil {
		key.Name = *req.Name
	}
	if req.Role != nil {
		if !domain.IsValidRole(*req.Role) {
			return nil, fmt.Errorf("invalid role: %s", *req.Role)
		}
		key.Role = *req.Role
	}

	if err := s.apiKeyRepo.Update(ctx, key); err != nil {
		return nil, fmt.Errorf("updating api key: %w", err)
	}
	return key, nil
}

func (s *APIKeyService) ListAPIKeys(ctx context.Context, tenantID string) ([]domain.APIKey, error) {
	return s.apiKeyRepo.ListByTenant(ctx, tenantID)
}

// APIKeyServiceInterface abstracts API key operations for testing.
// *APIKeyService satisfies this interface.
type APIKeyServiceInterface interface {
	CreateAPIKey(ctx context.Context, tenantID, name, role string) (*domain.APIKey, string, error)
	ListAPIKeys(ctx context.Context, tenantID string) ([]domain.APIKey, error)
	GetByID(ctx context.Context, id string) (*domain.APIKey, error)
	DeleteAPIKey(ctx context.Context, id string) error
	UpdateAPIKey(ctx context.Context, id, tenantID string, req *domain.UpdateAPIKeyRequest) (*domain.APIKey, error)
}

// GetByID returns a single API key by its prefixed ID (e.g. "k_<uuid>").
// Returns (nil, nil) when no such key exists.
func (s *APIKeyService) GetByID(ctx context.Context, id string) (*domain.APIKey, error) {
	return s.apiKeyRepo.GetByID(ctx, id)
}

func (s *APIKeyService) DeleteAPIKey(ctx context.Context, id string) error {
	return s.apiKeyRepo.Delete(ctx, id)
}

// AuthenticateRawKey looks up a key by its stable SHA-256 lookup hash and
// dispatches to the algorithm stored in the row.
//
// New keys are stored as argon2id (PHC-formatted); legacy keys created before
// migration 005 keep their hex SHA-256 hash. On successful verification of a
// legacy row the function transparently rehashes the key with argon2id and
// persists the upgrade, so callers don't need to do anything special and the
// migration finishes organically as each key is next used.
//
// The SHA-256 hex used for lookup is decoupled from the algorithm-specific
// KeyHash so the same lookup key works for both sha256 and argon2id rows
// (migration 006).
func (s *APIKeyService) AuthenticateRawKey(ctx context.Context, rawKey string) (*domain.APIKey, error) {
	if rawKey == "" {
		return nil, ErrInvalidAPIKey
	}

	lookup := hashutil.SHA256Hex(rawKey)

	candidate, err := s.apiKeyRepo.GetByLookupHash(ctx, lookup)
	if err != nil {
		return nil, fmt.Errorf("looking up api key: %w", err)
	}
	if candidate == nil {
		return nil, ErrInvalidAPIKey
	}

	// Defense-in-depth: enforce expiry BEFORE any DB writes (notably the
	// legacy SHA-256 → argon2id rehash). If the check ran after the
	// algorithm switch, an attacker who knew a legacy raw key for an
	// expired account could still cause a successful argon2id row
	// replacement via the lazy-rehash path. The CAS guard does not
	// protect against expiry — expiry must gate the response on its
	// own. The check sits after the lookup so a bug here cannot be
	// exploited to enumerate which IDs are valid vs expired (the lookup
	// already gates that).
	if candidate.ExpiresAt != nil && time.Now().After(*candidate.ExpiresAt) {
		return nil, ErrInvalidAPIKey
	}

	algo := candidate.HashAlgorithm
	if algo == "" {
		// Pre-migration rows. Treat as legacy SHA-256.
		algo = domain.HashAlgorithmSHA256
	}

	switch algo {
	case domain.HashAlgorithmSHA256:
		// Legacy fast path: the lookup_hash matched (the SHA-256 hex of the
		// raw key), so verification succeeds. Lazily upgrade to argon2id.
		// Use atomic CAS so concurrent auths don't ping-pong the row with
		// different random salts — only one CAS wins, the rest silently
		// observe the row in its upgraded state on their next auth.
		newHash, hashErr := HashAPIKey(rawKey)
		if hashErr != nil {
			log.Printf("warning: lazy rehash failed for key %s: %v", candidate.ID, hashErr)
		} else if _, err := s.apiKeyRepo.UpdateHashIfAlgorithm(
			ctx, candidate.ID,
			domain.HashAlgorithmSHA256, newHash, domain.HashAlgorithmArgon2ID,
		); err != nil {
			log.Printf("warning: failed to persist lazy rehash for key %s: %v", candidate.ID, err)
		}

	case domain.HashAlgorithmArgon2ID:
		ok, err := VerifyAPIKey(rawKey, candidate.KeyHash)
		if err != nil {
			return nil, fmt.Errorf("verifying api key: %w", err)
		}
		if !ok {
			return nil, ErrInvalidAPIKey
		}

	default:
		return nil, fmt.Errorf("unsupported hash algorithm %q for key %s", algo, candidate.ID)
	}

	if err := s.apiKeyRepo.UpdateLastUsed(ctx, candidate.ID); err != nil {
		log.Printf("warning: failed to update last_used for api key %s: %v", candidate.ID, err)
	}
	return candidate, nil
}
