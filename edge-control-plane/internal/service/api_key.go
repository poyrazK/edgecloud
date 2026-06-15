package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/google/uuid"
)

// APIKeyService handles API key business logic.
type APIKeyService struct {
	apiKeyRepo *repository.APIKeyRepository
}

func NewAPIKeyService(apiKeyRepo *repository.APIKeyRepository) *APIKeyService {
	return &APIKeyService{apiKeyRepo: apiKeyRepo}
}

// CreateAPIKey creates a new API key and returns the raw key (shown only once).
func (s *APIKeyService) CreateAPIKey(ctx context.Context, tenantID, name, role string) (*domain.APIKey, string, error) {
	if !domain.IsValidRole(role) {
		return nil, "", fmt.Errorf("invalid role: %s", role)
	}

	// Generate random key
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return nil, "", fmt.Errorf("generating key: %w", err)
	}
	rawKey := hex.EncodeToString(raw)

	hash := sha256.Sum256([]byte(rawKey))
	keyHash := hex.EncodeToString(hash[:])

	apiKey := &domain.APIKey{
		ID:       "k_" + uuid.New().String(),
		TenantID: tenantID,
		Name:     name,
		KeyHash:  keyHash,
		Role:     role,
	}

	if err := s.apiKeyRepo.Create(ctx, apiKey); err != nil {
		return nil, "", fmt.Errorf("creating api key: %w", err)
	}

	return apiKey, rawKey, nil
}

func (s *APIKeyService) ListAPIKeys(ctx context.Context, tenantID string) ([]domain.APIKey, error) {
	return s.apiKeyRepo.ListByTenant(ctx, tenantID)
}

func (s *APIKeyService) DeleteAPIKey(ctx context.Context, id string) error {
	return s.apiKeyRepo.Delete(ctx, id)
}
