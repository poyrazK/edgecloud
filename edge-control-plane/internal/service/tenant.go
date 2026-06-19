package service

import (
	"context"
	"fmt"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// TenantServiceInterface abstracts tenant operations for testing.
type TenantServiceInterface interface {
	BootstrapTenant(ctx context.Context, name, plan, keyName string) (*domain.Tenant, string, error)
	CreateTenant(ctx context.Context, name, plan string) (*domain.Tenant, error)
	GetTenant(ctx context.Context, id string) (*domain.TenantWithQuota, error)
	GetQuota(ctx context.Context, tenantID string) (*domain.Quota, error)
	ListTenants(ctx context.Context) ([]domain.Tenant, error)
	UpdateTenant(ctx context.Context, t *domain.Tenant) error
	DeleteTenant(ctx context.Context, id string) error
}

// TenantService handles tenant business logic.
type TenantService struct {
	db         *sqlx.DB
	tenantRepo *repository.TenantRepository
	quotaRepo  *repository.QuotaRepository
	apiKeyRepo *repository.APIKeyRepository
}

func NewTenantService(db *sqlx.DB, tenantRepo *repository.TenantRepository, quotaRepo *repository.QuotaRepository, apiKeyRepo *repository.APIKeyRepository) *TenantService {
	return &TenantService{db: db, tenantRepo: tenantRepo, quotaRepo: quotaRepo, apiKeyRepo: apiKeyRepo}
}

// CreateTenant creates a new tenant with default quota atomically.
func (s *TenantService) CreateTenant(ctx context.Context, name, plan string) (*domain.Tenant, error) {
	tenant := &domain.Tenant{
		ID:                      "t_" + uuid.New().String(),
		Name:                    name,
		Plan:                    plan,
		AllowlistedDestinations: []string{},
	}

	var created *domain.Tenant
	err := repository.Transaction(ctx, s.db, func(tx *sqlx.Tx) error {
		tenantRepo := s.tenantRepo.WithTx(tx)
		quotaRepo := s.quotaRepo.WithTx(tx)

		if err := tenantRepo.Create(ctx, tenant); err != nil {
			return fmt.Errorf("creating tenant: %w", err)
		}

		quota := domain.DefaultQuota(tenant.ID)
		if err := quotaRepo.Create(ctx, &quota); err != nil {
			return fmt.Errorf("creating quota: %w", err)
		}

		created = tenant
		return nil
	})

	if err != nil {
		return nil, err
	}
	return created, nil
}

// BootstrapTenant creates a new tenant with its first API key atomically.
// This is the self-signup entry point: one request creates tenant + initial
// owner key.
//
// The initial key is minted through mintAPIKey so it carries the same
// fields (argon2id hash, lookup hash, role) as a key produced by
// CreateAPIKey. Without this, the bootstrap path was writing raw SHA-256
// hashes with no HashAlgorithm or LookupHash — the new repo guards added
// for migrations 005/007 would have rejected the row outright.
func (s *TenantService) BootstrapTenant(ctx context.Context, name, plan, keyName string) (*domain.Tenant, string, error) {
	tenant := &domain.Tenant{
		ID:                      "t_" + uuid.New().String(),
		Name:                    name,
		Plan:                    plan,
		AllowlistedDestinations: []string{},
	}

	var rawKey string
	var created *domain.Tenant

	err := repository.Transaction(ctx, s.db, func(tx *sqlx.Tx) error {
		tenantRepo := s.tenantRepo.WithTx(tx)
		quotaRepo := s.quotaRepo.WithTx(tx)
		apiKeyRepo := s.apiKeyRepo.WithTx(tx)

		if err := tenantRepo.Create(ctx, tenant); err != nil {
			return fmt.Errorf("creating tenant: %w", err)
		}

		quota := domain.DefaultQuota(tenant.ID)
		if err := quotaRepo.Create(ctx, &quota); err != nil {
			return fmt.Errorf("creating quota: %w", err)
		}

		mintedRaw, apiKey, err := mintAPIKey(tenant.ID, keyName, domain.RoleOwner)
		if err != nil {
			return fmt.Errorf("minting initial api key: %w", err)
		}
		if err := apiKeyRepo.Create(ctx, apiKey); err != nil {
			return fmt.Errorf("creating api key: %w", err)
		}

		rawKey = mintedRaw
		created = tenant
		return nil
	})

	if err != nil {
		return nil, "", err
	}
	return created, rawKey, nil
}

func (s *TenantService) GetTenant(ctx context.Context, id string) (*domain.TenantWithQuota, error) {
	tenant, err := s.tenantRepo.GetByID(ctx, id)
	if err != nil || tenant == nil {
		return nil, err
	}

	quota, err := s.quotaRepo.GetByTenantID(ctx, id)
	if err != nil {
		return nil, err
	}
	if quota == nil {
		return nil, fmt.Errorf("quota not found for tenant")
	}

	return &domain.TenantWithQuota{Tenant: *tenant, Quota: *quota}, nil
}

func (s *TenantService) GetQuota(ctx context.Context, tenantID string) (*domain.Quota, error) {
	return s.quotaRepo.GetByTenantID(ctx, tenantID)
}

func (s *TenantService) ListTenants(ctx context.Context) ([]domain.Tenant, error) {
	return s.tenantRepo.List(ctx)
}

func (s *TenantService) UpdateTenant(ctx context.Context, t *domain.Tenant) error {
	return s.tenantRepo.Update(ctx, t)
}

func (s *TenantService) DeleteTenant(ctx context.Context, id string) error {
	return s.tenantRepo.Delete(ctx, id)
}
