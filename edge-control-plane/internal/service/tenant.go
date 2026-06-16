package service

import (
	"context"
	"fmt"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// TenantService handles tenant business logic.
type TenantService struct {
	db        *sqlx.DB
	tenantRepo *repository.TenantRepository
	quotaRepo  *repository.QuotaRepository
}

func NewTenantService(db *sqlx.DB, tenantRepo *repository.TenantRepository, quotaRepo *repository.QuotaRepository) *TenantService {
	return &TenantService{db: db, tenantRepo: tenantRepo, quotaRepo: quotaRepo}
}

// CreateTenant creates a new tenant with default quota atomically.
func (s *TenantService) CreateTenant(ctx context.Context, name, plan string) (*domain.Tenant, error) {
	tenant := &domain.Tenant{
		ID:                     "t_" + uuid.New().String(),
		Name:                   name,
		Plan:                   plan,
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

func (s *TenantService) ListTenants(ctx context.Context) ([]domain.Tenant, error) {
	return s.tenantRepo.List(ctx)
}

func (s *TenantService) UpdateTenant(ctx context.Context, t *domain.Tenant) error {
	return s.tenantRepo.Update(ctx, t)
}

func (s *TenantService) DeleteTenant(ctx context.Context, id string) error {
	return s.tenantRepo.Delete(ctx, id)
}
