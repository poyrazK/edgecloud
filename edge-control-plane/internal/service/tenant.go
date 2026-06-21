package service

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// MaxEgressAllowlistEntries is the maximum number of entries a tenant may specify.
const MaxEgressAllowlistEntries = 50

// hostnameRe accepts a plain hostname or FQDN: alphanumeric labels separated
// by dots, hyphens allowed in the interior of each label.
var hostnameRe = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*$`)

// EgressValidationError is returned when an allowlist entry fails input validation.
// Handlers use errors.As to map it to HTTP 400; all other errors become HTTP 500.
type EgressValidationError struct{ msg string }

func (e *EgressValidationError) Error() string { return e.msg }

func egressValidationErr(format string, args ...any) *EgressValidationError {
	return &EgressValidationError{msg: fmt.Sprintf(format, args...)}
}

// validateEgressAllowlist returns an *EgressValidationError if any entry is malformed.
// Accepted forms: "foo.example.com" or "*.example.com" (one wildcard label).
// The bare "*" sentinel is rejected so tenants cannot bypass enforcement.
func validateEgressAllowlist(entries []string) error {
	if len(entries) > MaxEgressAllowlistEntries {
		return egressValidationErr("allowlist exceeds maximum of %d entries", MaxEgressAllowlistEntries)
	}
	for _, e := range entries {
		if e == "*" {
			return egressValidationErr("wildcard-only entry %q is not allowed; use a hostname or *.suffix pattern", e)
		}
		host := e
		if strings.HasPrefix(e, "*.") {
			host = e[2:]
			if !strings.Contains(host, ".") {
				return egressValidationErr("wildcard entry %q must have at least two labels after *. (e.g. *.example.com)", e)
			}
		} else if strings.ContainsAny(e, "*/") || strings.HasPrefix(e, "http") {
			return egressValidationErr("entry %q must be a plain hostname or *.suffix (no scheme, no path, no slash)", e)
		} else if !strings.Contains(e, ".") {
			// Single-label names (localhost, intranet, metadata) are not valid
			// public hostnames and may resolve to internal services on the worker.
			return egressValidationErr("entry %q must be a fully qualified hostname (e.g. api.example.com)", e)
		}
		if !hostnameRe.MatchString(host) {
			return egressValidationErr("entry %q is not a valid hostname", e)
		}
	}
	return nil
}

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
		AllowlistedDestinations: pq.StringArray{},
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
		AllowlistedDestinations: pq.StringArray{},
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

// GetEgressAllowlist returns the current outbound allowlist for a tenant.
// Returns an empty slice (not nil) when no allowlist is configured (allow-all).
func (s *TenantService) GetEgressAllowlist(ctx context.Context, tenantID string) ([]string, error) {
	tenant, err := s.tenantRepo.GetByID(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("getting tenant: %w", err)
	}
	if tenant == nil {
		return nil, fmt.Errorf("tenant not found")
	}
	if len(tenant.AllowlistedDestinations) == 0 {
		return []string{}, nil
	}
	return []string(tenant.AllowlistedDestinations), nil
}

// UpdateEgressAllowlist replaces the tenant's outbound allowlist after validation.
// Passing an empty slice clears the list (restores allow-all behaviour).
func (s *TenantService) UpdateEgressAllowlist(ctx context.Context, tenantID string, allowlist []string) error {
	if err := validateEgressAllowlist(allowlist); err != nil {
		return err
	}
	tenant, err := s.tenantRepo.GetByID(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("getting tenant: %w", err)
	}
	if tenant == nil {
		return fmt.Errorf("tenant not found")
	}
	tenant.AllowlistedDestinations = pq.StringArray(allowlist)
	return s.tenantRepo.Update(ctx, tenant)
}
