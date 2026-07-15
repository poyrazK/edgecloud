package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// Sentinel errors used by the tenant service. Handlers use errors.Is to
// translate these into HTTP status codes (400 for validation, 404 for
// missing rows, 500 otherwise). ErrTenantNotFound is the canonical
// "tenant row not present" sentinel for the tenant CRUD path; the
// reconcile package's older sentinel has been renamed to
// ErrTenantNotFoundInReconcile so this name is free.
var (
	ErrTenantNotFound = errors.New("tenant not found")
	ErrQuotaNotFound  = errors.New("quota not found for tenant")
	// ErrTenantDisabled is returned by ActivateDeployment /
	// RollbackDeployment when the tenant row's disabled_at is set
	// inside the FOR UPDATE gate (issue #440). The handler maps
	// this to HTTP 409 / 423 — the activate / rollback is
	// intentionally aborted rather than published.
	ErrTenantDisabled = errors.New("tenant is disabled")
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
// Entries are expected to already be lowercased (UpdateEgressAllowlist lowercases before calling).
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
		} else if strings.ContainsAny(e, "*/") || strings.HasPrefix(e, "http://") || strings.HasPrefix(e, "https://") {
			return egressValidationErr("entry %q must be a plain hostname or *.suffix (no scheme, no path, no slash)", e)
		} else if !strings.Contains(e, ".") {
			// Single-label names (localhost, intranet, metadata) are not valid
			// public hostnames and may resolve to internal services on the worker.
			return egressValidationErr("entry %q must be a fully qualified hostname (e.g. api.example.com)", e)
		}
		if net.ParseIP(host) != nil {
			return egressValidationErr("entry %q must be a hostname, not an IP address", e)
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
	GetQuotaForInternal(ctx context.Context, tenantID string) (*domain.Quota, error)
	ListTenants(ctx context.Context) ([]domain.Tenant, error)
	UpdateTenant(ctx context.Context, t *domain.Tenant) error
	UpdateTenantPlan(ctx context.Context, tenantID, newPlan string, applyQuotaDefaults bool) error
	OverrideTenantQuota(ctx context.Context, req QuotaOverrideRequest) (*domain.TenantWithQuota, error)
	DeleteTenant(ctx context.Context, id string) error
	// EnableTenant clears tenants.disabled_at for a tenant that
	// SetDisabledAt previously stamped (issue #440). The handler's
	// 409 message references this endpoint.
	EnableTenant(ctx context.Context, tenantID string) error
}

// QuotaOverrideRequest is the input to the admin quota-override
// endpoint (issue #420). All fields are optional — a missing field
// leaves the corresponding row untouched. The handler validates the
// shape (RFC3339 timestamps, non-negative ints) before reaching the
// service.
type QuotaOverrideRequest struct {
	TenantID                 string
	MaxRequestsPerMonth      *int
	MaxOutboundMB            *int
	MaxDeployments           *int
	SetOverageAllowedUntil   *time.Time
	ClearOverageAllowedUntil bool
	ClearDisabledAt          bool
	ClearGrace               bool
}

// Package-level interfaces for testability. The concrete
// *repository.* types satisfy these interfaces structurally.

// tenantRepoForTenantSvc is the subset of *repository.TenantRepository
// methods used by TenantService.
type tenantRepoForTenantSvc interface {
	WithTx(tx *sqlx.Tx) *repository.TenantRepository
	GetByID(ctx context.Context, id string) (*domain.Tenant, error)
	Create(ctx context.Context, tenant *domain.Tenant) error
	Update(ctx context.Context, tenant *domain.Tenant) error
	Delete(ctx context.Context, id string) error
	List(ctx context.Context) ([]domain.Tenant, error)
	SetOverageAllowedUntil(ctx context.Context, tenantID string, at time.Time) error
	ClearOverageAllowedUntil(ctx context.Context, tenantID string) error
	ClearDisabledAt(ctx context.Context, tenantID string) error
}

// quotaRepoForTenantSvc is the subset of *repository.QuotaRepository
// methods used by TenantService.
type quotaRepoForTenantSvc interface {
	WithTx(tx *sqlx.Tx) *repository.QuotaRepository
	GetByTenantID(ctx context.Context, tenantID string) (*domain.Quota, error)
	Create(ctx context.Context, quota *domain.Quota) error
	Update(ctx context.Context, q *domain.Quota) error
	SetGraceUntil(ctx context.Context, tenantID string, until *time.Time) error
}

// apiKeyRepoForTenantSvc is the subset of *repository.APIKeyRepository
// methods used by TenantService.
type apiKeyRepoForTenantSvc interface {
	WithTx(tx *sqlx.Tx) *repository.APIKeyRepository
	Create(ctx context.Context, k *domain.APIKey) error
}

// TenantService handles tenant business logic.
type TenantService struct {
	db            *sqlx.DB
	tenantRepo    tenantRepoForTenantSvc
	quotaRepo     quotaRepoForTenantSvc
	apiKeyRepo    apiKeyRepoForTenantSvc
	appRepo       *repository.AppRepository
	outboxRepo    *repository.OutboxRepository
	defaultRegion string
}

func NewTenantService(db *sqlx.DB, tenantRepo tenantRepoForTenantSvc, quotaRepo quotaRepoForTenantSvc, apiKeyRepo apiKeyRepoForTenantSvc, appRepo *repository.AppRepository, outboxRepo *repository.OutboxRepository, defaultRegion string) *TenantService {
	if defaultRegion == "" {
		defaultRegion = "global"
	}
	return &TenantService{
		db:            db,
		tenantRepo:    tenantRepo,
		quotaRepo:     quotaRepo,
		apiKeyRepo:    apiKeyRepo,
		appRepo:       appRepo,
		outboxRepo:    outboxRepo,
		defaultRegion: defaultRegion,
	}
}

// CreateTenant creates a new tenant with default quota atomically.
// The plan argument must be a known plan name (see domain.IsValidPlan); an
// unknown plan returns domain.ErrUnknownPlan wrapped.
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

		quota, err := domain.QuotaForPlan(plan)
		if err != nil {
			return err
		}
		quota.TenantID = tenant.ID
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

		quota, err := domain.QuotaForPlan(plan)
		if err != nil {
			return err
		}
		quota.TenantID = tenant.ID
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

// GetQuotaForInternal is the read endpoint the edge-ingress quota
// fetcher polls (issue #420). Returns the same shape as GetQuota; the
// "internal" name signals the caller is a trusted service (validated
// by the InternalAuth middleware) so we can skip the tenant-context
// lookup and the row-level authorization that GetQuota requires.
//
// over_cap is computed from max_* vs used_* in the handler — the
// service stays a thin pass-through. subscription_status is out of
// scope for this PR (it's not part of the per-tenant quota cap); the
// field is reserved in the OpenAPI spec for a follow-up that adds
// the billing repo dependency to TenantService.
func (s *TenantService) GetQuotaForInternal(ctx context.Context, tenantID string) (*domain.Quota, error) {
	return s.quotaRepo.GetByTenantID(ctx, tenantID)
}

func (s *TenantService) ListTenants(ctx context.Context) ([]domain.Tenant, error) {
	return s.tenantRepo.List(ctx)
}

func (s *TenantService) UpdateTenant(ctx context.Context, t *domain.Tenant) error {
	return s.tenantRepo.Update(ctx, t)
}

// UpdateTenantPlan changes a tenant's plan and, by default, reapplies the
// per-tier quota defaults so the new plan's ceilings take effect immediately.
//
// When applyQuotaDefaults is true, every Max* column on the quotas row is
// overwritten with domain.QuotaForPlan(newPlan). The used_outbound_bytes,
// used_request_count, and quota_period_start columns are NOT touched —
// existing usage in the current billing period carries over to the new plan.
//
// When applyQuotaDefaults is false, only tenants.plan is updated; the quotas
// row is left untouched (useful when an admin has hand-tuned per-tenant limits
// and only wants to flip the plan label).
//
// Returns domain.ErrUnknownPlan when newPlan is not a known tier,
// ErrTenantNotFound / ErrQuotaNotFound when the corresponding row is
// missing — both mappable to HTTP 404 by the handler.
func (s *TenantService) UpdateTenantPlan(ctx context.Context, tenantID, newPlan string, applyQuotaDefaults bool) error {
	if !domain.IsValidPlan(newPlan) {
		return fmt.Errorf("%w: %q", domain.ErrUnknownPlan, newPlan)
	}

	return repository.Transaction(ctx, s.db, func(tx *sqlx.Tx) error {
		tenantRepo := s.tenantRepo.WithTx(tx)
		quotaRepo := s.quotaRepo.WithTx(tx)

		tenant, err := tenantRepo.GetByID(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("getting tenant: %w", err)
		}
		if tenant == nil {
			return ErrTenantNotFound
		}
		tenant.Plan = newPlan
		if err := tenantRepo.Update(ctx, tenant); err != nil {
			return fmt.Errorf("updating tenant: %w", err)
		}

		if !applyQuotaDefaults {
			return nil
		}

		quota, err := quotaRepo.GetByTenantID(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("getting quota: %w", err)
		}
		if quota == nil {
			return ErrQuotaNotFound
		}
		newDefaults, err := domain.QuotaForPlan(newPlan)
		if err != nil {
			return err
		}
		newDefaults.TenantID = tenantID
		if err := quotaRepo.Update(ctx, &newDefaults); err != nil {
			return fmt.Errorf("updating quota: %w", err)
		}
		return nil
	})
}

// DeleteTenant removes a tenant and enqueues a per-app task_purge
// for each app the tenant owns. The purge tombstone flows through
// the same issue #42 outbox pipeline that activate uses, so the
// worker `Supervisor::handle_task_message` clears the per-tenant
// KV / cache / scheduling state for every app (issue #569).
//
// Atomicity: the tenant row delete AND the per-app outbox rows
// enqueue run inside a single transaction. If tenantRepo.Delete
// fails, no outbox row is written — workers never receive a
// phantom purge for a tenant whose CP-side rows still exist.
//
// Multi-region: today the per-app purge row targets the CP's
// own region (cfg.Region). When multi-region ships, the
// pq.StringArray{s.defaultRegion} line is the seam — wire it
// to the same regions lookup that DeploymentService uses.
func (s *TenantService) DeleteTenant(ctx context.Context, id string) error {
	return repository.Transaction(ctx, s.db, func(tx *sqlx.Tx) error {
		// List apps BEFORE deleting the tenant — the FK cascade
		// from `tenants` → `apps` removes the rows inside
		// tenantRepo.Delete below, so we need the names now to
		// build the purge payloads. List runs against the same
		// tx so it sees a consistent snapshot.
		apps, err := s.appRepo.WithTx(tx).List(ctx, id, 1000, "")
		if err != nil {
			return fmt.Errorf("listing apps for purge: %w", err)
		}

		if err := s.tenantRepo.WithTx(tx).Delete(ctx, id); err != nil {
			return fmt.Errorf("deleting tenant: %w", err)
		}

		outboxRepo := s.outboxRepo.WithTx(tx)
		for _, app := range apps {
			payload, err := json.Marshal(nats.PurgePayload{
				Type:      nats.TaskMessageKindTaskPurge,
				Timestamp: time.Now().UTC(),
				TenantID:  id,
				AppName:   app.Name,
				Reason:    nats.PurgeReasonTenantOffboarded,
			})
			if err != nil {
				return fmt.Errorf("marshaling tenant purge payload: %w", err)
			}
			if err := outboxRepo.Enqueue(ctx, &repository.OutboxRow{
				TenantID:  id,
				AppName:   app.Name,
				Kind:      nats.TaskMessageKindTaskPurge,
				Payload:   payload,
				Regions:   pq.StringArray{s.defaultRegion},
				DedupeKey: "purge:" + id + ":" + app.Name + ":" + uuid.NewString(),
			}); err != nil {
				return fmt.Errorf("enqueueing tenant task_purge: %w", err)
			}
		}
		return nil
	})
}

// EnableTenant clears tenants.disabled_at for a tenant that
// SetDisabledAt previously stamped (issue #440). Called from the
// owner-only admin endpoint POST /api/v1/admin/tenants/{id}/enable.
//
// Returns the wrapped sentinel service.ErrTenantNotFound if no such
// tenant exists; the handler maps it to 404. Calling on an
// already-enabled tenant is a no-op (ClearDisabledAt is idempotent —
// it writes NULL whether or not disabled_at was previously set).
func (s *TenantService) EnableTenant(ctx context.Context, tenantID string) error {
	existing, err := s.tenantRepo.GetByID(ctx, tenantID)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("%w: %s", ErrTenantNotFound, tenantID)
	}
	return s.tenantRepo.ClearDisabledAt(ctx, tenantID)
}

// GetByID is the thin pass-through that satisfies the narrow
// `handler.tenantGetter` contract used by POST /api/internal/tokens/tenant
// (issue #491). No business logic — the JWT-mint handler only needs
// the raw row so it can detect "tenant not found" / "tenant disabled"
// upstream of the heavy per-request signing path. Keeps TenantService
// the single interface for tenant row reads; the alternative
// (exposing the underlying tenantRepo at the app composition layer)
// would duplicate the wiring in two places.
//
// Translates the repo's (nil, nil) on missing-row into
// service.ErrTenantNotFound so callers can use a single `errors.Is`
// check — `repository.TenantRepository.GetByID` returns `(nil, nil)`
// for not-found (matches the pattern of `tenantRepo.GetForUpdate`),
// but every service-level method on this struct returns the wrapped
// sentinel. Without the translation, callers would have to nil-check
// the row alongside `errors.Is(err, ErrTenantNotFound)`, which is
// exactly the foot-gun PR #491 review flagged: a handler that calls
// `tenant.IsDisabled()` on a nil tenant panics on the typo path.
func (s *TenantService) GetByID(ctx context.Context, id string) (*domain.Tenant, error) {
	t, err := s.tenantRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, fmt.Errorf("%w: %s", ErrTenantNotFound, id)
	}
	return t, nil
}

// OverrideTenantQuota is the manual recovery path for issue #420.
// Operator-only; the handler is mounted under RequireRole("owner")
// and audit-logs every call. Each non-nil field is applied; nil
// fields leave the row untouched. Returns the post-override
// TenantWithQuota so the operator can verify the change in the
// response. Does NOT touch the heartbeat counters (used_*) — those
// are the actual consumption signal and should only move via
// heartbeats.
func (s *TenantService) OverrideTenantQuota(ctx context.Context, req QuotaOverrideRequest) (*domain.TenantWithQuota, error) {
	if req.TenantID == "" {
		return nil, fmt.Errorf("tenant id required")
	}
	tenant, err := s.tenantRepo.GetByID(ctx, req.TenantID)
	if err != nil {
		return nil, fmt.Errorf("getting tenant: %w", err)
	}
	if tenant == nil {
		return nil, ErrTenantNotFound
	}
	quota, err := s.quotaRepo.GetByTenantID(ctx, req.TenantID)
	if err != nil {
		return nil, fmt.Errorf("getting quota: %w", err)
	}
	if quota == nil {
		return nil, ErrQuotaNotFound
	}

	// Quota row updates: only the max_* columns, never the used_*
	// counters. The UPDATE is a thin pass-through — sqlx scans
	// directly into the domain struct; if any of the optional
	// fields is nil we keep the prior value.
	if req.MaxRequestsPerMonth != nil {
		quota.MaxRequestsPerMonth = *req.MaxRequestsPerMonth
	}
	if req.MaxOutboundMB != nil {
		quota.MaxOutboundMB = *req.MaxOutboundMB
	}
	if req.MaxDeployments != nil {
		quota.MaxDeployments = *req.MaxDeployments
	}
	if err := s.quotaRepo.Update(ctx, quota); err != nil {
		return nil, fmt.Errorf("updating quota: %w", err)
	}

	// Grace-clock clear (free-tier first-cross grace).
	if req.ClearGrace {
		if err := s.quotaRepo.SetGraceUntil(ctx, req.TenantID, nil); err != nil {
			return nil, fmt.Errorf("clearing grace: %w", err)
		}
		quota.QuotaLockGraceUntil = nil
	}

	// Tenant row: overage grace + disabled_at clear.
	if req.SetOverageAllowedUntil != nil {
		if err := s.tenantRepo.SetOverageAllowedUntil(ctx, req.TenantID, *req.SetOverageAllowedUntil); err != nil {
			return nil, fmt.Errorf("setting overage grace: %w", err)
		}
		tenant.OverageAllowedUntil = req.SetOverageAllowedUntil
	}
	if req.ClearOverageAllowedUntil {
		if err := s.tenantRepo.ClearOverageAllowedUntil(ctx, req.TenantID); err != nil {
			return nil, fmt.Errorf("clearing overage grace: %w", err)
		}
		tenant.OverageAllowedUntil = nil
	}
	if req.ClearDisabledAt {
		if err := s.tenantRepo.ClearDisabledAt(ctx, req.TenantID); err != nil {
			return nil, fmt.Errorf("clearing disabled_at: %w", err)
		}
		tenant.DisabledAt = nil
	}

	// Re-read the post-override state so the response payload is
	// canonical. Cheap and avoids drift between the in-memory
	// struct above and what's on disk (a partial failure would
	// otherwise be invisible to the operator).
	freshTenant, err := s.tenantRepo.GetByID(ctx, req.TenantID)
	if err != nil || freshTenant == nil {
		return nil, fmt.Errorf("re-reading tenant: %w", err)
	}
	freshQuota, err := s.quotaRepo.GetByTenantID(ctx, req.TenantID)
	if err != nil || freshQuota == nil {
		return nil, fmt.Errorf("re-reading quota: %w", err)
	}
	return &domain.TenantWithQuota{Tenant: *freshTenant, Quota: *freshQuota}, nil
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
// Passing an empty slice clears the list; on the wire the worker receives an absent
// or empty `allowlist` field, which its serde deserializer maps to None → allow_all().
func (s *TenantService) UpdateEgressAllowlist(ctx context.Context, tenantID string, allowlist []string) error {
	// Normalize to lowercase before validation and storage. EgressPolicy::check
	// compares against the lowercased hostname produced by url::Url::parse; storing
	// mixed-case entries would cause silent 403s for those hosts.
	normalized := make([]string, len(allowlist))
	for i, e := range allowlist {
		if strings.HasPrefix(e, "*.") {
			normalized[i] = "*." + strings.ToLower(e[2:])
		} else {
			normalized[i] = strings.ToLower(e)
		}
	}
	allowlist = normalized
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
