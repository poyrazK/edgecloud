package service

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// MaxDomainsPerApp caps the number of custom domains a single app can
// claim. Defensive ceiling against abuse; realistic tenants want ≤5.
// Operators can raise this constant if needed. Mirrors the pattern set
// by `MaxRegionsPerDeployment` (issue #82).
const MaxDomainsPerApp = 50

// edgecloudDevSuffix is the wildcard host suffix the platform manages
// (`<tenant>-<app>.edgecloud.dev`). Custom domains cannot share this
// suffix because they'd collide with the synthetic hostname namespace;
// rejecting it here is defense-in-depth on top of the UNIQUE constraint
// on `domains.fqdn`.
const edgecloudDevSuffix = ".edgecloud.dev"

// fqdnPattern is the RFC 1035-ish shape we accept. Each label is
// 1-63 chars, `[a-z0-9-]`, no leading/trailing hyphen, lowercase only
// (DNS is case-insensitive but case-sensitive operators cause
// `tls-allowed` lookup misses and `edge domains check` confusion). We
// reject wildcards (`*`) because v1 only does single-FQDN HTTP-01 ACME;
// wildcard support requires DNS-01 and is deferred to v2.
//
// The regex is intentionally NOT anchored to a max total length — that's
// a separate length check below so the error message can name the
// offending value.
var fqdnPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$`)

// IsValidFQDN returns true if the FQDN shape is acceptable. Rejects:
//   - empty strings,
//   - strings longer than 253 chars (DNS hard limit),
//   - any character outside `[a-z0-9.-]` (uppercase, whitespace, `*`,
//     `..`, leading/trailing dot, etc.),
//   - labels starting or ending with a hyphen,
//   - labels longer than 63 chars (DNS hard limit),
//   - FQDNs ending in `.edgecloud.dev` (platform-managed host).
//
// Modeled on `IsValidAppName` / `IsValidRegion`. The service layer
// rejects invalid FQDNs before they reach the DB or the ingress poller.
func IsValidFQDN(fqdn string) bool {
	if fqdn == "" || len(fqdn) > 253 {
		return false
	}
	if strings.HasSuffix(fqdn, edgecloudDevSuffix) {
		return false
	}
	if strings.Contains(fqdn, "*") {
		return false
	}
	return fqdnPattern.MatchString(fqdn)
}

// Sentinel errors.
//
// The handler matches these via errors.Is and maps them to HTTP status
// codes (400, 404, 429 respectively).
//
// `ErrAppNotFound` is defined in app.go (it's the cross-service "app
// does not exist" sentinel); we wrap it via `%w` so handlers can match.
var (
	ErrInvalidFQDN         = errors.New("invalid fqdn")
	ErrDomainNotFound      = errors.New("domain not found")
	ErrDomainQuotaExceeded = errors.New("too many domains for this app")
)

// appLookupForDomain is the narrow contract DomainService needs to
// lock an (tenant, app) row from inside a transaction. The `WithTx`
// method returns a new instance bound to a *sqlx.Tx so the
// `SELECT … FOR UPDATE` runs on the same transaction (and the row
// lock is held until commit/rollback) — this is what closes the
// count-then-insert race in AddDomain.
//
// In production this is implemented by `*repository.AppRepository`,
// which already exposes WithTx + GetForUpdate. We keep it as an
// interface here so handler/service tests can substitute a mock
// without standing up the full AppRepository + Postgres graph.
//
// The WithTx return type is the concrete *repository.AppRepository
// (matching the existing pattern in the repository package's
// other repos).
type appLookupForDomain interface {
	WithTx(tx *sqlx.Tx) *repository.AppRepository
	Get(ctx context.Context, tenantID, appName string) (*domain.App, error)
	// GetForUpdate locks the (tenant, app) row for the lifetime of the
	// surrounding tx so concurrent domain inserts against the same
	// app serialize on the parent row — closing the count-then-insert
	// race that would otherwise let two callers at count==cap-1 both
	// insert and overshoot MaxDomainsPerApp. Returns (nil, nil) when
	// the app does not exist; service maps that to ErrAppNotFound.
	GetForUpdate(ctx context.Context, tenantID, appName string) (*domain.App, error)
}

// DomainRepositoryInterface is the narrow contract DomainService needs
// from DomainRepository. Mirrors the pattern in worker.go / app.go.
//
// `WithTx` returns a *repository.DomainRepository (the concrete
// type, not the interface) — this matches the existing pattern in
// *repository.AppRepository, *repository.DeploymentRepository, and
// *repository.ActiveDeploymentRepository. The receiver re-uses
// the interface methods on the tx-bound instance.
type DomainRepositoryInterface interface {
	WithTx(tx *sqlx.Tx) *repository.DomainRepository
	Create(ctx context.Context, d *domain.Domain) error
	GetByID(ctx context.Context, id string) (*domain.Domain, error)
	GetByFQDN(ctx context.Context, fqdn string) (*domain.Domain, error)
	ListByApp(ctx context.Context, tenantID, appName string) ([]domain.Domain, error)
	CountByApp(ctx context.Context, tenantID, appName string) (int, error)
	ListAll(ctx context.Context) ([]domain.Domain, error)
	AtomicDelete(ctx context.Context, tenantID, appName, fqdn string) (bool, error)
	UpdateStatus(ctx context.Context, id string, status domain.DomainStatus, lastError *string) (bool, error)
}

// DomainService handles custom-domain business logic.
type DomainService struct {
	db         *sqlx.DB
	domainRepo DomainRepositoryInterface
	appLookup  appLookupForDomain
}

// NewDomainService creates a new DomainService. The `db` is the
// shared *sqlx.DB so AddDomain can wrap count+insert in a single
// transaction (see AddDomain's doc comment). The `appLookup`
// dependency is the (tenant, app) row-lock provider — production
// passes `*repository.AppRepository`, which already exposes
// WithTx + GetForUpdate. The `*AppService` wrapper is intentionally
// NOT used here: AddDomain needs the FOR UPDATE to run on the same
// tx as the count + insert, and a *AppService pass-through would
// re-enter the shared *sqlx.DB (a different physical connection)
// which would release the row lock immediately.
func NewDomainService(db *sqlx.DB, domainRepo *repository.DomainRepository, appLookup *repository.AppRepository) *DomainService {
	return &DomainService{
		db:         db,
		domainRepo: domainRepo,
		appLookup:  appLookup,
	}
}

// AddDomain validates the FQDN shape, ensures the (tenant, app) exists,
// enforces the per-app quota, and inserts the row in `pending` state.
// The ingress's 30s poller picks up the new row on its next tick.
//
// Returns ErrInvalidFQDN, ErrDomainQuotaExceeded, or an unwrapped DB
// error. Callers (handlers, tests) match via errors.Is.
//
// Concurrency: the per-app quota (`MaxDomainsPerApp = 50`) is enforced
// inside a transaction that takes `SELECT … FOR UPDATE` on the parent
// `apps` row, mirroring the pattern in
// `service/deployment.go::ActivateDeployment`. Two concurrent
// `AddDomain` calls targeting the same (tenant, app) therefore
// serialize on the parent row — the second one observes the first's
// inserted count and (if it pushes the count to or past the cap)
// returns ErrDomainQuotaExceeded. The lock is released on commit
// or rollback.
//
// `GetForUpdate` returns (nil, nil) when no app exists; we map that
// to ErrAppNotFound. The error mapping happens inside the tx so a
// non-existent app doesn't leave the lock hanging (the rollback
// releases it).
func (s *DomainService) AddDomain(ctx context.Context, tenantID, appName, fqdn string) (*domain.Domain, error) {
	if !IsValidFQDN(fqdn) {
		return nil, fmt.Errorf("%w %q: must be RFC-1035 shape, lowercase, ≤253 chars, no wildcard, no .edgecloud.dev suffix", ErrInvalidFQDN, fqdn)
	}

	var d *domain.Domain
	if err := repository.Transaction(ctx, s.db, func(tx *sqlx.Tx) error {
		// Bind the appLookup to the surrounding tx so the
		// FOR UPDATE on apps(tenant_id, name) runs on the
		// same connection. The lock is held until commit/rollback.
		txApp := s.appLookup.WithTx(tx)
		app, err := txApp.GetForUpdate(ctx, tenantID, appName)
		if err != nil {
			return fmt.Errorf("locking app row: %w", err)
		}
		if app == nil {
			return fmt.Errorf("%w: %s", ErrAppNotFound, appName)
		}

		txRepo := s.domainRepo.WithTx(tx)
		count, err := txRepo.CountByApp(ctx, tenantID, appName)
		if err != nil {
			return fmt.Errorf("counting domains: %w", err)
		}
		if count >= MaxDomainsPerApp {
			return fmt.Errorf("%w: %d (max %d)", ErrDomainQuotaExceeded, count, MaxDomainsPerApp)
		}

		d = &domain.Domain{
			ID:        "dom_" + uuid.New().String(),
			TenantID:  tenantID,
			AppName:   appName,
			FQDN:      fqdn,
			Status:    domain.DomainStatusPending,
			CreatedAt: time.Now(),
		}
		if err := txRepo.Create(ctx, d); err != nil {
			return fmt.Errorf("creating domain: %w", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return d, nil
}

// ListDomains returns all domains for a (tenant, app). Returns an empty
// slice (not nil, not an error) when the app has no domains — matches
// the existing pattern in DeploymentRepository.ListByApp.
func (s *DomainService) ListDomains(ctx context.Context, tenantID, appName string) ([]domain.Domain, error) {
	return s.domainRepo.ListByApp(ctx, tenantID, appName)
}

// GetDomain returns a single domain by (tenant, app, fqdn). Returns
// (nil, ErrDomainNotFound) when no row matches; the handler maps that
// to 404.
func (s *DomainService) GetDomain(ctx context.Context, tenantID, appName, fqdn string) (*domain.Domain, error) {
	d, err := s.domainRepo.GetByFQDN(ctx, fqdn)
	if err != nil {
		return nil, fmt.Errorf("looking up domain: %w", err)
	}
	if d == nil || d.TenantID != tenantID || d.AppName != appName {
		return nil, ErrDomainNotFound
	}
	return d, nil
}

// RemoveDomain deletes the row matching (tenant, app, fqdn). Returns
// ErrDomainNotFound when no row matched (the row may have been deleted
// by a concurrent request or the tenant is targeting the wrong (app,
// fqdn) pair). The 30s poller on the ingress will pick up the deletion
// on its next tick and drop the FQDN from its routing table.
func (s *DomainService) RemoveDomain(ctx context.Context, tenantID, appName, fqdn string) error {
	deleted, err := s.domainRepo.AtomicDelete(ctx, tenantID, appName, fqdn)
	if err != nil {
		return fmt.Errorf("deleting domain: %w", err)
	}
	if !deleted {
		return ErrDomainNotFound
	}
	return nil
}

// IsTlsAllowed answers Caddy's `on_demand.ask` query: should I issue a
// cert for this FQDN? Returns true iff a row exists for the FQDN in
// either `pending` or `active` state. `failed` rows do NOT authorize
// issuance (a previous ACME failure means re-trying the same cert
// would just fail again — the operator needs to fix the upstream
// issue first).
//
// The cascading FK added in 011_domains_cascade.up.sql (PR #133
// review finding #4) ensures that when an app is deleted, its
// domain rows are removed in the same transaction — so this method
// correctly returns false (no row → 404 from the handler) instead
// of authorizing TLS issuance for a hostname whose app no longer
// exists. Pinned by
// `handler.TestInternal_TlsAllowed_AppDeletionCascadesToDomainRow`.
func (s *DomainService) IsTlsAllowed(ctx context.Context, fqdn string) (bool, error) {
	d, err := s.domainRepo.GetByFQDN(ctx, fqdn)
	if err != nil {
		return false, fmt.Errorf("looking up fqdn: %w", err)
	}
	if d == nil {
		return false, nil
	}
	return d.Status == domain.DomainStatusPending || d.Status == domain.DomainStatusActive, nil
}

// ListAllDomains returns every domain row across all tenants. Used by
// the ingress's `GET /api/internal/domains` poll endpoint. JWT-protected
// at the handler layer; the service trusts its caller.
func (s *DomainService) ListAllDomains(ctx context.Context) ([]domain.Domain, error) {
	return s.domainRepo.ListAll(ctx)
}

// GetDomainByID returns a domain by its primary key. Used by the v2
// Caddy event hook (which only sees the row id). Returns (nil, nil)
// when no row matches — the handler maps that to 404.
func (s *DomainService) GetDomainByID(ctx context.Context, id string) (*domain.Domain, error) {
	return s.domainRepo.GetByID(ctx, id)
}

// UpdateStatus updates the status (and optionally last_error) of a
// domain row. Used by the v2 Caddy event hook via
// `POST /api/internal/domains/{id}/status`. v1 has no callers.
//
// Returns ErrDomainNotFound when no row matched the id — the handler
// maps that to 404. A future caller that wants to silently no-op
// instead of surfacing a 404 must opt in explicitly.
func (s *DomainService) UpdateStatus(ctx context.Context, id string, status domain.DomainStatus, lastError *string) error {
	ok, err := s.domainRepo.UpdateStatus(ctx, id, status, lastError)
	if err != nil {
		return fmt.Errorf("updating domain status: %w", err)
	}
	if !ok {
		return ErrDomainNotFound
	}
	return nil
}
