package repository

import (
	"context"
	"database/sql"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// DomainRepository handles custom-domain data access (issue #83).
//
// All queries are scoped by tenant_id or app_name to keep tenants from
// observing each other's rows. The one exception is `GetByFQDN` and
// `ListAll`, which are the ingress poller's hot path and need cross-tenant
// visibility — both are JWT-protected at the handler layer.
type DomainRepository struct {
	db DBTX
}

func NewDomainRepository(db *sqlx.DB) *DomainRepository {
	return &DomainRepository{db: db}
}

// NewDomainRepositoryFromDBTX constructs a repository bound to a DBTX
// (either *sqlx.DB or *sqlx.Tx). Used by tests that need a tx-bound
// repo without standing up a full database connection.
func NewDomainRepositoryFromDBTX(dbtx DBTX) *DomainRepository {
	return &DomainRepository{db: dbtx}
}

// WithTx returns a new DomainRepository using the provided transaction.
func (r *DomainRepository) WithTx(tx *sqlx.Tx) *DomainRepository {
	return &DomainRepository{db: tx}
}

// Create inserts a new domain row. Status is taken from the struct
// (defaulting to "pending" via the schema if the caller leaves it empty,
// though the service layer always sets it explicitly).
func (r *DomainRepository) Create(ctx context.Context, d *domain.Domain) error {
	query := `INSERT INTO domains (id, tenant_id, app_name, fqdn, status, created_at)
	          VALUES ($1, $2, $3, $4, $5, $6)`
	_, err := r.db.ExecContext(ctx, query, d.ID, d.TenantID, d.AppName, d.FQDN, d.Status, d.CreatedAt)
	return err
}

// GetByID returns the row or (nil, nil) when no row matches.
func (r *DomainRepository) GetByID(ctx context.Context, id string) (*domain.Domain, error) {
	var d domain.Domain
	query := `SELECT id, tenant_id, app_name, fqdn, status, last_error, created_at, verified_at
	          FROM domains WHERE id = $1`
	err := r.db.GetContext(ctx, &d, query, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &d, err
}

// GetByFQDN is the hot path for Caddy's on-demand `tls-allowed` ask URL.
// The UNIQUE constraint on `fqdn` provides the index; no redundant index
// is created in the schema.
func (r *DomainRepository) GetByFQDN(ctx context.Context, fqdn string) (*domain.Domain, error) {
	var d domain.Domain
	query := `SELECT id, tenant_id, app_name, fqdn, status, last_error, created_at, verified_at
	          FROM domains WHERE fqdn = $1`
	err := r.db.GetContext(ctx, &d, query, fqdn)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &d, err
}

// ListByApp returns all domains for a (tenant, app). Used by the tenant
// handler GET endpoint.
func (r *DomainRepository) ListByApp(ctx context.Context, tenantID, appName string) ([]domain.Domain, error) {
	var domains []domain.Domain
	query := `SELECT id, tenant_id, app_name, fqdn, status, last_error, created_at, verified_at
	          FROM domains WHERE tenant_id = $1 AND app_name = $2 ORDER BY created_at DESC`
	err := r.db.SelectContext(ctx, &domains, query, tenantID, appName)
	return domains, err
}

// CountByApp returns the number of domains for a (tenant, app). Used to
// enforce `MaxDomainsPerApp`.
func (r *DomainRepository) CountByApp(ctx context.Context, tenantID, appName string) (int, error) {
	var count int
	query := `SELECT COUNT(*) FROM domains WHERE tenant_id = $1 AND app_name = $2`
	err := r.db.GetContext(ctx, &count, query, tenantID, appName)
	return count, err
}

// ListAll returns every domain row across all tenants. Used by the
// ingress's `GET /api/internal/domains` poll endpoint. Domain counts are
// small (per-tenant quotas cap them), so an unpaginated SELECT is fine
// for v1. If this becomes a bottleneck in v2, switch to a streaming or
// paginated shape.
func (r *DomainRepository) ListAll(ctx context.Context) ([]domain.Domain, error) {
	var domains []domain.Domain
	query := `SELECT id, tenant_id, app_name, fqdn, status, last_error, created_at, verified_at
	          FROM domains ORDER BY created_at`
	err := r.db.SelectContext(ctx, &domains, query)
	return domains, err
}

// AtomicDelete removes the row matching (tenant_id, app_name, fqdn)
// and returns whether a row was deleted. Pattern matches
// `AppRepository.AtomicDelete`. The service layer maps a `false` return
// to ErrDomainNotFound so handlers can return 404.
func (r *DomainRepository) AtomicDelete(ctx context.Context, tenantID, appName, fqdn string) (bool, error) {
	var deleted bool
	err := r.db.GetContext(ctx, &deleted,
		`DELETE FROM domains WHERE tenant_id = $1 AND app_name = $2 AND fqdn = $3 RETURNING true`,
		tenantID, appName, fqdn)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return deleted, err
}

// UpdateStatus updates the status (and optionally last_error) for a domain.
// Called by the v2 Caddy webhook via `POST /api/internal/domains/{id}/status`.
// v1 never calls this from anywhere; it's exposed for completeness so the
// service doesn't need to be updated when the webhook lands.
//
// Returns `false` (with no error) when no row matches the id, so the
// handler can render a 404 instead of a misleading 204. Without this
// distinction a stale id would silently look like success — see the
// review on PR #133.
func (r *DomainRepository) UpdateStatus(ctx context.Context, id string, status domain.DomainStatus, lastError *string) (bool, error) {
	query := `UPDATE domains SET status = $2, last_error = $3 WHERE id = $1`
	res, err := r.db.ExecContext(ctx, query, id, status, lastError)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}
