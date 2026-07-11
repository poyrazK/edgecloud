package repository

import (
	"context"
	"database/sql"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// TenantRepository handles tenant data access.
type TenantRepository struct {
	db DBTX
}

func NewTenantRepository(db *sqlx.DB) *TenantRepository {
	return &TenantRepository{db: db}
}

// NewTenantRepositoryFromDBTX constructs a TenantRepository
// bound to a DBTX — useful for tests that need to run inside
// a sqlmock-backed *sqlx.Tx without exposing a real DB
// connection. Mirrors NewAppRepositoryFromDBTX at app.go.
func NewTenantRepositoryFromDBTX(dbtx DBTX) *TenantRepository {
	return &TenantRepository{db: dbtx}
}

// WithTx returns a new TenantRepository using the provided transaction.
func (r *TenantRepository) WithTx(tx *sqlx.Tx) *TenantRepository {
	return &TenantRepository{db: tx}
}

func (r *TenantRepository) Create(ctx context.Context, t *domain.Tenant) error {
	query := `INSERT INTO tenants (id, name, plan, allowlisted_destinations) VALUES ($1, $2, $3, $4)`
	// pq.Array wraps pq.StringArray so lib/pq encodes it as a
	// Postgres array literal. Without the wrap, a bare []string
	// would be encoded as comma-separated bytes — which Postgres
	// rejects as a malformed array literal. See commit notes for
	// the latent bug this fixed (issue #82 fan-out tests surfaced).
	_, err := r.db.ExecContext(ctx, query, t.ID, t.Name, t.Plan, pq.Array(t.AllowlistedDestinations))
	return err
}

func (r *TenantRepository) GetByID(ctx context.Context, id string) (*domain.Tenant, error) {
	var t domain.Tenant
	query := `SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id = $1`
	err := r.db.GetContext(ctx, &t, query, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &t, err
}

// GetForUpdate is GetByID with a row-level write lock (SELECT ... FOR
// UPDATE). Used by DeploymentService.activateDeployment /
// RollbackDeployment to serialize against concurrent SetDisabledAt /
// ClearDisabledAt — without the lock a disable could land between our
// active_deployments read and the post-commit publishSwap, and the
// worker would receive a TaskMessage for a now-disabled tenant
// (issue #440).
//
// Returns (nil, nil) for sql.ErrNoRows — the same shape as GetByID so
// callers can branch on tenant == nil to distinguish "missing" from
// "real error". The lock is released when the surrounding tx commits
// or rolls back.
func (r *TenantRepository) GetForUpdate(ctx context.Context, id string) (*domain.Tenant, error) {
	var t domain.Tenant
	query := `SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id = $1 FOR UPDATE`
	err := r.db.GetContext(ctx, &t, query, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &t, err
}

func (r *TenantRepository) List(ctx context.Context) ([]domain.Tenant, error) {
	var tenants []domain.Tenant
	query := `SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants ORDER BY created_at DESC`
	err := r.db.SelectContext(ctx, &tenants, query)
	return tenants, err
}

func (r *TenantRepository) Update(ctx context.Context, t *domain.Tenant) error {
	query := `UPDATE tenants SET name = $2, plan = $3, allowlisted_destinations = $4 WHERE id = $1`
	_, err := r.db.ExecContext(ctx, query, t.ID, t.Name, t.Plan, pq.Array(t.AllowlistedDestinations))
	return err
}

func (r *TenantRepository) Delete(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM tenants WHERE id = $1`, id)
	return err
}

// ListActive returns tenants that are not disabled (disabled_at IS NULL).
// Used by ReconcileService to skip disabled tenants when fanning out
// full-state sync messages (issue #155).
func (r *TenantRepository) ListActive(ctx context.Context) ([]domain.Tenant, error) {
	var tenants []domain.Tenant
	query := `SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE disabled_at IS NULL ORDER BY created_at DESC`
	err := r.db.SelectContext(ctx, &tenants, query)
	return tenants, err
}

// SetDisabledAt marks a tenant as disabled at the given timestamp. Called
// when a tenant exceeds their outbound bandwidth quota (issue #155).
func (r *TenantRepository) SetDisabledAt(ctx context.Context, tenantID string, at time.Time) error {
	_, err := r.db.ExecContext(ctx, `UPDATE tenants SET disabled_at = $2 WHERE id = $1`, tenantID, at)
	return err
}

// ClearDisabledAt removes the disabled_at marker. Called when the billing
// period resets, a plan upgrades, or an operator manually re-enables the
// tenant.
func (r *TenantRepository) ClearDisabledAt(ctx context.Context, tenantID string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE tenants SET disabled_at = NULL WHERE id = $1`, tenantID)
	return err
}

// ListDisabled returns tenants that are currently disabled (disabled_at
// IS NOT NULL). Useful for admin views and billing-cycle reset sweeps.
func (r *TenantRepository) ListDisabled(ctx context.Context) ([]domain.Tenant, error) {
	var tenants []domain.Tenant
	query := `SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE disabled_at IS NOT NULL ORDER BY disabled_at DESC`
	err := r.db.SelectContext(ctx, &tenants, query)
	return tenants, err
}

// SetOverageAllowedUntil stamps tenants.overage_allowed_until for a paid
// tenant (issue #420). The deploy-time cap check is skipped while
// now() < overage_allowed_until. Called by the admin quota-override
// endpoint. The grace column is per-tenant; leaving it NULL restores
// the normal cap check.
func (r *TenantRepository) SetOverageAllowedUntil(ctx context.Context, tenantID string, at time.Time) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE tenants SET overage_allowed_until = $2 WHERE id = $1`,
		tenantID, at)
	return err
}

// ClearOverageAllowedUntil removes the per-tenant overage grace marker.
// Called by the admin quota-override endpoint when the operator submits
// `clear_overage_allowed_until: true` (or omits a future timestamp).
func (r *TenantRepository) ClearOverageAllowedUntil(ctx context.Context, tenantID string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE tenants SET overage_allowed_until = NULL WHERE id = $1`, tenantID)
	return err
}
