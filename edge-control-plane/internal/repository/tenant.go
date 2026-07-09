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
	query := `SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at FROM tenants WHERE id = $1`
	err := r.db.GetContext(ctx, &t, query, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &t, err
}

func (r *TenantRepository) List(ctx context.Context) ([]domain.Tenant, error) {
	var tenants []domain.Tenant
	query := `SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at FROM tenants ORDER BY created_at DESC`
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
	query := `SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at FROM tenants WHERE disabled_at IS NULL ORDER BY created_at DESC`
	err := r.db.SelectContext(ctx, &tenants, query)
	return tenants, err
}

// GetForUpdate reads the tenants row for `tenantID` under a row-level
// `SELECT … FOR UPDATE` lock. The lock is held until the surrounding
// `repository.Transaction` commits or rolls back, so the caller has
// exclusive write access to the disabled_at column for the duration.
//
// Issue #440 serializes ActivateDeployment vs the disable path through
// this lock: ActivateDeployment acquires it inside its tx and rejects
// the activation when disabled_at is non-nil; the disable path acquires
// it before SetDisabledAt + notifyDisableTenant so a racing activate
// either commits before disable (disable's post-commit active-deployments
// diff sees the fresh row and skips the empty task_update publish) or
// blocks waiting for the lock (the activate then observes disabled_at
// != nil and returns ErrTenantDisabled). Mirrors the pattern on
// AppRepository.GetForUpdate and ActiveDeploymentRepository.GetForUpdate.
//
// Returns (nil, nil) when no tenant exists; callers map that to
// ErrTenantNotFound.
func (r *TenantRepository) GetForUpdate(ctx context.Context, tenantID string) (*domain.Tenant, error) {
	var t domain.Tenant
	query := `SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at FROM tenants WHERE id = $1 FOR UPDATE`
	err := r.db.GetContext(ctx, &t, query, tenantID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &t, err
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
	query := `SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at FROM tenants WHERE disabled_at IS NOT NULL ORDER BY disabled_at DESC`
	err := r.db.SelectContext(ctx, &tenants, query)
	return tenants, err
}
