package repository

import (
	"context"
	"database/sql"

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
	query := `SELECT id, name, plan, allowlisted_destinations, created_at FROM tenants WHERE id = $1`
	err := r.db.GetContext(ctx, &t, query, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &t, err
}

func (r *TenantRepository) List(ctx context.Context) ([]domain.Tenant, error) {
	var tenants []domain.Tenant
	query := `SELECT id, name, plan, allowlisted_destinations, created_at FROM tenants ORDER BY created_at DESC`
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
