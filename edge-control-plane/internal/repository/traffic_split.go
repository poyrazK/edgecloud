package repository

import (
	"context"
	"database/sql"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// TrafficSplitRepository handles traffic split rows.
type TrafficSplitRepository struct {
	db DBTX
}

func NewTrafficSplitRepository(db *sqlx.DB) *TrafficSplitRepository {
	return &TrafficSplitRepository{db: db}
}

// WithTx returns a new TrafficSplitRepository using the provided transaction.
func (r *TrafficSplitRepository) WithTx(tx *sqlx.Tx) *TrafficSplitRepository {
	return &TrafficSplitRepository{db: tx}
}

// Set atomically replaces all traffic split rows for a given app.
// It deletes all existing rows for (tenant_id, app_name) then inserts
// the new ones in a single transaction.
func (r *TrafficSplitRepository) Set(ctx context.Context, splits []*domain.TrafficSplit) error {
	if len(splits) == 0 {
		return nil
	}
	tx, err := r.db.(interface {
		BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)
	}).BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Delete existing rows for this app.
	_, err = tx.ExecContext(ctx,
		`DELETE FROM app_traffic_splits WHERE tenant_id = $1 AND app_name = $2`,
		splits[0].TenantID, splits[0].AppName)
	if err != nil {
		return err
	}

	// Insert new rows.
	for _, s := range splits {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO app_traffic_splits (tenant_id, app_name, deployment_id, weight) VALUES ($1, $2, $3, $4)`,
			s.TenantID, s.AppName, s.DeploymentID, s.Weight)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// Get returns all traffic split rows for a given app.
func (r *TrafficSplitRepository) Get(ctx context.Context, tenantID, appName string) ([]*domain.TrafficSplit, error) {
	var splits []*domain.TrafficSplit
	query := `SELECT tenant_id, app_name, deployment_id, weight, created_at FROM app_traffic_splits WHERE tenant_id = $1 AND app_name = $2 ORDER BY created_at ASC`
	err := r.db.SelectContext(ctx, &splits, query, tenantID, appName)
	if err != nil {
		return nil, err
	}
	return splits, nil
}

// DeleteAllForApp removes every traffic split row for a given app.
func (r *TrafficSplitRepository) DeleteAllForApp(ctx context.Context, tenantID, appName string) error {
	_, err := r.db.ExecContext(ctx,
		`DELETE FROM app_traffic_splits WHERE tenant_id = $1 AND app_name = $2`,
		tenantID, appName)
	return err
}
