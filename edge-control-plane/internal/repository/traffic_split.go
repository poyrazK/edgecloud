package repository

import (
	"context"

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

// SetTrafficSplits atomically replaces all traffic split rows for an app.
// Implemented as a package-level function rather than a method on
// TrafficSplitRepository because `Transaction` (the package's shared
// BeginTxx/Rollback/Commit helper) takes a *sqlx.DB, and a DBTX-method
// would have to type-assert to BeginTxx — exactly the pattern that
// broke WithTx composition in earlier revisions (the assertion succeeds
// for *sqlx.Tx too, so `WithTx(tx).Set(...)` would silently start a
// nested transaction instead of joining the parent).
//
// Callers needing Set inside an existing transaction should compose the
// operations at the service level (a Tx-scoped repository built via
// WithTx can run DeleteAllForApp + a manual INSERT in the parent tx).
func SetTrafficSplits(ctx context.Context, db *sqlx.DB, splits []*domain.TrafficSplit) error {
	if len(splits) == 0 {
		return nil
	}
	return Transaction(ctx, db, func(tx *sqlx.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM app_traffic_splits WHERE tenant_id = $1 AND app_name = $2`,
			splits[0].TenantID, splits[0].AppName); err != nil {
			return err
		}
		for _, s := range splits {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO app_traffic_splits (tenant_id, app_name, deployment_id, weight) VALUES ($1, $2, $3, $4)`,
				s.TenantID, s.AppName, s.DeploymentID, s.Weight); err != nil {
				return err
			}
		}
		return nil
	})
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
