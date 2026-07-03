package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// addColumnAccumulators is the whitelist of columns addColumn will accept.
// The column name is interpolated into SQL via fmt.Sprintf AFTER this
// whitelist check, so no caller-supplied SQL fragment can reach the query.
// Adding a new usage counter means (a) adding the column here and
// (b) writing a thin public wrapper below.
var addColumnAccumulators = map[string]bool{
	"used_outbound_bytes": true,
	"used_request_count":  true,
}

// addColumn atomically adds delta to one of the per-month usage counters on
// the quotas row, with a lazy month rollover against quota_period_start (the
// counter resets if the stored period is in a past UTC month). Used by
// AddOutboundBytes and AddRequestCount — the public wrappers are the only
// callers.
func (r *QuotaRepository) addColumn(ctx context.Context, tenantID string, delta int64, col string) (*domain.Quota, error) {
	if !addColumnAccumulators[col] {
		return nil, fmt.Errorf("quota repo: refusing to add to non-allowlisted column %q", col)
	}
	var q domain.Quota
	// #nosec G201 — col is whitelisted above; never caller-supplied SQL.
	query := fmt.Sprintf(`
		UPDATE quotas SET
			%s = CASE
				WHEN date_trunc('month', quota_period_start AT TIME ZONE 'UTC')
				     < date_trunc('month', now() AT TIME ZONE 'UTC')
				THEN $2
				ELSE %s + $2
			END,
			quota_period_start = CASE
				WHEN date_trunc('month', quota_period_start AT TIME ZONE 'UTC')
				     < date_trunc('month', now() AT TIME ZONE 'UTC')
				THEN date_trunc('month', now() AT TIME ZONE 'UTC')
				ELSE quota_period_start
			END
		WHERE tenant_id = $1
		RETURNING tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, used_outbound_bytes, used_request_count, quota_period_start`, col, col)
	err := r.db.GetContext(ctx, &q, query, tenantID, delta)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &q, err
}

// QuotaRepository handles quota data access.
type QuotaRepository struct {
	db DBTX
}

func NewQuotaRepository(db *sqlx.DB) *QuotaRepository {
	return &QuotaRepository{db: db}
}

// WithTx returns a new QuotaRepository using the provided transaction.
func (r *QuotaRepository) WithTx(tx *sqlx.Tx) *QuotaRepository {
	return &QuotaRepository{db: tx}
}

func (r *QuotaRepository) Create(ctx context.Context, q *domain.Quota) error {
	query := `INSERT INTO quotas (tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month) VALUES ($1, $2, $3, $4, $5, $6, $7)`
	_, err := r.db.ExecContext(ctx, query, q.TenantID, q.MaxDeployments, q.MaxApps, q.MaxWorkers, q.MaxMemoryMB, q.MaxOutboundMB, q.MaxRequestsPerMonth)
	return err
}

func (r *QuotaRepository) GetByTenantID(ctx context.Context, tenantID string) (*domain.Quota, error) {
	var q domain.Quota
	query := `SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, used_outbound_bytes, used_request_count, quota_period_start FROM quotas WHERE tenant_id = $1`
	err := r.db.GetContext(ctx, &q, query, tenantID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &q, err
}

// AddOutboundBytes atomically accumulates delta into used_outbound_bytes and
// returns the updated quota row. When the stored quota_period_start is in a
// past calendar month (UTC), the counter and period are reset first so the
// monthly cap applies to the current month only — no separate cron required.
func (r *QuotaRepository) AddOutboundBytes(ctx context.Context, tenantID string, delta uint64) (*domain.Quota, error) {
	return r.addColumn(ctx, tenantID, int64(delta), "used_outbound_bytes")
}

// AddRequestCount atomically accumulates delta into used_request_count and
// returns the updated quota row. Mirrors AddOutboundBytes: a lazy month
// rollover against quota_period_start resets the counter when the stored
// period is in a past calendar month (UTC). Used by
// service.WorkerService.checkRequestCount on every heartbeat.
func (r *QuotaRepository) AddRequestCount(ctx context.Context, tenantID string, delta uint64) (*domain.Quota, error) {
	return r.addColumn(ctx, tenantID, int64(delta), "used_request_count")
}

func (r *QuotaRepository) Update(ctx context.Context, q *domain.Quota) error {
	query := `UPDATE quotas SET max_deployments = $2, max_apps = $3, max_workers = $4, max_memory_mb = $5, max_outbound_mb = $6, max_requests_per_month = $7 WHERE tenant_id = $1`
	_, err := r.db.ExecContext(ctx, query, q.TenantID, q.MaxDeployments, q.MaxApps, q.MaxWorkers, q.MaxMemoryMB, q.MaxOutboundMB, q.MaxRequestsPerMonth)
	return err
}