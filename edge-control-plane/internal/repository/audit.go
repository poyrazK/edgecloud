package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// AuditRepository handles audit event persistence.
type AuditRepository struct {
	db DBTX
}

func NewAuditRepository(db *sqlx.DB) *AuditRepository {
	return &AuditRepository{db: db}
}

// Insert appends one audit event. Returns the auto-generated ID.
func (r *AuditRepository) Insert(ctx context.Context, e *domain.AuditEvent) (int64, error) {
	const q = `INSERT INTO audit_logs
		(tenant_id, api_key_id, role, action, resource, resource_id, details, outcome, error_msg, request_ip)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		RETURNING id`
	var id int64
	err := r.db.QueryRowxContext(ctx, q,
		e.TenantID, e.APIKeyID, e.Role, e.Action, e.Resource,
		e.ResourceID, e.Details, e.Outcome, e.ErrorMsg, e.RequestIP,
	).Scan(&id)
	return id, err
}

// DeleteOlderThanBatched deletes up to `batchSize` rows whose created_at
// is older than `retention` (server-side: NOW() - retention), looping
// until either the DB has no more matching rows or `maxBatches` is hit.
// Returns the total rows deleted across all batches.
//
// Paginated shape mirrors LogEntryRepository.DeleteOlderThanBatched —
// see that method's doc-comment for the lock-duration and clock-skew
// rationale. The retention GC driving this method is
// service.AuditGCService (issue #574); the GC refuses to run with
// non-positive retention, so this guard is belt-and-suspenders.
//
// Index path: the (created_at) index added in migration
// 031_gc_retention_indexes covers the WHERE created_at < … predicate.
// The (tenant_id, created_at DESC) and (resource, resource_id,
// created_at DESC) indexes from migration 014 remain in place for
// existing read paths; this method does not use them.
func (r *AuditRepository) DeleteOlderThanBatched(
	ctx context.Context, retention time.Duration, batchSize, maxBatches int,
) (int64, error) {
	if retention <= 0 {
		return 0, fmt.Errorf("retention must be positive, got %s", retention)
	}
	const cap = 10_000
	if batchSize <= 0 || batchSize > cap {
		batchSize = cap
	}
	if maxBatches <= 0 {
		maxBatches = 1
	}

	var total int64
	for i := 0; i < maxBatches; i++ {
		if ctx.Err() != nil {
			return total, ctx.Err()
		}
		res, err := r.db.ExecContext(ctx,
			`DELETE FROM audit_logs WHERE id IN (SELECT id FROM audit_logs WHERE created_at < NOW() - make_interval(secs => $1) LIMIT $2)`,
			retention.Seconds(), int64(batchSize))
		if err != nil {
			return total, fmt.Errorf("deleting old audit_logs (batch %d): %w", i, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, fmt.Errorf("rows affected (batch %d): %w", i, err)
		}
		total += n
		if n < int64(batchSize) {
			break
		}
	}
	return total, nil
}
