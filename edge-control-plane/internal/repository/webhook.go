package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// WebhookRepository handles webhook and webhook_deliveries persistence.
type WebhookRepository struct {
	db DBTX
}

func NewWebhookRepository(db *sqlx.DB) *WebhookRepository {
	return &WebhookRepository{db: db}
}

func (r *WebhookRepository) Create(ctx context.Context, wh *domain.Webhook) error {
	const q = `INSERT INTO webhooks (id, tenant_id, url, secret, events, description, enabled, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`
	_, err := r.db.ExecContext(ctx, q,
		wh.ID, wh.TenantID, wh.URL, wh.Secret, wh.Events,
		wh.Description, wh.Enabled, wh.CreatedAt)
	return err
}

func (r *WebhookRepository) GetByID(ctx context.Context, id string) (*domain.Webhook, error) {
	var wh domain.Webhook
	const q = `SELECT id, tenant_id, url, secret, events, description, enabled, created_at FROM webhooks WHERE id = $1`
	err := r.db.GetContext(ctx, &wh, q, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &wh, err
}

func (r *WebhookRepository) ListByTenant(ctx context.Context, tenantID string) ([]domain.Webhook, error) {
	var whs []domain.Webhook
	const q = `SELECT id, tenant_id, url, secret, events, description, enabled, created_at FROM webhooks WHERE tenant_id = $1 ORDER BY created_at DESC`
	err := r.db.SelectContext(ctx, &whs, q, tenantID)
	return whs, err
}

func (r *WebhookRepository) ListEnabledByTenantAndEvent(ctx context.Context, tenantID, eventType string) ([]domain.Webhook, error) {
	var whs []domain.Webhook
	const q = `SELECT id, tenant_id, url, secret, events, description, enabled, created_at FROM webhooks
		WHERE tenant_id = $1 AND enabled = true AND $2 = ANY(events)
		ORDER BY created_at ASC`
	err := r.db.SelectContext(ctx, &whs, q, tenantID, eventType)
	return whs, err
}

func (r *WebhookRepository) Update(ctx context.Context, wh *domain.Webhook) error {
	const q = `UPDATE webhooks SET url=$2, secret=$3, events=$4, description=$5, enabled=$6 WHERE id=$1 AND tenant_id=$7`
	_, err := r.db.ExecContext(ctx, q,
		wh.ID, wh.URL, wh.Secret, wh.Events,
		wh.Description, wh.Enabled, wh.TenantID)
	return err
}

func (r *WebhookRepository) Delete(ctx context.Context, id, tenantID string) (bool, error) {
	var deleted bool
	err := r.db.QueryRowxContext(ctx,
		`DELETE FROM webhooks WHERE id = $1 AND tenant_id = $2 RETURNING true`,
		id, tenantID).Scan(&deleted)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return deleted, err
}

func (r *WebhookRepository) InsertDelivery(ctx context.Context, d *domain.WebhookDelivery) (int64, error) {
	const q = `INSERT INTO webhook_deliveries
		(webhook_id, event_type, status, status_code, request_body, response_body, error_msg, attempt, max_attempts, created_at, completed_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id`
	var id int64
	err := r.db.QueryRowxContext(ctx, q,
		d.WebhookID, d.EventType, d.Status, d.StatusCode, d.RequestBody,
		d.ResponseBody, d.ErrorMsg, d.Attempt, d.MaxAttempts,
		d.CreatedAt, d.CompletedAt).Scan(&id)
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
// service.WebhookDeliveryGCService (issue #574); the GC refuses to run
// with non-positive retention, so this guard is belt-and-suspenders.
//
// Index path: the (created_at) index added in migration
// 031_gc_retention_indexes covers the WHERE created_at < … predicate.
// The (webhook_id, created_at DESC) index from migration 015 remains
// for the per-webhook delivery history endpoint (if/when one ships);
// this method does not use it.
//
// Note: webhook_deliveries.webhook_id has ON DELETE CASCADE to
// webhooks, but a retention sweep of webhook_deliveries only touches
// the deliveries side — no FK impact.
func (r *WebhookRepository) DeleteOlderThanBatched(
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
			`DELETE FROM webhook_deliveries WHERE id IN (SELECT id FROM webhook_deliveries WHERE created_at < NOW() - make_interval(secs => $1) LIMIT $2)`,
			retention.Seconds(), int64(batchSize))
		if err != nil {
			return total, fmt.Errorf("deleting old webhook_deliveries (batch %d): %w", i, err)
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
