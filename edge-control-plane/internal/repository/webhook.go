package repository

import (
	"context"
	"database/sql"

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
