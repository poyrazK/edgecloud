package repository

import (
	"context"

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
