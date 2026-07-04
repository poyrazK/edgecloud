package domain

import "time"

// AuditEvent records one state-changing API action.
// Append-only — rows are never updated or deleted.
type AuditEvent struct {
	ID         int64     `db:"id" json:"id,omitempty"`
	TenantID   string    `db:"tenant_id" json:"tenant_id"`
	APIKeyID   string    `db:"api_key_id" json:"api_key_id"`
	Role       string    `db:"role" json:"role"`
	Action     string    `db:"action" json:"action"`
	Resource   string    `db:"resource" json:"resource"`
	ResourceID string    `db:"resource_id" json:"resource_id"`
	Details    string    `db:"details" json:"details"`
	Outcome    string    `db:"outcome" json:"outcome"`
	ErrorMsg   string    `db:"error_msg" json:"error_msg"`
	RequestIP  string    `db:"request_ip" json:"request_ip"`
	CreatedAt  time.Time `db:"created_at" json:"created_at"`
}
