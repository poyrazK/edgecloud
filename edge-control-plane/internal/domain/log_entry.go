package domain

import (
	"encoding/json"
	"time"
)

// LogEntry is one tenant log record — written by a worker via
// POST /api/internal/logs (issue #76) and surfaced to operators via the
// upcoming GET /api/logs/{appName} (issue #77).
//
// The control plane is the source of truth for TenantID, WorkerID, Region,
// and TS: the handler overwrites TenantID from the JWT, stamps WorkerID/Region
// from the JWT, and lets TS default to NOW() on insert. DeploymentID, AppName,
// Level, Message, and Labels come from the request body as supplied by the
// guest (via edge:observe.emit_log on the worker).
type LogEntry struct {
	ID           int64           `db:"id" json:"id,omitempty"`
	TenantID     string          `db:"tenant_id" json:"tenant_id"`
	DeploymentID string          `db:"deployment_id" json:"deployment_id"`
	AppName      string          `db:"app_name" json:"app_name"`
	WorkerID     string          `db:"worker_id" json:"worker_id"`
	Region       string          `db:"region" json:"region"`
	Level        string          `db:"level" json:"level"` // trace | debug | info | warn | error
	Message      string          `db:"message" json:"message"`
	Labels       json.RawMessage `db:"labels" json:"labels"`
	TS           time.Time       `db:"ts" json:"ts"`
}
