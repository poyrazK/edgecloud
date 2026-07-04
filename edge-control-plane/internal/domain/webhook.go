package domain

import (
	"time"

	"github.com/lib/pq"
)

// Webhook represents a tenant-managed webhook subscription.
type Webhook struct {
	ID          string         `db:"id" json:"id"`
	TenantID    string         `db:"tenant_id" json:"tenant_id"`
	URL         string         `db:"url" json:"url"`
	Secret      string         `db:"secret" json:"-"`
	Events      pq.StringArray `db:"events" json:"events"`
	Description string         `db:"description" json:"description"`
	Enabled     bool           `db:"enabled" json:"enabled"`
	CreatedAt   time.Time      `db:"created_at" json:"created_at"`
}

// WebhookEvent is the JSON payload POSTed to a webhook URL.
type WebhookEvent struct {
	EventType string      `json:"event_type"`
	TenantID  string      `json:"tenant_id"`
	AppName   string      `json:"app_name"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

// WebhookDelivery records one delivery attempt for observability.
type WebhookDelivery struct {
	ID           int64      `db:"id" json:"id,omitempty"`
	WebhookID    string     `db:"webhook_id" json:"webhook_id"`
	EventType    string     `db:"event_type" json:"event_type"`
	Status       string     `db:"status" json:"status"`
	StatusCode   *int       `db:"status_code" json:"status_code,omitempty"`
	RequestBody  string     `db:"request_body" json:"-"`
	ResponseBody string     `db:"response_body" json:"-"`
	ErrorMsg     string     `db:"error_msg" json:"error_msg,omitempty"`
	Attempt      int        `db:"attempt" json:"attempt"`
	MaxAttempts  int        `db:"max_attempts" json:"max_attempts"`
	CreatedAt    time.Time  `db:"created_at" json:"created_at"`
	CompletedAt  *time.Time `db:"completed_at" json:"completed_at,omitempty"`
}

// Valid webhook event types.
const (
	WebhookEventDeploy       = "deploy"
	WebhookEventActivate     = "activate"
	WebhookEventRollback     = "rollback"
	WebhookEventAutoRollback = "auto_rollback"
)

// ValidWebhookEvents returns all valid event types.
var ValidWebhookEvents = []string{
	WebhookEventDeploy,
	WebhookEventActivate,
	WebhookEventRollback,
	WebhookEventAutoRollback,
}

// IsValidWebhookEvent checks if a string is a valid event type.
func IsValidWebhookEvent(e string) bool {
	for _, v := range ValidWebhookEvents {
		if e == v {
			return true
		}
	}
	return false
}
