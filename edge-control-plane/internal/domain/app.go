package domain

import (
	"time"
)

// App represents a registered application.
type App struct {
	ID             string    `db:"id"`
	TenantID       string    `db:"tenant_id"`
	Name           string    `db:"name"`
	Description    *string   `db:"description"` // nullable
	CreatedAt      time.Time `db:"created_at"`
	RateLimitRPS   int       `db:"rate_limit_rps"    json:"rate_limit_rps"`
	RateLimitBurst int       `db:"rate_limit_burst"  json:"rate_limit_burst"`
}

// AppRateLimit is the rate limit override for a single app.
// Returned by the internal rate-limits endpoint for the ingress fetcher.
type AppRateLimit struct {
	RPS   int `json:"rps"`
	Burst int `json:"burst"`
}

// CreateAppRequest is sent when creating a new app.
type CreateAppRequest struct {
	Description string `json:"description"` // optional
}

// UpdateAppRequest is sent when updating an existing app.
// nil pointer fields mean "don't change"; non-nil means "set to this value".
type UpdateAppRequest struct {
	Description *string `json:"description"` // nil = no change, "" = clear
}
