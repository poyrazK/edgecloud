package domain

import (
	"time"
)

// App represents a registered application.
type App struct {
	ID          string    `db:"id"`
	TenantID    string    `db:"tenant_id"`
	Name        string    `db:"name"`
	Description *string   `db:"description"` // nullable
	CreatedAt   time.Time `db:"created_at"`
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
