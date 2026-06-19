package domain

import (
	"time"
)

// Deployment represents a deployed Wasm artifact.
type Deployment struct {
	ID        string    `db:"id"`
	TenantID  string    `db:"tenant_id"`
	AppName   string    `db:"app_name"`
	Status    string    `db:"status"`
	Hash      string    `db:"hash"` // SHA-256 of Wasm payload
	CreatedAt time.Time `db:"created_at"`
}

// Deployment status constants.
const (
	StatusDeployed = "deployed"
	StatusActive   = "active"
	StatusFailed   = "failed"
	StatusMigrated = "migrated"
)

// ActiveDeployment maps an app name to its active deployment for a tenant.
type ActiveDeployment struct {
	TenantID     string `db:"tenant_id"`
	AppName      string `db:"app_name"`
	DeploymentID string `db:"deployment_id"`
}

// AppEnv stores environment variables for an app.
type AppEnv struct {
	TenantID string `db:"tenant_id"`
	AppName  string `db:"app_name"`
	EnvKey   string `db:"env_key"`
	EnvValue string `db:"env_value"`
}
