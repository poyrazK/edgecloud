package domain

import (
	"time"

	"github.com/lib/pq"
)

// Deployment represents a deployed Wasm artifact.
type Deployment struct {
	ID       string `db:"id"`
	TenantID string `db:"tenant_id"`
	AppName  string `db:"app_name"`
	Status   string `db:"status"`
	Hash     string `db:"hash"` // SHA-256 of Wasm payload
	// Regions is the list of regions this deployment is replicated to.
	// The activate path loops over this list and publishes one
	// `TaskMessage` per region to `edgecloud.tasks.<region>`. An empty
	// slice (e.g. for rows created before migration 008) means
	// "use the control plane's default region" — the service layer
	// resolves the fallback. See `service.ActivateDeployment`.
	//
	// Typed as pq.StringArray (which is `[]string` underneath) so the
	// `TEXT[]` column scans correctly via lib/pq's Scanner — a bare
	// `[]string` does NOT implement `sql.Scanner` and would fail on
	// SELECT. The JSON wire format is unchanged because
	// pq.StringArray marshals identically to []string.
	//
	// No `omitempty`: an empty slice serializes as `[]`, which is
	// more useful for clients than `null` and matches the codebase
	// convention of not using `omitempty` on domain structs.
	Regions   pq.StringArray `db:"regions" json:"regions"`
	CreatedAt time.Time      `db:"created_at"`
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
