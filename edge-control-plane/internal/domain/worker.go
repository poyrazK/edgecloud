package domain

import (
	"encoding/json"
	"strings"
	"time"
)

// Worker represents a registered worker supervisor node.
type Worker struct {
	ID        string    `db:"id"`
	TenantID  string    `db:"tenant_id"`
	Region    string    `db:"region"`
	IP        *string   `db:"ip"` // nil means NULL in DB
	MemoryMB  int       `db:"memory_mb"`
	LastSeen  time.Time `db:"last_seen"`
	CreatedAt time.Time `db:"created_at"`
}

// RegisterWorkerRequest is sent by a worker when registering with the control plane.
type RegisterWorkerRequest struct {
	WorkerID string `json:"worker_id"` // e.g. "w_fra_<uuid>"
	Region   string `json:"region"`    // e.g. "fra"
	IP       string `json:"ip"`        // optional, worker's IP address
	MemoryMB int    `json:"memory_mb"` // optional, default 4096
}

// WorkerStatus holds the running app state reported by a worker.
type WorkerStatus struct {
	WorkerID   string          `db:"worker_id"`
	Apps       json.RawMessage `db:"apps"` // { app_name: { status, exit_code, deployment_id } }
	LastReport time.Time       `db:"last_report"`
}

// AppStatus represents the status of a single app on a worker.
type AppStatus struct {
	Status       string `json:"status"`
	ExitCode     int    `json:"exit_code"`
	DeploymentID string `json:"deployment_id"`
	// Tenant the app belongs to. Sourced from the worker's `tenant_id`
	// (the worker can host apps from multiple tenants).
	TenantID string `json:"tenant_id,omitempty"`
	// Port the app's HTTP server is listening on, on the worker host.
	// Sourced from `AppInstance.port` in the worker; used by the public
	// ingress to dial the upstream.
	Port int `json:"port,omitempty"`
}

// AppTarget describes a running app reachable on a worker — what the
// public ingress needs to render a route. Extracted from the JSONB apps
// blob on `worker_status` joined with `workers.ip`.
type AppTarget struct {
	AppName    string `json:"app_name"`
	TenantID   string `json:"tenant_id"`
	WorkerID   string `json:"worker_id"`
	Region     string `json:"region"`
	WorkerAddr string `json:"worker_addr"`
	Port       int    `json:"port"`
}

// IsValidWorkerID checks that worker ID matches the format w_<region>_<uuid>.
// The whitepaper specifies: Worker IDs are validated with format w_<region>_<uuid>.
func IsValidWorkerID(id string) bool {
	if len(id) < 6 || id[0] != 'w' || id[1] != '_' {
		return false
	}
	rest := id[2:]
	parts := strings.SplitN(rest, "_", 2)
	if len(parts) != 2 {
		return false
	}
	return len(parts[0]) > 0 && len(parts[1]) > 0
}

// IngressHostSuffix is the wildcard DNS suffix the public ingress serves.
// MUST stay in sync with the Rust ingress's INGRESS_HOST_SUFFIX constant
// (edge-ingress/src/config.rs) and the hostname rendered by the ingress's
// Caddyfile JSON. A drift between the two produces 404s for every public
// URL the control plane has advertised to tenants.
const IngressHostSuffix = "edgecloud.dev"

// IngressHost returns the public hostname for a (tenant, app) pair.
// Example: IngressHost("t_acme", "api") == "t_acme-api.edgecloud.dev".
func IngressHost(tenantID, appName string) string {
	return tenantID + "-" + appName + "." + IngressHostSuffix
}
