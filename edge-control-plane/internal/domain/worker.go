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
	IP        *string   `db:"ip"`   // nil means NULL in DB
	MemoryMB  int       `db:"memory_mb"`
	LastSeen  time.Time `db:"last_seen"`
	CreatedAt time.Time `db:"created_at"`
}

// RegisterWorkerRequest is sent by a worker when registering with the control plane.
type RegisterWorkerRequest struct {
	WorkerID string `json:"worker_id"` // e.g. "w_fra_<uuid>"
	Region   string `json:"region"`    // e.g. "fra"
	IP       string `json:"ip"`       // optional, worker's IP address
	MemoryMB int    `json:"memory_mb"` // optional, default 4096
}

// WorkerStatus holds the running app state reported by a worker.
type WorkerStatus struct {
	WorkerID   string          `db:"worker_id"`
	Apps       json.RawMessage `db:"apps"`  // { app_name: { status, exit_code, deployment_id } }
	LastReport time.Time       `db:"last_report"`
}

// AppStatus represents the status of a single app on a worker.
type AppStatus struct {
	Status        string `json:"status"`
	ExitCode      int    `json:"exit_code"`
	DeploymentID  string `json:"deployment_id"`
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
