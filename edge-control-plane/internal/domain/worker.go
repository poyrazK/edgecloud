package domain

import (
	"encoding/json"
	"time"
)

// Worker represents a registered worker supervisor node.
type Worker struct {
	ID        string    `db:"id"`
	Region    string    `db:"region"`
	IP        *string   `db:"ip"`   // nil means NULL in DB
	MemoryMB  int       `db:"memory_mb"`
	LastSeen  time.Time `db:"last_seen"`
	CreatedAt time.Time `db:"created_at"`
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
