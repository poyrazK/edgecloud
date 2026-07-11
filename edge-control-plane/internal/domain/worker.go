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
	// PublicKey is the Ed25519 public key the worker enrolled during the
	// /worker-bootstrap/enroll handshake (issue #430). Hex-encoded (64
	// ASCII chars, lowercase). nil for pre-#430 workers — those workers
	// never enrolled and the cluster rejects their bootstrap attempts.
	// Persisted by WorkerRepository.SetPublicKey after the handshake
	// succeeds; read by the WorkerAuth wkr_-kid verification path and
	// by the worker_key_cache middleware.
	PublicKey *string   `db:"public_key"`
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

// MetricKind is the kind of metric in a MetricSample.
type MetricKind string

const (
	MetricKindCounter         MetricKind = "counter"
	MetricKindGauge           MetricKind = "gauge"
	MetricKindHistogramSample MetricKind = "histogram_sample"
)

// MetricSample is a single metric observation shipped inside a heartbeat.
type MetricSample struct {
	Name   string      `json:"name"`
	Kind   MetricKind  `json:"kind"`
	Value  float64     `json:"value"`
	Labels [][2]string `json:"labels"`
}

// AppStatus represents the status of a single app on a worker.
type AppStatus struct {
	Status       string `json:"status"`
	ExitCode     int    `json:"exit_code"`
	DeploymentID string `json:"deployment_id"`
	// Number of HTTP requests handled since the last heartbeat interval.
	RequestCount uint64 `json:"request_count"`
	// Total outbound bytes since the last heartbeat interval.
	// Absent from old workers; defaults to 0 via json omitempty absence — treat
	// 0 as "no data" rather than "zero traffic" when deciding quota actions.
	OutboundBytes uint64 `json:"outbound_bytes"`
	// Tenant the app belongs to. Sourced from the worker's `tenant_id`
	// (the worker can host apps from multiple tenants).
	TenantID string `json:"tenant_id,omitempty"`
	// Port the app's HTTP server is listening on, on the worker host.
	// Sourced from `AppInstance.port` in the worker; used by the public
	// ingress to dial the upstream.
	Port int `json:"port,omitempty"`
	// Guest-emitted metrics from edge:observe. Absent from old workers;
	// defaults to nil — control plane treats nil as "no metric data this interval".
	ObserverMetrics []MetricSample `json:"observer_metrics,omitempty"`
	// DedupeID is the idempotency token the worker stamps on each heartbeat
	// (issue #418). Stable across redeliveries within the same
	// `(worker_id, deployment_id, 30s_bucket)` tuple. The metering pipeline
	// caches recently-seen IDs and skips re-applying the same delta when
	// JetStream or reconcile replay delivers a duplicate heartbeat. Absent
	// on pre-#418 workers — control planes that don't see this field fall
	// back to the historical behaviour (apply every delivery).
	DedupeID string `json:"dedupe_id,omitempty"`
	// ResidentSeconds is the total resident seconds since the last
	// heartbeat interval. nil for Handler (FaaS) apps that do not
	// contribute; Some(&0) for a LongRunning app that started within
	// the current interval. Used by service.WorkerService.checkResidentSeconds
	// to accumulate into quotas.used_resident_seconds (issue #484 /
	// #485, third metered dimension). Nil for pre-#484 workers; control
	// planes that don't see this field translate to "no contribution"
	// (delta=0, no AddResidentSeconds call).
	ResidentSeconds *uint64 `json:"resident_seconds,omitempty"`
	// DurationMsTotal is the total elapsed wall-clock milliseconds
	// across all FaaS requests served by this Handler app since the
	// last heartbeat interval (issue #555, fourth metered dimension).
	// nil for pre-#555 workers (LongRunning apps always contribute 0
	// because the dispatch path never stamps for LR — the worker
	// sends 0 on the wire which Go decodes as a non-nil pointer to 0).
	// Used by service.WorkerService.checkComputeMs to accumulate into
	// quotas.used_compute_ms.
	DurationMsTotal *uint64 `json:"duration_ms_total,omitempty"`
}

// ResidentSecondsOrZero returns the resident-seconds counter treated as a
// scalar: nil is folded to 0 so a Handler (FaaS) app or a pre-#484 worker
// contributes nothing to applyTenantDelta. This is the field selector
// passed into applyTenantDelta; it must remain `func(*AppStatus) uint64`
// (not a closure) so callers can use the value as a method expression.
func (a *AppStatus) ResidentSecondsOrZero() uint64 {
	if a.ResidentSeconds == nil {
		return 0
	}
	return *a.ResidentSeconds
}

// DurationMsTotalOrZero returns the FaaS duration counter treated as a
// scalar: nil is folded to 0 so a pre-#555 worker or a LongRunning app
// contributes nothing to applyTenantDelta. Mirrors ResidentSecondsOrZero
// (issue #484) but for the fourth metered dimension. Same method-value
// discipline applies — the field selector passed into applyTenantDelta
// must remain `func(*AppStatus) uint64` (not a closure) so callers can
// use the value as a method expression.
func (a *AppStatus) DurationMsTotalOrZero() uint64 {
	if a.DurationMsTotal == nil {
		return 0
	}
	return *a.DurationMsTotal
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

// AppWorkerStatus is the tenant-facing projection of one app's current
// worker-reported status. Returned by GET /api/v1/apps/{appName}/status.
//
// `Status` is the same string the worker publishes in NATS heartbeats
// (see edge-worker/src/supervisor.rs build_heartbeat), projected to
// tenants verbatim so they can match on the documented set:
//
//	"running" | "starting" | "stopping" | "crashed" | "hung" | "unknown"
//
// `"unknown"` is the zero-value returned when no worker has reported on
// this app yet (the JSONB blob in `worker_status` does not contain a
// key for `appName`). It distinguishes "no data" from "last reported
// `running` 3s ago" — both would be a bare `""` otherwise.
//
// `LastHeartbeat` is nil when no worker has ever reported on the app;
// tenants can use the age of this timestamp to detect a dead worker
// (no TTL is enforced server-side; see the staleness note in
// WorkerStatusHandler.Get). `Region` and `WorkerID` are also empty
// in that case.
//
// The `db` tags are for sqlx scan; the `json` tags are the wire format.
type AppWorkerStatus struct {
	AppName       string     `db:"app_name"       json:"app_name"`
	Status        string     `db:"status"          json:"status"`
	DeploymentID  string     `db:"deployment_id"   json:"deployment_id"`
	LastHeartbeat *time.Time `db:"last_heartbeat"  json:"last_heartbeat"`
	Region        string     `db:"region"          json:"region"`
	WorkerID      string     `db:"worker_id"       json:"worker_id"`
	ExitCode      *int32     `db:"exit_code"       json:"exit_code,omitempty"`
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
