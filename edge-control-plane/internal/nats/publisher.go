package nats

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/nats-io/nats.go"
)

// Stream name used by both the publisher and the worker for `TaskMessage`s.
// Exposed here so the worker can verify it's subscribing to the same stream.
const TaskStreamName = "edgecloud-tasks"

// Publisher defines the interface for NATS publishing.
type Publisher interface {
	PublishTaskUpdate(region string, msg *TaskMessage) error
	// PublishFullSync publishes the full desired-state snapshot for a
	// (tenant, region). Workers treat it as authoritative: stop any app
	// not in the set, start any missing, restart any whose deployment_id
	// doesn't match. Published periodically by ReconcileService (issue #53)
	// and on worker registration so cold-start is instant.
	PublishFullSync(region string, msg *TaskMessage) error
	PublishHeartbeat(region string, msg *HeartbeatMessage) error
	EnsureStream(cfg StreamConfig) error
}

// TaskMessage is published to edgecloud.tasks.<region> when app set changes.
type TaskMessage struct {
	Type      string               `json:"type"`
	Timestamp time.Time            `json:"timestamp"`
	TenantID  string               `json:"tenant_id"`
	Apps      map[string]AppConfig `json:"apps"`
}

// AppConfig describes an app's deployment configuration.
type AppConfig struct {
	DeploymentID   string            `json:"deployment_id"`
	DeploymentHash string            `json:"deployment_hash"`
	Routes         []DeploymentRoute `json:"routes,omitempty"` // populated when canary splits are active
	Env            map[string]string `json:"env"`
	Allowlist      []string          `json:"allowlist"`
	MaxMemoryMB    int               `json:"max_memory_mb"`
}

// DeploymentRoute describes one deployment's weight in a canary traffic split.
// Workers use this to run multiple deployments of the same app concurrently.
type DeploymentRoute struct {
	DeploymentID   string `json:"deployment_id"`
	DeploymentHash string `json:"deployment_hash"`
	Weight         int    `json:"weight"`
}

// HeartbeatMessage is published by workers to edgecloud.heartbeats.<region>.
//
// This type is publish-only — no code in the repo deserializes into it
// (the consumer in service/worker.go uses an anonymous inline struct
// so it can pass the apps blob through as json.RawMessage to the JSONB
// upsert path). New wire fields should be added here AND mirrored in
// the consumer's anonymous struct.
type HeartbeatMessage struct {
	Type      string                      `json:"type"`
	Timestamp time.Time                   `json:"timestamp"`
	WorkerID  string                      `json:"worker_id"`
	Region    string                      `json:"region"`
	Apps      map[string]domain.AppStatus `json:"apps"`
	// ClusterHeadroom carries capacity info for the autoscaler (issue #85).
	// Optional on the wire so pre-#85 workers (no field) still serialize
	// cleanly through this struct, and a new worker talking to an old
	// control plane has the field silently dropped by the consumer's
	// partial unmarshal — both directions safe.
	//
	// The autoscaler reads `AppSlots` from this block; CPUPct / MemPct are
	// observability-only for now (no sysinfo on the worker yet).
	ClusterHeadroom *ClusterHeadroom `json:"cluster_headroom,omitempty"`
}

// ClusterHeadroom mirrors the Rust `ClusterHeadroom` struct in
// edge-worker/src/messages.rs. AppSlots is the only field the autoscaler
// acts on; the rest are pass-through for future PRs that add
// system-introspection.
type ClusterHeadroom struct {
	CPUPct   *float64 `json:"cpu_pct,omitempty"`
	MemPct   *float64 `json:"mem_pct,omitempty"`
	AppSlots uint32   `json:"app_slots"`
}

// StreamConfig describes a JetStream stream to be created/verified.
type StreamConfig struct {
	Name      string
	Subjects  []string
	Retention nats.RetentionPolicy
	MaxAge    time.Duration
	Replicas  int
}

// applyTypeOverride returns a *TaskMessage with the given `type` field
// set, preserving every other field from the input. Both the real
// NATSPublisher and the MockPublisher call this so the wire shape is
// guaranteed identical regardless of which publisher the operator
// configured — and so the wire-format invariant has a single source of
// truth (the override logic was previously only in
// NATSPublisher.publishTaskMessage, and the mock printed whatever the
// caller passed in; the two would diverge if a caller accidentally set
// `Type: "task_update"` and called PublishFullSync through the mock).
//
// We snapshot rather than mutate so callers who hold a TaskMessage
// pointer don't see their struct modified by the publish call.
func applyTypeOverride(msg *TaskMessage, typeField string) *TaskMessage {
	return &TaskMessage{
		Type:      typeField,
		Timestamp: msg.Timestamp,
		TenantID:  msg.TenantID,
		Apps:      msg.Apps,
	}
}

// MockPublisher is a no-op publisher for development.
type MockPublisher struct{}

func (p *MockPublisher) PublishTaskUpdate(region string, msg *TaskMessage) error {
	data, _ := json.Marshal(applyTypeOverride(msg, "task_update"))
	fmt.Printf("[NATS MOCK] Publish to edgecloud.tasks.%s: %s\n", region, string(data))
	return nil
}

func (p *MockPublisher) PublishFullSync(region string, msg *TaskMessage) error {
	data, _ := json.Marshal(applyTypeOverride(msg, "full_sync"))
	fmt.Printf("[NATS MOCK] Publish to edgecloud.tasks.%s: %s\n", region, string(data))
	return nil
}

func (p *MockPublisher) PublishHeartbeat(region string, msg *HeartbeatMessage) error {
	data, _ := json.Marshal(msg)
	fmt.Printf("[NATS MOCK] Publish to edgecloud.heartbeats.%s: %s\n", region, string(data))
	return nil
}

func (p *MockPublisher) EnsureStream(_ StreamConfig) error {
	return nil
}

// NATSPublisher is a real NATS JetStream publisher.
type NATSPublisher struct {
	nc *nats.Conn
	js nats.JetStreamContext
}

// NewNATSPublisher connects to NATS and returns a JetStream publisher.
func NewNATSPublisher(url string) (*NATSPublisher, error) {
	nc, err := nats.Connect(url,
		nats.Name("edge-cloud-control-plane"),
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS at %s: %w", url, err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("creating JetStream context: %w", err)
	}
	return &NATSPublisher{nc: nc, js: js}, nil
}

// EnsureStream idempotently creates the given JetStream stream. If the
// stream already exists with the same shape (subjects, retention, MaxAge,
// replicas), it's a no-op. If any of those four fields differ, an error
// is returned so the operator can reconcile (issue #86 — workers and
// the control plane must agree on stream shape for the queue-group
// consumer to work).
func (p *NATSPublisher) EnsureStream(cfg StreamConfig) error {
	info, err := p.js.StreamInfo(cfg.Name)
	if err == nil {
		// Stream exists — verify config matches. Any mismatch surfaces
		// loudly so a misconfigured cluster (e.g., RF=1 in dev, RF=3 in
		// prod) can't silently degrade durability.
		if !equalSubjects(info.Config.Subjects, cfg.Subjects) {
			return fmt.Errorf("stream %s already exists with subjects=%v, want %v", cfg.Name, info.Config.Subjects, cfg.Subjects)
		}
		if info.Config.Retention != cfg.Retention {
			return fmt.Errorf("stream %s already exists with retention=%v, want %v", cfg.Name, info.Config.Retention, cfg.Retention)
		}
		if info.Config.MaxAge != cfg.MaxAge {
			return fmt.Errorf("stream %s already exists with MaxAge=%v, want %v", cfg.Name, info.Config.MaxAge, cfg.MaxAge)
		}
		if info.Config.Replicas != cfg.Replicas {
			return fmt.Errorf("stream %s already exists with replicas=%d, want %d", cfg.Name, info.Config.Replicas, cfg.Replicas)
		}
		return nil
	}
	if !errors.Is(err, nats.ErrStreamNotFound) {
		return fmt.Errorf("checking stream %s: %w", cfg.Name, err)
	}
	_, err = p.js.AddStream(&nats.StreamConfig{
		Name:      cfg.Name,
		Subjects:  cfg.Subjects,
		Retention: cfg.Retention,
		MaxAge:    cfg.MaxAge,
		Replicas:  cfg.Replicas,
	})
	if err != nil {
		return fmt.Errorf("adding stream %s: %w", cfg.Name, err)
	}
	return nil
}

func equalSubjects(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// PublishTaskUpdate publishes a task update to edgecloud.tasks.<region>.
func (p *NATSPublisher) PublishTaskUpdate(region string, msg *TaskMessage) error {
	subject := "edgecloud.tasks." + region
	return p.publishTaskMessage(subject, msg, "task_update")
}

// PublishFullSync publishes a full-state sync to edgecloud.tasks.<region>.
// Wire format is identical to PublishTaskUpdate except the `type` field is
// "full_sync" so the worker can distinguish a scheduled reconcile from an
// event-driven update in metrics/logs. Used by:
//   - ReconcileService.Run — periodic safety net (RECONCILE_INTERVAL, default 5min)
//   - ReconcileService.RequestSync — on worker registration
//   - InternalHandler.Sync — HTTP fallback when NATS is silent > N seconds
func (p *NATSPublisher) PublishFullSync(region string, msg *TaskMessage) error {
	subject := "edgecloud.tasks." + region
	return p.publishTaskMessage(subject, msg, "full_sync")
}

// publishTaskMessage marshals and publishes a TaskMessage, overriding the
// `type` field via applyTypeOverride (shared with MockPublisher so the
// wire shape is identical regardless of which publisher the operator
// configured).
func (p *NATSPublisher) publishTaskMessage(subject string, msg *TaskMessage, typeField string) error {
	data, err := json.Marshal(applyTypeOverride(msg, typeField))
	if err != nil {
		return fmt.Errorf("marshaling task message: %w", err)
	}
	_, err = p.js.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("publishing to %s: %w", subject, err)
	}
	return nil
}

// PublishHeartbeat publishes a heartbeat to edgecloud.heartbeats.<region>.
func (p *NATSPublisher) PublishHeartbeat(region string, msg *HeartbeatMessage) error {
	subject := "edgecloud.heartbeats." + region
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshaling heartbeat message: %w", err)
	}
	_, err = p.js.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("publishing to %s: %w", subject, err)
	}
	return nil
}

// Close closes the NATS connection.
func (p *NATSPublisher) Close() {
	p.nc.Close()
}

// Conn returns the underlying NATS connection for subscriber use.
func (p *NATSPublisher) Conn() *nats.Conn {
	return p.nc
}
