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
	Env            map[string]string `json:"env"`
	Allowlist      []string          `json:"allowlist"`
	MaxMemoryMB    int               `json:"max_memory_mb"`
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
}

// StreamConfig describes a JetStream stream to be created/verified.
type StreamConfig struct {
	Name      string
	Subjects  []string
	Retention nats.RetentionPolicy
	MaxAge    time.Duration
	Replicas  int
}

// MockPublisher is a no-op publisher for development.
type MockPublisher struct{}

func (p *MockPublisher) PublishTaskUpdate(region string, msg *TaskMessage) error {
	data, _ := json.Marshal(msg)
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
	data, err := json.Marshal(msg)
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
