package nats

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/nats-io/nats.go"
)

// Publisher defines the interface for NATS publishing.
type Publisher interface {
	PublishTaskUpdate(region string, msg *TaskMessage) error
	PublishHeartbeat(region string, msg *HeartbeatMessage) error
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
}

// HeartbeatMessage is published by workers to edgecloud.heartbeats.<region>.
type HeartbeatMessage struct {
	Type       string                      `json:"type"`
	Timestamp  time.Time                   `json:"timestamp"`
	WorkerID   string                      `json:"worker_id"`
	Region     string                      `json:"region"`
	WorkerAddr string                      `json:"worker_addr,omitempty"`
	Apps       map[string]domain.AppStatus `json:"apps"`
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

// NATSPublisher is a real NATS JetStream publisher.
type NATSPublisher struct {
	nc *nats.Conn
	js nats.JetStream
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
