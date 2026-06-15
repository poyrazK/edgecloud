package nats

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// Publisher defines the interface for NATS publishing.
type Publisher interface {
	PublishTaskUpdate(region string, msg *TaskMessage) error
	PublishHeartbeat(region string, msg *HeartbeatMessage) error
}

// TaskMessage is published to edgecloud.tasks.<region> when app set changes.
type TaskMessage struct {
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	TenantID  string    `json:"tenant_id"`
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
	Type      string                        `json:"type"`
	Timestamp time.Time                     `json:"timestamp"`
	WorkerID  string                        `json:"worker_id"`
	Region    string                        `json:"region"`
	Apps      map[string]domain.AppStatus   `json:"apps"`
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
