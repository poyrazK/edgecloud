package nats

import (
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

func TestNATSPublisherImplementsPublisher(t *testing.T) {
	var p Publisher = &NATSPublisher{}
	_ = p // compile check: NATSPublisher implements Publisher
}

func TestNewNATSPublisher_ConnectionError(t *testing.T) {
	_, err := NewNATSPublisher("nats://localhost:4222")
	if err == nil {
		t.Skip("NATS not available, skipping")
	}
}

func TestMockPublisher_PublishTaskUpdate(t *testing.T) {
	p := &MockPublisher{}
	msg := &TaskMessage{
		Type:      "task_update",
		Timestamp: time.Now(),
		TenantID:  "t_test",
		Apps:      map[string]AppConfig{},
	}
	if err := p.PublishTaskUpdate("global", msg); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestMockPublisher_PublishHeartbeat(t *testing.T) {
	p := &MockPublisher{}
	msg := &HeartbeatMessage{
		Type:      "heartbeat",
		Timestamp: time.Now(),
		WorkerID:  "w_test",
		Region:    "global",
		Apps:      map[string]domain.AppStatus{},
	}
	if err := p.PublishHeartbeat("global", msg); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
