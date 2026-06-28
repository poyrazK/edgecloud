package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestLogEntry_JSONRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	le := LogEntry{
		ID:           1,
		TenantID:     "t_test",
		DeploymentID: "d_123",
		AppName:      "hello",
		WorkerID:     "w_fra_abc",
		Region:       "fra",
		Level:        "info",
		Message:      "request processed",
		Labels:       json.RawMessage(`{"method":"GET","path":"/"}`),
		TS:           now,
	}
	data, err := json.Marshal(le)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, want := range []string{
		`"level":"info"`,
		`"message":"request processed"`,
		`"labels":{"method":"GET","path":"/"}`,
		`"app_name":"hello"`,
	} {
		if !contains(data, want) {
			t.Errorf("missing %q in JSON: %s", want, string(data))
		}
	}
	var back LogEntry
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.ID != le.ID {
		t.Errorf("ID = %d, want %d", back.ID, le.ID)
	}
	if back.Level != le.Level {
		t.Errorf("Level = %q, want %q", back.Level, le.Level)
	}
	if back.Message != le.Message {
		t.Errorf("Message = %q, want %q", back.Message, le.Message)
	}
	// Labels should round-trip as raw JSON
	var labels map[string]interface{}
	if err := json.Unmarshal(back.Labels, &labels); err != nil {
		t.Fatalf("unmarshal labels: %v", err)
	}
	if labels["method"] != "GET" {
		t.Errorf("labels.method = %v, want 'GET'", labels["method"])
	}
}

func TestLogEntry_LabelsNull(t *testing.T) {
	le := LogEntry{
		ID:           2,
		TenantID:     "t_test",
		DeploymentID: "d_456",
		AppName:      "hello",
		WorkerID:     "w_sfo_xyz",
		Region:       "sfo",
		Level:        "error",
		Message:      "timeout",
		Labels:       json.RawMessage(`null`),
		TS:           time.Now(),
	}
	data, err := json.Marshal(le)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !contains(data, `"labels":null`) {
		t.Errorf("expected null labels, got: %s", string(data))
	}
}

func TestLogEntry_IDOmitempty(t *testing.T) {
	le := LogEntry{
		TenantID:     "t_test",
		DeploymentID: "d_789",
		AppName:      "hello",
		WorkerID:     "w_fra_xyz",
		Region:       "fra",
		Level:        "info",
		Message:      "startup",
		TS:           time.Now(),
	}
	data, err := json.Marshal(le)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// id with omitempty and value 0 should be omitted
	if contains(data, `"id"`) {
		t.Errorf("zero id with omitempty should be omitted, got: %s", string(data))
	}
}
