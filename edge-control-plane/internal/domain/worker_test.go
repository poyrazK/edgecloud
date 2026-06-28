package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestIsValidWorkerID(t *testing.T) {
	valid := []string{"w_fra_abc123", "w_sfo_xyz", "w_east_000-000", "w_region_id"}
	for _, id := range valid {
		if !IsValidWorkerID(id) {
			t.Errorf("IsValidWorkerID(%q) = false, want true", id)
		}
	}
	invalid := []string{"", "w_", "w__", "x_fra_abc", "wf ra_abc", "worker_abc", "w_fra", "w_fra_"}
	for _, id := range invalid {
		if IsValidWorkerID(id) {
			t.Errorf("IsValidWorkerID(%q) = true, want false", id)
		}
	}
}

func TestIngressHost_Format(t *testing.T) {
	got := IngressHost("t_acme", "api")
	want := "t_acme-api.edgecloud.dev"
	if got != want {
		t.Errorf("IngressHost = %q, want %q", got, want)
	}
}

func TestIngressHostSuffix_Constant(t *testing.T) {
	if IngressHostSuffix != "edgecloud.dev" {
		t.Errorf("IngressHostSuffix = %q, want 'edgecloud.dev'", IngressHostSuffix)
	}
}

func TestWorker_JSONRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	ip := "10.0.0.1"
	w := Worker{
		ID:        "w_fra_abc",
		TenantID:  "t_test",
		Region:    "fra",
		IP:        &ip,
		MemoryMB:  4096,
		LastSeen:  now,
		CreatedAt: now,
	}
	data, err := json.Marshal(w)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !contains(data, `"ID":"w_fra_abc"`) {
		t.Errorf("missing ID in JSON: %s", string(data))
	}
	if !contains(data, `"Region":"fra"`) {
		t.Errorf("missing Region in JSON: %s", string(data))
	}
	var back Worker
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.ID != w.ID {
		t.Errorf("ID = %q, want %q", back.ID, w.ID)
	}
	if back.IP == nil || *back.IP != ip {
		t.Errorf("IP = %v, want %q", back.IP, ip)
	}
}

func TestRegisterWorkerRequest_JSONRoundTrip(t *testing.T) {
	body := `{"worker_id":"w_fra_abc","region":"fra","ip":"10.0.0.1","memory_mb":4096}`
	var req RegisterWorkerRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if req.WorkerID != "w_fra_abc" {
		t.Errorf("WorkerID = %q, want 'w_fra_abc'", req.WorkerID)
	}
	if req.Region != "fra" {
		t.Errorf("Region = %q, want 'fra'", req.Region)
	}
	if req.MemoryMB != 4096 {
		t.Errorf("MemoryMB = %d, want 4096", req.MemoryMB)
	}
}

func TestAppWorkerStatus_JSONRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	exitCode := int32(0)
	aws := AppWorkerStatus{
		AppName:       "hello",
		Status:        "running",
		LastHeartbeat: &now,
		Region:        "fra",
		WorkerID:      "w_fra_abc",
		ExitCode:      &exitCode,
	}
	data, err := json.Marshal(aws)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !contains(data, `"status":"running"`) {
		t.Errorf("missing status in JSON: %s", string(data))
	}
	var back AppWorkerStatus
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Status != aws.Status {
		t.Errorf("Status = %q, want %q", back.Status, aws.Status)
	}
	if back.ExitCode == nil || *back.ExitCode != exitCode {
		t.Errorf("ExitCode = %v, want %d", back.ExitCode, exitCode)
	}
}

func TestAppWorkerStatus_NullFieldsOmitFromJSON(t *testing.T) {
	aws := AppWorkerStatus{
		AppName: "unknown-app",
		Status:  "unknown",
	}
	data, err := json.Marshal(aws)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// nil exit_code should be omitted
	if contains(data, "exit_code") {
		t.Errorf("nil exit_code should be omitted, got: %s", string(data))
	}
}

func TestMetricKind_Constants(t *testing.T) {
	if MetricKindCounter != "counter" {
		t.Errorf("MetricKindCounter = %q", MetricKindCounter)
	}
	if MetricKindGauge != "gauge" {
		t.Errorf("MetricKindGauge = %q", MetricKindGauge)
	}
	if MetricKindHistogramSample != "histogram_sample" {
		t.Errorf("MetricKindHistogramSample = %q", MetricKindHistogramSample)
	}
}

func TestAppTarget_JSONRoundTrip(t *testing.T) {
	at := AppTarget{
		AppName:    "hello",
		TenantID:   "t_test",
		WorkerID:   "w_fra_abc",
		Region:     "fra",
		WorkerAddr: "10.0.0.1",
		Port:       8080,
	}
	data, err := json.Marshal(at)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back AppTarget
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.AppName != at.AppName {
		t.Errorf("AppName = %q, want %q", back.AppName, at.AppName)
	}
	if back.Port != 8080 {
		t.Errorf("Port = %d, want 8080", back.Port)
	}
}

func TestAppStatus_JSONRoundTrip(t *testing.T) {
	as := AppStatus{
		Status:        "running",
		ExitCode:      0,
		DeploymentID:  "d_123",
		RequestCount:  42,
		OutboundBytes: 1024,
		TenantID:      "t_test",
		Port:          8080,
	}
	data, err := json.Marshal(as)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !contains(data, `"status":"running"`) {
		t.Errorf("missing status in JSON: %s", string(data))
	}
	if !contains(data, `"request_count":42`) {
		t.Errorf("missing request_count in JSON: %s", string(data))
	}
}

func TestAppStatus_ObserverMetricsOmitempty(t *testing.T) {
	as := AppStatus{
		Status:       "running",
		DeploymentID: "d_123",
	}
	data, err := json.Marshal(as)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// nil observer_metrics should be omitted
	if contains(data, "observer_metrics") {
		t.Errorf("nil observer_metrics with omitempty should be omitted, got: %s", string(data))
	}
}
