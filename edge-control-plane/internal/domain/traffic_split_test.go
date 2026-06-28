package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestTrafficSplit_JSONRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	ts := TrafficSplit{
		TenantID:     "t_test",
		AppName:      "hello",
		DeploymentID: "d_123",
		Weight:       80,
		CreatedAt:    now,
	}
	data, err := json.Marshal(ts)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !contains(data, `"Weight":80`) {
		t.Errorf("missing Weight in JSON: %s", string(data))
	}
	if !contains(data, `"DeploymentID":"d_123"`) {
		t.Errorf("missing DeploymentID in JSON: %s", string(data))
	}
	var back TrafficSplit
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Weight != 80 {
		t.Errorf("Weight = %d, want 80", back.Weight)
	}
	if back.DeploymentID != "d_123" {
		t.Errorf("DeploymentID = %q, want 'd_123'", back.DeploymentID)
	}
}

func TestTrafficSplitRequest_JSONRoundTrip(t *testing.T) {
	body := `{"splits":[{"deployment_id":"d_1","weight":70},{"deployment_id":"d_2","weight":30}]}`
	var req TrafficSplitRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(req.Splits) != 2 {
		t.Fatalf("len(Splits) = %d, want 2", len(req.Splits))
	}
	if req.Splits[0].DeploymentID != "d_1" || req.Splits[0].Weight != 70 {
		t.Errorf("Splits[0] = %+v", req.Splits[0])
	}
	if req.Splits[1].DeploymentID != "d_2" || req.Splits[1].Weight != 30 {
		t.Errorf("Splits[1] = %+v", req.Splits[1])
	}
	// Round-trip
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back TrafficSplitRequest
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(back.Splits) != 2 {
		t.Errorf("round-trip len = %d, want 2", len(back.Splits))
	}
}
