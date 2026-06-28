package domain

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/lib/pq"
)

func TestTenant_JSONRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	tnt := Tenant{
		ID:                     "t_abc",
		Name:                   "acme-corp",
		Plan:                   "free",
		AllowlistedDestinations: pq.StringArray{"*.example.com", "api.internal"},
		CreatedAt:              now,
	}
	data, err := json.Marshal(tnt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !contains(data, `"Name":"acme-corp"`) {
		t.Errorf("missing Name in JSON: %s", string(data))
	}
	if !contains(data, `"Plan":"free"`) {
		t.Errorf("missing Plan in JSON: %s", string(data))
	}
	// AllowlistedDestinations should serialize as JSON array
	if !contains(data, `["*.example.com","api.internal"]`) {
		t.Errorf("expected allowlisted_destinations array in JSON, got: %s", string(data))
	}
	var back Tenant
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Name != tnt.Name {
		t.Errorf("Name = %q, want %q", back.Name, tnt.Name)
	}
	if len(back.AllowlistedDestinations) != 2 {
		t.Errorf("AllowlistedDestinations length = %d, want 2", len(back.AllowlistedDestinations))
	}
}

func TestDefaultQuota(t *testing.T) {
	q := DefaultQuota("t_test")
	if q.TenantID != "t_test" {
		t.Errorf("TenantID = %q, want 't_test'", q.TenantID)
	}
	if q.MaxDeployments != 10 {
		t.Errorf("MaxDeployments = %d, want 10", q.MaxDeployments)
	}
	if q.MaxApps != 5 {
		t.Errorf("MaxApps = %d, want 5", q.MaxApps)
	}
	if q.MaxWorkers != 3 {
		t.Errorf("MaxWorkers = %d, want 3", q.MaxWorkers)
	}
	if q.MaxMemoryMB != 256 {
		t.Errorf("MaxMemoryMB = %d, want 256", q.MaxMemoryMB)
	}
	if q.MaxOutboundMB != 1000 {
		t.Errorf("MaxOutboundMB = %d, want 1000", q.MaxOutboundMB)
	}
}

func TestTenantWithQuota_JSONRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	tq := TenantWithQuota{
		Tenant: Tenant{
			ID:        "t_xyz",
			Name:      "test-corp",
			Plan:      "pro",
			CreatedAt: now,
		},
		Quota: Quota{
			TenantID:       "t_xyz",
			MaxDeployments: 50,
			MaxApps:        20,
			MaxWorkers:     10,
			MaxMemoryMB:    1024,
			MaxOutboundMB:  10000,
		},
	}
	data, err := json.Marshal(tq)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Tenant fields should be flattened at top level
	if !contains(data, `"Plan":"pro"`) {
		t.Errorf("missing Plan in JSON: %s", string(data))
	}
	if !contains(data, `"MaxApps":20`) {
		t.Errorf("missing MaxApps in JSON: %s", string(data))
	}
}
