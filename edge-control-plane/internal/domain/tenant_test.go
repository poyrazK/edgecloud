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
		ID:                      "t_abc",
		Name:                    "acme-corp",
		Plan:                    "free",
		AllowlistedDestinations: pq.StringArray{"*.example.com", "api.internal"},
		CreatedAt:               now,
	}
	data, err := json.Marshal(tnt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !contains(data, `"name":"acme-corp"`) {
		t.Errorf("missing name in JSON: %s", string(data))
	}
	if !contains(data, `"plan":"free"`) {
		t.Errorf("missing plan in JSON: %s", string(data))
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
	t.Skip("DefaultQuota was deleted in billing v0 review remediation; see plans.")
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
			TenantID:            "t_xyz",
			MaxDeployments:      50,
			MaxApps:             20,
			MaxWorkers:          10,
			MaxMemoryMB:         1024,
			MaxOutboundMB:       10000,
			MaxRequestsPerMonth: 5_000_000,
		},
	}
	data, err := json.Marshal(tq)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Tenant fields should be flattened at top level
	if !contains(data, `"plan":"pro"`) {
		t.Errorf("missing plan in JSON: %s", string(data))
	}
	// Quota fields are emitted with snake_case keys (per the json tags
	// added for billing v0). Verify one representative key plus the
	// new max_requests_per_month field.
	if !contains(data, `"max_apps":20`) {
		t.Errorf("missing max_apps in JSON: %s", string(data))
	}
	if !contains(data, `"max_requests_per_month":5000000`) {
		t.Errorf("missing max_requests_per_month in JSON: %s", string(data))
	}
}
