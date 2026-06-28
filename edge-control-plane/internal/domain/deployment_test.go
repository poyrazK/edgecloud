package domain

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/lib/pq"
)

func TestDeployment_JSONRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	d := Deployment{
		ID:                  "d_123",
		TenantID:            "t_test",
		AppName:             "hello",
		Status:              StatusDeployed,
		Hash:                "abcdef",
		Regions:             pq.StringArray{"fra", "sfo"},
		CreatedAt:           now,
		AutoRollbackEnabled: true,
	}
	data, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Regions must serialize as a JSON array
	if !contains(data, `["fra","sfo"]`) {
		t.Errorf("expected regions array in JSON, got: %s", string(data))
	}
	if !contains(data, `"auto_rollback_enabled":true`) {
		t.Errorf("expected auto_rollback_enabled in JSON, got: %s", string(data))
	}
	var back Deployment
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.ID != d.ID {
		t.Errorf("ID = %q, want %q", back.ID, d.ID)
	}
	if back.Status != StatusDeployed {
		t.Errorf("Status = %q, want %q", back.Status, StatusDeployed)
	}
	if len(back.Regions) != 2 || back.Regions[0] != "fra" || back.Regions[1] != "sfo" {
		t.Errorf("Regions = %v", back.Regions)
	}
	if !back.AutoRollbackEnabled {
		t.Errorf("AutoRollbackEnabled = false, want true")
	}
}

func TestActiveDeployment_JSONRoundTrip(t *testing.T) {
	goodID := "d_good_001"
	now := time.Now().Truncate(time.Second)
	pubID := "pub-123"
	ad := ActiveDeployment{
		TenantID:             "t_test",
		AppName:              "hello",
		DeploymentID:         "d_active",
		LastGoodDeploymentID: &goodID,
		AutoRollbackEnabled:  true,
		StableSince:          &now,
		RegionsPublished:     pq.StringArray{"fra"},
		RegionsFailed:        pq.StringArray{},
		LastPublishAt:        &now,
		LastPublishAttemptID: &pubID,
	}
	data, err := json.Marshal(ad)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back ActiveDeployment
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.DeploymentID != ad.DeploymentID {
		t.Errorf("DeploymentID = %q, want %q", back.DeploymentID, ad.DeploymentID)
	}
	if back.LastGoodDeploymentID == nil || *back.LastGoodDeploymentID != goodID {
		t.Errorf("LastGoodDeploymentID = %v, want %q", back.LastGoodDeploymentID, goodID)
	}
	if back.LastPublishAttemptID == nil || *back.LastPublishAttemptID != pubID {
		t.Errorf("LastPublishAttemptID = %v, want %q", back.LastPublishAttemptID, pubID)
	}
}

func TestActiveDeployment_NullFieldsOmitFromJSON(t *testing.T) {
	ad := ActiveDeployment{
		TenantID:     "t_test",
		AppName:      "hello",
		DeploymentID: "d_1",
	}
	data, err := json.Marshal(ad)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// nil pointer fields should be absent
	if contains(data, "last_good_deployment_id") {
		t.Errorf("nil last_good_deployment_id should be omitted, got: %s", string(data))
	}
	if contains(data, "stable_since") {
		t.Errorf("nil stable_since should be omitted, got: %s", string(data))
	}
	// nil pq.StringArray (zero value) serializes as null when no json tag
	if !contains(data, `"RegionsPublished":null`) {
		t.Errorf("nil pq.StringArray should serialize as null, got: %s", string(data))
	}
}

func TestStatusConstants(t *testing.T) {
	if StatusDeployed != "deployed" {
		t.Errorf("StatusDeployed = %q", StatusDeployed)
	}
	if StatusActive != "active" {
		t.Errorf("StatusActive = %q", StatusActive)
	}
	if StatusFailed != "failed" {
		t.Errorf("StatusFailed = %q", StatusFailed)
	}
	if StatusMigrated != "migrated" {
		t.Errorf("StatusMigrated = %q", StatusMigrated)
	}
}

func TestAppEnv_JSONRoundTrip(t *testing.T) {
	ae := AppEnv{
		TenantID: "t_test",
		AppName:  "hello",
		EnvKey:   "DATABASE_URL",
		EnvValue: "postgres://localhost",
	}
	data, err := json.Marshal(ae)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back AppEnv
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.EnvKey != ae.EnvKey {
		t.Errorf("EnvKey = %q, want %q", back.EnvKey, ae.EnvKey)
	}
	if back.EnvValue != ae.EnvValue {
		t.Errorf("EnvValue = %q, want %q", back.EnvValue, ae.EnvValue)
	}
}
