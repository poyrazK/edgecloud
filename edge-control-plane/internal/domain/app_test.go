package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestApp_JSONRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	desc := "my first app"
	app := App{
		ID:          "a_xyz",
		TenantID:    "t_test",
		Name:        "hello-world",
		Description: &desc,
		CreatedAt:   now,
	}
	data, err := json.Marshal(app)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back App
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.ID != app.ID {
		t.Errorf("ID = %q, want %q", back.ID, app.ID)
	}
	if back.TenantID != app.TenantID {
		t.Errorf("TenantID = %q, want %q", back.TenantID, app.TenantID)
	}
	if back.Name != app.Name {
		t.Errorf("Name = %q, want %q", back.Name, app.Name)
	}
	if back.Description == nil || *back.Description != desc {
		t.Errorf("Description = %v, want %q", back.Description, desc)
	}
}

func TestApp_NilDescriptionOmittedFromJSON(t *testing.T) {
	app := App{
		ID:        "a_abc",
		TenantID:  "t_test",
		Name:      "test-app",
		CreatedAt: time.Now(),
	}
	data, err := json.Marshal(app)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if contains(data, "description") {
		t.Errorf("description should be omitted when nil, got: %s", string(data))
	}
}

func TestCreateAppRequest_JSONRoundTrip(t *testing.T) {
	body := `{"description":"my-app-desc"}`
	var req CreateAppRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if req.Description != "my-app-desc" {
		t.Errorf("Description = %q, want 'my-app-desc'", req.Description)
	}
	// Empty description
	bodyEmpty := `{}`
	var reqEmpty CreateAppRequest
	if err := json.Unmarshal([]byte(bodyEmpty), &reqEmpty); err != nil {
		t.Fatalf("unmarshal empty: %v", err)
	}
	if reqEmpty.Description != "" {
		t.Errorf("Description = %q, want ''", reqEmpty.Description)
	}
}
