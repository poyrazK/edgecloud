package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDomain_JSONRoundTrip(t *testing.T) {
	errStr := "DNS propagation timeout"
	now := time.Now().Truncate(time.Second)
	d := Domain{
		ID:         "dom_1",
		TenantID:   "t_test",
		AppName:    "hello",
		FQDN:       "hello.edgecloud.dev",
		Status:     DomainStatusPending,
		LastError:  &errStr,
		CreatedAt:  now,
		VerifiedAt: &now,
	}
	data, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Required fields
	for _, want := range []string{`"id":"dom_1"`, `"fqdn":"hello.edgecloud.dev"`, `"status":"pending"`} {
		if !contains(data, want) {
			t.Errorf("missing %q in JSON: %s", want, string(data))
		}
	}
	// last_error with omitempty should be present when non-nil
	if !contains(data, `"last_error":"DNS propagation timeout"`) {
		t.Errorf("missing last_error in JSON: %s", string(data))
	}
	var back Domain
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.FQDN != d.FQDN {
		t.Errorf("FQDN = %q, want %q", back.FQDN, d.FQDN)
	}
	if back.Status != DomainStatusPending {
		t.Errorf("Status = %q, want %q", back.Status, DomainStatusPending)
	}
	if back.LastError == nil || *back.LastError != errStr {
		t.Errorf("LastError = %v, want %q", back.LastError, errStr)
	}
}

func TestDomain_NullFieldsOmitFromJSON(t *testing.T) {
	d := Domain{
		ID:        "dom_2",
		TenantID:  "t_test",
		AppName:   "app",
		FQDN:      "app.edgecloud.dev",
		Status:    DomainStatusActive,
		CreatedAt: time.Now(),
	}
	data, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if contains(data, "last_error") {
		t.Errorf("nil last_error should be omitted, got: %s", string(data))
	}
	if contains(data, "verified_at") {
		t.Errorf("nil verified_at should be omitted, got: %s", string(data))
	}
}

func TestDomainStatusConstants(t *testing.T) {
	if DomainStatusPending != "pending" {
		t.Errorf("DomainStatusPending = %q", DomainStatusPending)
	}
	if DomainStatusActive != "active" {
		t.Errorf("DomainStatusActive = %q", DomainStatusActive)
	}
	if DomainStatusFailed != "failed" {
		t.Errorf("DomainStatusFailed = %q", DomainStatusFailed)
	}
}
