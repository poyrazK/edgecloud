package service

import (
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

func TestAuditor_Record(t *testing.T) {
	db, mock, cleanup := newWebhookMockDB(t)
	defer cleanup()

	repo := repository.NewAuditRepository(db)
	auditor := NewAuditor(repo)

	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO audit_logs`)).
		WithArgs("t_1", "ak_test", "owner", "deploy", "app", "hello", "deployed v2", "success", "", "10.0.0.1").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

	auditor.Record(AuditInfo{
		TenantID:   "t_1",
		APIKeyID:   "ak_test",
		Role:       "owner",
		Action:     "deploy",
		Resource:   "app",
		ResourceID: "hello",
		Details:    "deployed v2",
		Outcome:    "success",
		RequestIP:  "10.0.0.1",
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestAuditor_Record_NilAuditor(t *testing.T) {
	var auditor *Auditor
	// Should not panic.
	auditor.Record(AuditInfo{TenantID: "t_1"})
}

func TestAuditor_Record_NilRepo(t *testing.T) {
	auditor := &Auditor{repo: nil}
	// Should not panic.
	auditor.Record(AuditInfo{TenantID: "t_1"})
}

func TestStripPort_WithPort(t *testing.T) {
	if got := StripPort("1.2.3.4:5678"); got != "1.2.3.4" {
		t.Errorf("got %q, want 1.2.3.4", got)
	}
}

func TestStripPort_WithoutPort(t *testing.T) {
	if got := StripPort("1.2.3.4"); got != "1.2.3.4" {
		t.Errorf("got %q, want 1.2.3.4", got)
	}
}

func TestStripPort_IPv6(t *testing.T) {
	// StripPort is intentionally simple (first colon split). IPv6 addresses
	// from Go's net/http are typically bracketed: [::1]:80. Without brackets
	// the format is ambiguous so the function returns whatever is before the
	// first colon.
	if got := StripPort("::1:80"); got != "" {
		t.Errorf("got %q, want \"\" (first-colon split for bare IPv6)", got)
	}
}
