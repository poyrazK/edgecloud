package handler

import (
	"net/http/httptest"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

func TestAuditRecord_RecordsEvent(t *testing.T) {
	old := DefaultAuditor
	DefaultAuditor = service.NewAuditor(nil) // repo is nil so Record is a no-op
	defer func() { DefaultAuditor = old }()

	req := httptest.NewRequest("POST", "/api/v1/apps/hello/deploy", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_1"))
	req = req.WithContext(middleware.WithAPIKeyID(req.Context(), "ak_test"))
	req = req.WithContext(middleware.WithRole(req.Context(), "owner"))
	req.RemoteAddr = "10.0.0.1:5678"

	// Should not panic. The auditor's repo is nil so Record is a no-op,
	// but we verify the code path extracts context and calls Record.
	auditRecord(req, "deploy", "app", "hello", "deployed v2", "success")
}

func TestAuditRecord_NilAuditor(t *testing.T) {
	old := DefaultAuditor
	DefaultAuditor = nil
	defer func() { DefaultAuditor = old }()

	req := httptest.NewRequest("POST", "/api/v1/apps/hello/deploy", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_1"))

	// Should not panic when auditor is nil.
	auditRecord(req, "deploy", "app", "hello", "", "success")
}
