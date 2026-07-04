package handler

import (
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// DefaultAuditor is the package-level auditor used by all handlers.
// Set once at startup in app.go. Read by the auditRecord helper.
var DefaultAuditor *service.Auditor

// DefaultTenantCreationLimiter limits tenant creation per IP.
// Set once at startup in app.go. Read by the Bootstrap handler.
var DefaultTenantCreationLimiter *middleware.TenantCreationLimiter

// auditRecord records an audit event using context extracted from the request.
// No-op when DefaultAuditor is nil (safe for tests).
func auditRecord(r *http.Request, action, resource, resourceID, details, outcome string) {
	if DefaultAuditor == nil {
		return
	}
	DefaultAuditor.Record(service.AuditInfo{
		TenantID:   middleware.GetTenantID(r.Context()),
		APIKeyID:   middleware.GetAPIKeyID(r.Context()),
		Role:       middleware.GetRole(r.Context()),
		Action:     action,
		Resource:   resource,
		ResourceID: resourceID,
		Details:    details,
		Outcome:    outcome,
		ErrorMsg:   "",
		RequestIP:  service.StripPort(r.RemoteAddr),
	})
}
