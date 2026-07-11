package handler

import (
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// AuditRecorder is the narrow contract the auditRecord helper needs.
// Holding an interface (instead of *service.Auditor directly) lets
// handler tests swap in a spy without spinning up a sqlmock. The
// production wiring in app.go:308 still passes *service.Auditor,
// which satisfies this contract.
type AuditRecorder interface {
	Record(info service.AuditInfo)
}

// DefaultAuditor is the package-level auditor used by all handlers.
// Set once at startup in app.go. Read by the auditRecord helper.
var DefaultAuditor AuditRecorder

// DefaultTenantCreationLimiter limits tenant creation per IP.
// Set once at startup in app.go. Read by the Bootstrap handler.
var DefaultTenantCreationLimiter *middleware.TenantCreationLimiter

// auditRecord records an audit event using context extracted from the request.
// No-op when DefaultAuditor is nil (safe for tests).
//
// TenantID resolution: the tenant-auth middleware stamps
// `TenantIDKey`; the worker-auth and bootstrap-auth middlewares stamp
// `WorkerTenantIDKey` (issue #430 — bootstrap and per-worker
// enrollment happen before the worker has a tenant-scoped JWT).
// For worker-scoped audit events (resource="worker", action
// starts with "worker_"/"bootstrap") we fall back to
// `WorkerTenantIDKey` so the audit row's tenant_id column is
// populated and operators can correlate "show me every enrollment
// for tenant X" without parsing the Details blob.
//
// Region is intentionally NOT a separate audit column today — the
// `Details` field carries it as `region=<value>` for the
// bootstrap/enroll paths and a follow-up migration would add a
// dedicated column. Until that ships, the Details string is the
// single source of truth.
func auditRecord(r *http.Request, action, resource, resourceID, details, outcome string) {
	if DefaultAuditor == nil {
		return
	}
	ctx := r.Context()
	tenantID := middleware.GetTenantID(ctx)
	if tenantID == "" {
		// Fall back to the worker-auth tenant claim so per-worker
		// audit rows (bootstrap, worker_enroll, worker_token_mint)
		// have a populated tenant_id column for tenant-scoped
		// queries. The bootstrap context also stamps
		// WorkerTenantIDKey, which means this fallback covers
		// the bootstrap → enroll → token-mint lifecycle end to
		// end without each handler having to plumb tenant_id
		// through manually.
		if wid := middleware.GetWorkerTenantID(ctx); wid != "" {
			tenantID = wid
		}
	}
	DefaultAuditor.Record(service.AuditInfo{
		TenantID:   tenantID,
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
