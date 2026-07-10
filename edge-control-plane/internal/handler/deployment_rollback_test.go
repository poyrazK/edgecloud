package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// stubRollbacker is the minimum implementation of deploymentRollbacker
// needed by the Rollback handler. It records the args it was called with
// so tests can assert the tenant filter is applied correctly, and
// returns whatever response/err the test sets.
type stubRollbacker struct {
	resp   string
	err    error
	called bool
	// lastTenant / lastApp record the arguments the handler passed so
	// tests can assert that the tenant context (not the URL) wins.
	lastTenant string
	lastApp    string
}

func (s *stubRollbacker) RollbackDeployment(_ context.Context, tenantID, appName string) (string, error) {
	s.called = true
	s.lastTenant = tenantID
	s.lastApp = appName
	return s.resp, s.err
}

// newRollbackMux wires a single POST /api/apps/{appName}/rollback route
// through a real *http.ServeMux so r.PathValue("appName") is populated
// the same way it is in production. workerSvc is nil because Rollback
// never touches it.
func newRollbackMux(svc *stubRollbacker) *http.ServeMux {
	h := &DeploymentHandler{
		workerSvc: nil,
		// deploymentSvc is nil — the handler body for Rollback never
		// touches it; only rollbackSvc is consulted.
		deploymentSvc: nil,
		rollbackSvc:   svc,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/apps/{appName}/rollback", h.Rollback)
	return mux
}

// ---------------------------------------------------------------------------
// Rollback — 200 (happy path)
// ---------------------------------------------------------------------------

func TestRollback_HappyPath_Returns200WithDeploymentID(t *testing.T) {
	svc := &stubRollbacker{resp: "d_prev"}
	mux := newRollbackMux(svc)

	req := httptest.NewRequest("POST", "/api/apps/myapp/rollback", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var got map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["deployment_id"] != "d_prev" {
		t.Errorf("deployment_id = %q, want d_prev", got["deployment_id"])
	}
	// The handler must propagate the tenant id from the auth context,
	// not from the URL — this is what keeps cross-tenant rollback from
	// working.
	if svc.lastTenant != "t_test" {
		t.Errorf("RollbackDeployment called with tenant %q, want t_test", svc.lastTenant)
	}
	if svc.lastApp != "myapp" {
		t.Errorf("RollbackDeployment called with app %q, want myapp", svc.lastApp)
	}
	if !svc.called {
		t.Error("RollbackDeployment was not called")
	}
}

// ---------------------------------------------------------------------------
// Rollback — 409 (no last-good pointer)
// ---------------------------------------------------------------------------

func TestRollback_NoLastGood_Returns409(t *testing.T) {
	svc := &stubRollbacker{err: service.ErrNoLastGood}
	mux := newRollbackMux(svc)

	req := httptest.NewRequest("POST", "/api/apps/myapp/rollback", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body: %s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, "no previous deployment to roll back to") {
		t.Errorf("body %q should contain the typed 409 message", body)
	}
	// Body must not leak the raw sentinel.
	if strings.Contains(body, "ErrNoLastGood") {
		t.Errorf("body leaks sentinel: %s", body)
	}
}

// ---------------------------------------------------------------------------
// Rollback — 404 (no active deployment row at all)
// ---------------------------------------------------------------------------

func TestRollback_AppNotFound_Returns404(t *testing.T) {
	// Service returns ErrNoActiveDeployment when GetForUpdate yields nil
	// (no active-deployment row for this app). Handler maps via
	// errors.Is, so the stub must return the typed sentinel.
	svc := &stubRollbacker{err: service.ErrNoActiveDeployment}
	mux := newRollbackMux(svc)

	req := httptest.NewRequest("POST", "/api/apps/missing/rollback", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "no active deployment") {
		t.Errorf("body should contain 404 message, got %s", rr.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Rollback — 500 (unexpected service error)
// ---------------------------------------------------------------------------
//
// Note: prior versions of this file asserted a 502 path for post-commit
// NATS publish failures. Issue #42 made that path obsolete: durable
// publish is now owned by service.OutboxDrainer (see CLAUDE.md schema
// table). RollbackDeployment enqueues the task_update inside the same
// transaction as the active_deployments mutation; NATS unreachability
// surfaces as a backlogged `pending` outbox row, never as a 502.

func TestRollback_ServiceError_Returns500(t *testing.T) {
	svc := &stubRollbacker{err: errors.New("db unreachable")}
	mux := newRollbackMux(svc)

	req := httptest.NewRequest("POST", "/api/apps/myapp/rollback", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body: %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "db unreachable") {
		t.Errorf("body must not leak raw error, got %s", rr.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Rollback — 400 (path traversal in appName)
// ---------------------------------------------------------------------------

func TestRollback_PathTraversal_Returns400(t *testing.T) {
	// Same reasoning as TestAppIngress_PathTraversal_Returns400:
	// after mux cleaning, literal backslashes and percent-encoded forms
	// survive and reach the handler.
	tests := []struct {
		name    string
		appName string
	}{
		{"backslash", `foo\bar`},
		{"percent-encoded dots", "%2E%2E"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &stubRollbacker{resp: "should-not-be-called"}
			mux := newRollbackMux(svc)
			url := "/api/apps/" + tt.appName + "/rollback"
			req := httptest.NewRequest("POST", url, nil)
			req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
			}
			if svc.called {
				t.Errorf("RollbackDeployment should not have been called for traversal appName")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Rollback — 409 (tenant is disabled, issue #440 gate)
// ---------------------------------------------------------------------------
//
// TestRollback_TenantDisabled_Returns409 mirrors the activate-side test.
// The rollback path goes through the same lockTenantForUpdate gate as
// activate (PR #524), so the typed ErrTenantDisabled sentinel reaches
// the handler and must be mapped to 409 Conflict via httperror so the
// CLI can branch on the envelope instead of probing the request.

func TestRollback_TenantDisabled_Returns409(t *testing.T) {
	svc := &stubRollbacker{err: service.ErrTenantDisabled}
	mux := newRollbackMux(svc)

	req := httptest.NewRequest("POST", "/api/apps/myapp/rollback", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body: %s", rr.Code, rr.Body.String())
	}
	var got map[string]map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal envelope: %v; body: %s", err, rr.Body.String())
	}
	if got["error"]["code"] != "CONFLICT" {
		t.Errorf("error.code = %q, want CONFLICT; body: %s", got["error"]["code"], rr.Body.String())
	}
	if got["error"]["message"] != "tenant is disabled" {
		t.Errorf("error.message = %q, want %q; body: %s", got["error"]["message"], "tenant is disabled", rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "ErrTenantDisabled") {
		t.Errorf("body leaks sentinel: %s", rr.Body.String())
	}
}
