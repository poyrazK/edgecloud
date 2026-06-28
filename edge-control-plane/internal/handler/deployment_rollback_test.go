package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
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
// Rollback — 502 (post-commit NATS publish failed)
// ---------------------------------------------------------------------------

func TestRollback_PublishFailed_Returns502(t *testing.T) {
	// Service returns the wrapped ErrPublishFailed sentinel that
	// RollbackDeployment emits when PublishTaskUpdate fails after the
	// DB transaction has committed. Handler must surface this as 502
	// (not 500) so the client knows the DB write may have succeeded
	// and to treat it as an upstream-dependency failure.
	wrapped := fmt.Errorf("%w: %w", service.ErrPublishFailed, errors.New("nats unreachable"))
	svc := &stubRollbacker{err: wrapped}
	mux := newRollbackMux(svc)

	req := httptest.NewRequest("POST", "/api/apps/myapp/rollback", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "worker notification failed") {
		t.Errorf("body should explain 502, got %s", rr.Body.String())
	}
	// Typed-envelope assertions (issue #127 follow-ups): the 502
	// body must conform to httperror.ErrorResponse so clients that
	// parse the typed envelope work across every status code.
	var env httperror.ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &env); err != nil {
		t.Fatalf("body is not typed envelope: %v; body: %s", err, rr.Body.String())
	}
	if env.Error.Code != httperror.CodeBadGateway {
		t.Errorf("error.code = %q, want BAD_GATEWAY", env.Error.Code)
	}
	if !strings.Contains(env.Error.Message, "worker notification failed") {
		t.Errorf("error.message = %q, want it to mention worker notification", env.Error.Message)
	}
	// Body must not leak the sentinel or the raw NATS error.
	if strings.Contains(rr.Body.String(), "nats unreachable") {
		t.Errorf("body leaks raw error: %s", rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "ErrPublishFailed") {
		t.Errorf("body leaks sentinel: %s", rr.Body.String())
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
