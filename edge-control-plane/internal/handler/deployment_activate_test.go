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

// stubActivator is the minimum implementation of deploymentActivator
// needed by the Activate handler. It records the args it was called
// with so tests can assert the tenant filter is applied correctly,
// and returns whatever response/err the test sets.
type stubActivator struct {
	err    error
	called bool
	// lastTenant / lastApp / lastDeploymentID record the arguments the
	// handler passed so tests can assert that the tenant context (not
	// the URL) wins and that the path values reach the service layer.
	lastTenant       string
	lastApp          string
	lastDeploymentID string
}

func (s *stubActivator) ActivateDeployment(_ context.Context, tenantID, appName, deploymentID string) error {
	s.called = true
	s.lastTenant = tenantID
	s.lastApp = appName
	s.lastDeploymentID = deploymentID
	return s.err
}

// newActivateMux wires a single POST /api/apps/{appName}/activate/{deploymentID}
// route through a real *http.ServeMux so r.PathValue("appName") and
// r.PathValue("deploymentID") are populated the same way they are in
// production. workerSvc, deploymentSvc, and rollbackSvc are nil because
// the Activate body never touches them.
func newActivateMux(svc *stubActivator) *http.ServeMux {
	h := &DeploymentHandler{
		workerSvc:     nil,
		deploymentSvc: nil,
		rollbackSvc:   nil,
		activateSvc:   svc,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/apps/{appName}/activate/{deploymentID}", h.Activate)
	return mux
}

// ---------------------------------------------------------------------------
// Activate — 200 (happy path)
// ---------------------------------------------------------------------------

func TestActivate_HappyPath_Returns200(t *testing.T) {
	svc := &stubActivator{}
	mux := newActivateMux(svc)

	req := httptest.NewRequest("POST", "/api/apps/myapp/activate/d_x", nil)
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
	if got["status"] != "activated" {
		t.Errorf("status = %q, want activated", got["status"])
	}
	// The handler must propagate the tenant id from the auth context,
	// not from the URL — this is what keeps cross-tenant activation
	// from working.
	if !svc.called {
		t.Fatal("ActivateDeployment was not called")
	}
	if svc.lastTenant != "t_test" {
		t.Errorf("ActivateDeployment called with tenant %q, want t_test", svc.lastTenant)
	}
	if svc.lastApp != "myapp" {
		t.Errorf("ActivateDeployment called with app %q, want myapp", svc.lastApp)
	}
	if svc.lastDeploymentID != "d_x" {
		t.Errorf("ActivateDeployment called with deploymentID %q, want d_x", svc.lastDeploymentID)
	}
}

// ---------------------------------------------------------------------------
// Activate — 500 (unexpected service error)
// ---------------------------------------------------------------------------
//
// Note: prior versions of this file asserted a 502 path for post-commit
// NATS publish failures. Issue #42 made that path obsolete: durable
// publish is now owned by service.OutboxDrainer, which writes the
// task_update inside the same transaction as the active_deployments
// mutation and relays via a background drainer. ActivateDeployment
// therefore returns nil on a successful enqueue even if NATS is
// unreachable at request time — the only error surface left at the
// handler is unexpected 500s.

func TestActivate_ServiceError_Returns500(t *testing.T) {
	svc := &stubActivator{err: errors.New("db unreachable")}
	mux := newActivateMux(svc)

	req := httptest.NewRequest("POST", "/api/apps/myapp/activate/d_x", nil)
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
// Activate — 400 (path traversal in appName or deploymentID)
// ---------------------------------------------------------------------------

// TestActivate_InvalidAppName_Returns400 verifies that path-traversal
// characters in the appName path segment are rejected before reaching
// the service layer. The deployment id flows into the worker's
// /registry/{tenant}/{app}/{deployment}.wasm path — a "/" or ".."
// would let a caller reference arbitrary files on the worker.
//
// Note: Go's net/http mux percent-decodes path values before matching
// and normalizes "/.." segments before routing — so a literal
// "../etc" in the URL becomes a redirect, not a 400. The realistic
// attack is percent-encoded traversal that decodes to ".." or "/"
// AFTER the mux has parsed the segments; those reach the handler
// post-decode and must be rejected here.
func TestActivate_InvalidAppName_Returns400(t *testing.T) {
	svc := &stubActivator{}
	mux := newActivateMux(svc)

	for _, name := range []string{"%2e%2e", "foo%2fbar", "foo%5cbar"} {
		req := httptest.NewRequest("POST", "/api/apps/"+name+"/activate/d_x", nil)
		req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Errorf("appName %q: status = %d, want 400; body: %s", name, rr.Code, rr.Body.String())
		}
		if svc.called {
			t.Errorf("appName %q: ActivateDeployment must not be called for invalid appName", name)
		}
	}
}

// TestActivate_InvalidDeploymentID_Returns400 mirrors the appName
// check for the deploymentID path segment. deploymentID feeds into
// file paths on the worker download side and into SQL queries here,
// so a "/" or ".." injection would be at minimum a logging/UX bug,
// at worst a cross-tenant read primitive. See TestActivate_InvalidAppName_Returns400
// for why we use percent-encoded inputs.
func TestActivate_InvalidDeploymentID_Returns400(t *testing.T) {
	svc := &stubActivator{}
	mux := newActivateMux(svc)

	for _, id := range []string{"%2e%2e", "d_x%2f..%2fd_y", "d_x%5cd_y"} {
		req := httptest.NewRequest("POST", "/api/apps/myapp/activate/"+id, nil)
		req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Errorf("deploymentID %q: status = %d, want 400; body: %s", id, rr.Code, rr.Body.String())
		}
		if svc.called {
			t.Errorf("deploymentID %q: ActivateDeployment must not be called for invalid deploymentID", id)
		}
	}
}

// ---------------------------------------------------------------------------
// Activate — 409 (tenant is disabled, issue #440 gate)
// ---------------------------------------------------------------------------
//
// TestActivate_TenantDisabled_Returns409 verifies that
// service.ErrTenantDisabled bubbles up from ActivateDeployment as a 409
// Conflict with the canonical httperror envelope (code=CONFLICT). This
// is the surface contract that lets the CLI / operator tooling
// distinguish "tenant is locked, retry after the operator un-disables"
// from a generic 500 — see the issue #440 note in deployment.go's
// Activate handler header comment.
//
// The disabled-vs-activate race window is closed by lockTenantForUpdate
// at the service layer; the handler's job is just to surface the typed
// sentinel without leaking it.

func TestActivate_TenantDisabled_Returns409(t *testing.T) {
	svc := &stubActivator{err: service.ErrTenantDisabled}
	mux := newActivateMux(svc)

	req := httptest.NewRequest("POST", "/api/apps/myapp/activate/d_x", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body: %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
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
	// The service must have been called before the gate fired — the
	// mapping only matters if the request actually reaches the
	// tenant-locking tx.
	if !svc.called {
		t.Error("ActivateDeployment was not called")
	}
	// Body must not leak the raw sentinel or the underlying DB driver error.
	if strings.Contains(rr.Body.String(), "ErrTenantDisabled") {
		t.Errorf("body leaks sentinel: %s", rr.Body.String())
	}
}
