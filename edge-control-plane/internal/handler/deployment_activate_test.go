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
// Activate — 502 (post-commit NATS publish failed)
// ---------------------------------------------------------------------------

func TestActivate_PublishFailed_Returns502(t *testing.T) {
	// Service returns the wrapped ErrPublishFailed sentinel that
	// ActivateDeployment emits when PublishTaskUpdate fails after the
	// DB transaction has committed. Handler must surface this as 502
	// (not 500) so the client knows the DB write may have succeeded
	// and to treat it as an upstream-dependency failure.
	wrapped := fmt.Errorf("%w: %w", service.ErrPublishFailed, errors.New("nats unreachable"))
	svc := &stubActivator{err: wrapped}
	mux := newActivateMux(svc)

	req := httptest.NewRequest("POST", "/api/apps/myapp/activate/d_x", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "worker notification failed") {
		t.Errorf("body should explain 502, got %s", rr.Body.String())
	}
	// Typed-envelope assertions: the 502 body must conform to the
	// httperror.ErrorResponse shape so clients that parse the typed
	// envelope work across every status code. The regions arrays
	// live at the top level so a non-typed reader can still see them
	// (see writePublishFailureEnvelope).
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
	// Without a *service.PublishError, the regions arrays are empty.
	var top map[string]json.RawMessage
	if err := json.Unmarshal(rr.Body.Bytes(), &top); err != nil {
		t.Fatalf("unmarshal top-level: %v", err)
	}
	if _, ok := top["regions_published"]; !ok {
		t.Errorf("body missing top-level regions_published: %s", rr.Body.String())
	}
	if _, ok := top["regions_failed"]; !ok {
		t.Errorf("body missing top-level regions_failed: %s", rr.Body.String())
	}
}

// TestActivate_PublishFailed_TypedError_SurfacesRegionBreakdown covers
// the typed-error path: when the service returns a *service.PublishError
// (not just a wrapped sentinel), the handler surfaces the per-region
// breakdown alongside the typed envelope.
func TestActivate_PublishFailed_TypedError_SurfacesRegionBreakdown(t *testing.T) {
	pubErr := &service.PublishError{
		Published: []string{"us-east"},
		Failed:    []string{"eu-west"},
		Err:       service.ErrPublishFailed,
	}
	svc := &stubActivator{err: pubErr}
	mux := newActivateMux(svc)

	req := httptest.NewRequest("POST", "/api/apps/myapp/activate/d_x", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body: %s", rr.Code, rr.Body.String())
	}
	// Decode the body as both the typed envelope and a top-level map
	// so we can assert both halves of the shape.
	var env httperror.ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &env); err != nil {
		t.Fatalf("body is not typed envelope: %v; body: %s", err, rr.Body.String())
	}
	if env.Error.Code != httperror.CodeBadGateway {
		t.Errorf("error.code = %q, want BAD_GATEWAY", env.Error.Code)
	}
	var top struct {
		RegionsPublished []string `json:"regions_published"`
		RegionsFailed    []string `json:"regions_failed"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &top); err != nil {
		t.Fatalf("unmarshal top-level: %v", err)
	}
	if got, want := top.RegionsPublished, []string{"us-east"}; !equalStrings(got, want) {
		t.Errorf("regions_published = %v, want %v", got, want)
	}
	if got, want := top.RegionsFailed, []string{"eu-west"}; !equalStrings(got, want) {
		t.Errorf("regions_failed = %v, want %v", got, want)
	}
}

// equalStrings is a small helper that doesn't pull in slices.Equal
// just for this single comparison; keeps the test self-contained.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Activate — 500 (unexpected service error)
// ---------------------------------------------------------------------------

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
	// 502 must NOT be returned for an unrelated error.
	if rr.Code == http.StatusBadGateway {
		t.Errorf("status = 502, want 500; non-publish errors must not become 502")
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
