package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// stubPromoter is the minimum implementation of deploymentPromoter
// needed by the Promote handler. It records the args it was called
// with so tests can assert the tenant filter is applied correctly,
// and returns whatever err the test sets.
//
// Note: there is no happy-path coverage for PromoteDeployment at any
// layer today. Adding TestPromoteDeployment_HappyPath at the service
// layer (mirroring the TestActivateDeployment_* family) is tracked as
// a follow-up; this file ships only the disabled-tenant mapping test
// because that is what the PR scope required.
type stubPromoter struct {
	err    error
	called bool
	// lastTenant / lastApp / lastDeploymentID record the arguments the
	// handler passed so the disabled-tenant test can assert that the
	// request actually reached the service layer before the gate fired.
	lastTenant       string
	lastApp          string
	lastDeploymentID string
}

func (s *stubPromoter) PromoteDeployment(_ context.Context, tenantID, appName, deploymentID string) error {
	s.called = true
	s.lastTenant = tenantID
	s.lastApp = appName
	s.lastDeploymentID = deploymentID
	return s.err
}

// newPromoteMux wires a single POST /api/apps/{appName}/promote/{deploymentID}
// route through a real *http.ServeMux so r.PathValue("appName") and
// r.PathValue("deploymentID") are populated the same way they are in
// production. workerSvc, deploymentSvc, rollbackSvc, and activateSvc are
// nil because Promote never touches them.
func newPromoteMux(svc *stubPromoter) *http.ServeMux {
	h := &DeploymentHandler{
		workerSvc:     nil,
		deploymentSvc: nil,
		rollbackSvc:   nil,
		activateSvc:   nil,
		promoteSvc:    svc,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/apps/{appName}/promote/{deploymentID}", h.Promote)
	return mux
}

// ---------------------------------------------------------------------------
// Promote — 409 (tenant is disabled, issue #440 gate)
// ---------------------------------------------------------------------------
//
// TestPromote_TenantDisabled_Returns409 verifies that
// service.ErrTenantDisabled bubbles up from PromoteDeployment as a 409
// Conflict with the canonical httperror envelope (code=CONFLICT).
//
// Promote delegates to the same activateDeployment inner function as
// Activate, so the lockTenantForUpdate gate (issue #440) fires
// identically and the typed sentinel reaches this handler. Symmetry
// with the Activate/Rollback mappings keeps the public contract
// consistent across all three deployment-mutating endpoints — every
// write that gates on tenant-active surfaces a 409 with the same body
// shape, so the CLI can implement one retry-on-disabled branch and
// reuse it for activate, rollback, and promote.

func TestPromote_TenantDisabled_Returns409(t *testing.T) {
	svc := &stubPromoter{err: service.ErrTenantDisabled}
	mux := newPromoteMux(svc)

	req := httptest.NewRequest("POST", "/api/apps/myapp/promote/d_x", nil)
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
		t.Error("PromoteDeployment was not called")
	}
	// Body must not leak the raw sentinel.
	if strings.Contains(rr.Body.String(), "ErrTenantDisabled") {
		t.Errorf("body leaks sentinel: %s", rr.Body.String())
	}
}
