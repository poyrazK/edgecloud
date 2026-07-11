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
	// lastTenant / lastApp / lastDeploymentID / lastIdempotencyKey
	// record the arguments the handler passed so the disabled-tenant
	// test can assert that the request actually reached the service
	// layer before the gate fired, and that the Idempotency-Key header
	// is plumbed through (issue #439).
	lastTenant         string
	lastApp            string
	lastDeploymentID   string
	lastIdempotencyKey string
}

func (s *stubPromoter) PromoteDeployment(_ context.Context, tenantID, appName, deploymentID, idempotencyKey string) error {
	s.called = true
	s.lastTenant = tenantID
	s.lastApp = appName
	s.lastDeploymentID = deploymentID
	s.lastIdempotencyKey = idempotencyKey
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

// ---------------------------------------------------------------------------
// Promote — 404 (deployment not found, issue #546 follow-up)
// ---------------------------------------------------------------------------
//
// TestPromote_DeploymentNotFound_Returns404 pins the handler-level
// mapping for service.ErrDeploymentNotFound (typed sentinel at
// internal/service/deployment.go:227-229). PromoteDeployment returns
// this sentinel for both "row absent" and "wrong tenant" sub-cases;
// the handler collapses them into a single 404 with the canonical
// httperror envelope (code=NOT_FOUND, message="deployment not
// found"). Symmetric with the activate-side and rollback-side 404
// mappings at internal/handler/deployment.go:664 (activate) and
// :975 (rollback).
//
// This is the handler-level companion to the service-layer
// TestPromoteDeployment_DeploymentNotFound_404AtServiceLayer in
// internal/service/deployment_promote_test.go (PR #638, issue
// #546). The service test pins that PromoteDeployment returns the
// typed sentinel; this handler test pins that the handler maps
// the sentinel to a 404 envelope so the CLI can branch on
// status code without parsing the message string.

func TestPromote_DeploymentNotFound_Returns404(t *testing.T) {
	svc := &stubPromoter{err: service.ErrDeploymentNotFound}
	mux := newPromoteMux(svc)

	req := httptest.NewRequest("POST", "/api/apps/myapp/promote/d_x", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body: %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var got map[string]map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal envelope: %v; body: %s", err, rr.Body.String())
	}
	if got["error"]["code"] != "NOT_FOUND" {
		t.Errorf("error.code = %q, want NOT_FOUND; body: %s", got["error"]["code"], rr.Body.String())
	}
	if got["error"]["message"] != "deployment not found" {
		t.Errorf("error.message = %q, want %q; body: %s", got["error"]["message"], "deployment not found", rr.Body.String())
	}
	// The service must have been called before the mapping fired —
	// the mapping only matters if the request actually reaches the
	// service layer where the sentinel is returned.
	if !svc.called {
		t.Error("PromoteDeployment was not called")
	}
	// Body must not leak the raw sentinel — CLI parses the message,
	// not the Go symbol name.
	if strings.Contains(rr.Body.String(), "ErrDeploymentNotFound") {
		t.Errorf("body leaks sentinel: %s", rr.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Promote — Idempotency-Key plumbing (issue #439)
// ---------------------------------------------------------------------------
//
// PromoteDeployment delegates to the same activateDeployment
// inner function as Activate. Promoting with the same
// `Idempotency-Key` should be idempotent under the issue #439
// fix — the handler plumbs the header through and the service
// layer short-circuits on cache hit.

// TestPromote_HonoursIdempotencyKey asserts the header reaches the
// service layer untouched. Service-side replay short-circuit is
// exercised at the service layer (deployment_test.go).
func TestPromote_HonoursIdempotencyKey(t *testing.T) {
	svc := &stubPromoter{}
	mux := newPromoteMux(svc)

	const valid = "01234567-89ab-cdef-0123-456789abcdef"
	req := httptest.NewRequest("POST", "/api/apps/myapp/promote/d_x", nil)
	req.Header.Set("Idempotency-Key", valid)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	// Promote stub returns nil err on success — the handler writes a
	// 200 with the {"status":"promoted"} body shape.
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	if !svc.called {
		t.Fatal("PromoteDeployment was not called")
	}
	if svc.lastIdempotencyKey != valid {
		t.Errorf("PromoteDeployment called with idempotencyKey %q, want %q", svc.lastIdempotencyKey, valid)
	}
}

// TestPromote_MalformedIdempotencyKey_Returns400 mirrors the
// activate-side and rollback-side tests. The regex gate sits at
// the top of Promote (after path-segment validation), so a
// malformed key short-circuits before any service call. The
// stub's `called` flag must stay false.
func TestPromote_MalformedIdempotencyKey_Returns400(t *testing.T) {
	svc := &stubPromoter{}
	mux := newPromoteMux(svc)

	for _, bad := range []string{"short", "with spaces", "01234567-89ab-cdef-0123-456789abcdeX"} {
		req := httptest.NewRequest("POST", "/api/apps/myapp/promote/d_x", nil)
		req.Header.Set("Idempotency-Key", bad)
		req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Errorf("key %q: status = %d, want 400; body: %s", bad, rr.Code, rr.Body.String())
		}
		if svc.called {
			t.Errorf("key %q: PromoteDeployment must not be called for malformed key", bad)
		}
	}
}

// TestPromote_IdempotencyKeyMismatch_Returns422 covers the
// re-use-with-different-body path on the promote side. Body
// shape mirrors the activate-side and rollback-side 422
// (legacy bare http.Error — no httperror.UnprocessableEntityCtx
// helper exported today).
func TestPromote_IdempotencyKeyMismatch_Returns422(t *testing.T) {
	svc := &stubPromoter{err: service.ErrIdempotencyKeyMismatch}
	mux := newPromoteMux(svc)

	req := httptest.NewRequest("POST", "/api/apps/myapp/promote/d_x", nil)
	req.Header.Set("Idempotency-Key", "01234567-89ab-cdef-0123-456789abcdef")
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), service.ErrIdempotencyKeyMismatch.Error()) {
		t.Errorf("body must surface sentinel message %q; got %s", service.ErrIdempotencyKeyMismatch.Error(), rr.Body.String())
	}
	if !svc.called {
		t.Error("PromoteDeployment was not called — the 422 mapping only matters if the request reached the cache check")
	}
}
