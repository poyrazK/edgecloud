package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// autoRollbackSvc is the minimum surface InternalHandler.AutoRollback
// needs. Mirrors stubRollbacker in deployment_rollback_test.go — kept
// package-local so the test file can stand alone without standing up a
// full DeploymentService (DB + NATS + publisher + artifact store).
type autoRollbackSvc struct {
	resp   string
	err    error
	called bool
	// lastTenant / lastApp record the args the handler passed.
	lastTenant string
	lastApp    string
}

func (s *autoRollbackSvc) RollbackDeployment(_ context.Context, tenantID, appName string) (string, error) {
	s.called = true
	s.lastTenant = tenantID
	s.lastApp = appName
	return s.resp, s.err
}

// autoRollbacker is the narrow contract serveAutoRollbackWithStub
// dispatches against. Matches service.RollbackDeployment's signature
// so the production handler and the test stub are interchangeable.
type autoRollbacker interface {
	RollbackDeployment(ctx context.Context, tenantID, appName string) (string, error)
}

// serveAutoRollbackWithStub mirrors InternalHandler.AutoRollback but
// takes a narrow interface instead of *service.DeploymentService, so
// tests don't need a live DB or NATS publisher. The production
// handler's deploymentSvc field is concrete, so we can't stub it
// directly without pulling in those dependencies.
//
// Kept package-local (lowercase) to scope this test-only helper to
// the handler package. Behavior matches InternalHandler.AutoRollback
// byte-for-byte; if you change one, change the other.
func serveAutoRollbackWithStub(w http.ResponseWriter, r *http.Request, svc autoRollbacker) {
	appName := r.PathValue("appName")

	var req AutoRollbackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error": "invalid request body"}`, http.StatusBadRequest)
		return
	}
	if req.TenantID == "" || req.AppName == "" {
		http.Error(w, `{"error": "tenant_id and app_name are required"}`, http.StatusBadRequest)
		return
	}
	if appName == "" {
		appName = req.AppName
	}
	if appName != req.AppName {
		http.Error(w, `{"error": "app_name in URL and body must match"}`, http.StatusBadRequest)
		return
	}

	newID, err := svc.RollbackDeployment(r.Context(), req.TenantID, appName)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrNoLastGood):
			http.Error(w, `{"error": "no previous deployment to roll back to"}`, http.StatusConflict)
		case errors.Is(err, service.ErrNoActiveDeployment):
			http.Error(w, `{"error": "no active deployment"}`, http.StatusNotFound)
		case errors.Is(err, service.ErrAutoRollbackDisabled):
			http.Error(w, `{"error": "auto-rollback disabled for this app"}`, http.StatusPreconditionFailed)
		case errors.Is(err, service.ErrPublishFailed):
			http.Error(w, `{"error": "rollback committed but worker notification failed; please retry"}`, http.StatusBadGateway)
		default:
			http.Error(w, `{"error": "auto-rollback failed"}`, http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{"deployment_id": newID}); err != nil {
		log.Printf("serveAutoRollbackWithStub: failed to encode response: %v", err)
	}
}

// newInternalAutoRollbackMux wires the route through a real
// *http.ServeMux so r.PathValue("appName") is populated the same way
// it is in production. The handler dispatch goes through
// serveAutoRollbackWithStub (see comment on that helper for why we
// don't call InternalHandler.AutoRollback directly).
//
// This test bypasses the auth middleware that wraps /api/internal/
// in production (cmd/api/main.go applies middleware.WorkerAuth,
// which today is a no-op because the JWT signing path is dead code
// — see the comment in cmd/api/main.go). We assert handler-level
// contracts, not middleware behavior.
func newInternalAutoRollbackMux(svc *autoRollbackSvc) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/internal/apps/{appName}/auto-rollback", func(w http.ResponseWriter, r *http.Request) {
		serveAutoRollbackWithStub(w, r, svc)
	})
	return mux
}

// ---------------------------------------------------------------------------
// 200 — happy path
// ---------------------------------------------------------------------------

func TestInternalAutoRollback_HappyPath_Returns200WithDeploymentID(t *testing.T) {
	svc := &autoRollbackSvc{resp: "d_prev"}
	mux := newInternalAutoRollbackMux(svc)

	body := strings.NewReader(`{"tenant_id":"t_test","app_name":"myapp","current_deployment_id":"d_broken","restart_count":5}`)
	req := httptest.NewRequest("POST", "/api/internal/apps/myapp/auto-rollback", body)
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
// 409 — no last-good pointer
// ---------------------------------------------------------------------------

func TestInternalAutoRollback_NoLastGood_Returns409(t *testing.T) {
	svc := &autoRollbackSvc{err: service.ErrNoLastGood}
	mux := newInternalAutoRollbackMux(svc)

	body := strings.NewReader(`{"tenant_id":"t_test","app_name":"myapp"}`)
	req := httptest.NewRequest("POST", "/api/internal/apps/myapp/auto-rollback", body)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "no previous deployment") {
		t.Errorf("body should contain the typed 409 message, got %s", rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "ErrNoLastGood") {
		t.Errorf("body leaks sentinel: %s", rr.Body.String())
	}
}

// ---------------------------------------------------------------------------
// 404 — no active deployment row
// ---------------------------------------------------------------------------

func TestInternalAutoRollback_NoActiveDeployment_Returns404(t *testing.T) {
	svc := &autoRollbackSvc{err: service.ErrNoActiveDeployment}
	mux := newInternalAutoRollbackMux(svc)

	body := strings.NewReader(`{"tenant_id":"t_test","app_name":"missing"}`)
	req := httptest.NewRequest("POST", "/api/internal/apps/missing/auto-rollback", body)
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
// 412 — auto-rollback disabled on the active_deployments row
// ---------------------------------------------------------------------------

func TestInternalAutoRollback_Disabled_Returns412(t *testing.T) {
	// Service surfaces the repo's string-matched sentinel via the
	// ErrAutoRollbackDisabled re-export. The handler distinguishes
	// this from ErrNoLastGood so the worker can tell a config issue
	// apart from a "nothing to roll back to" issue.
	svc := &autoRollbackSvc{err: service.ErrAutoRollbackDisabled}
	mux := newInternalAutoRollbackMux(svc)

	body := strings.NewReader(`{"tenant_id":"t_test","app_name":"myapp"}`)
	req := httptest.NewRequest("POST", "/api/internal/apps/myapp/auto-rollback", body)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusPreconditionFailed {
		t.Fatalf("status = %d, want 412; body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "auto-rollback disabled") {
		t.Errorf("body should explain 412, got %s", rr.Body.String())
	}
}

// ---------------------------------------------------------------------------
// 502 — post-commit NATS publish failed
// ---------------------------------------------------------------------------

func TestInternalAutoRollback_PublishFailed_Returns502(t *testing.T) {
	// Same multi-%w wrapping that ActivateDeployment / RollbackDeployment
	// emit. Handler must distinguish from a 500 because the DB row may
	// already be swapped — a retry on 500 would re-swap or 409.
	wrapped := fmt.Errorf("%w: %w", service.ErrPublishFailed, errors.New("nats unreachable"))
	svc := &autoRollbackSvc{err: wrapped}
	mux := newInternalAutoRollbackMux(svc)

	body := strings.NewReader(`{"tenant_id":"t_test","app_name":"myapp"}`)
	req := httptest.NewRequest("POST", "/api/internal/apps/myapp/auto-rollback", body)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "worker notification failed") {
		t.Errorf("body should explain 502, got %s", rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "nats unreachable") {
		t.Errorf("body leaks raw error: %s", rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "ErrPublishFailed") {
		t.Errorf("body leaks sentinel: %s", rr.Body.String())
	}
}

// ---------------------------------------------------------------------------
// 400 — bad request body
// ---------------------------------------------------------------------------

func TestInternalAutoRollback_BadBody_Returns400(t *testing.T) {
	svc := &autoRollbackSvc{}
	mux := newInternalAutoRollbackMux(svc)

	body := strings.NewReader(`not json`)
	req := httptest.NewRequest("POST", "/api/internal/apps/myapp/auto-rollback", body)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	if svc.called {
		t.Error("RollbackDeployment should not have been called for malformed body")
	}
}

func TestInternalAutoRollback_MissingTenant_Returns400(t *testing.T) {
	svc := &autoRollbackSvc{}
	mux := newInternalAutoRollbackMux(svc)

	// app_name missing — handler must reject before calling the service.
	body := strings.NewReader(`{"tenant_id":"t_test"}`)
	req := httptest.NewRequest("POST", "/api/internal/apps/myapp/auto-rollback", body)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	if svc.called {
		t.Error("RollbackDeployment should not have been called when app_name is missing")
	}
}

func TestInternalAutoRollback_PathBodyMismatch_Returns400(t *testing.T) {
	svc := &autoRollbackSvc{}
	mux := newInternalAutoRollbackMux(svc)

	// URL says "foo" but body says "bar" — must reject to avoid
	// hitting the wrong app's state.
	body := strings.NewReader(`{"tenant_id":"t_test","app_name":"bar"}`)
	req := httptest.NewRequest("POST", "/api/internal/apps/foo/auto-rollback", body)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	if svc.called {
		t.Error("RollbackDeployment should not have been called on path/body mismatch")
	}
}

// ---------------------------------------------------------------------------
// 500 — unexpected service error
// ---------------------------------------------------------------------------

func TestInternalAutoRollback_ServiceError_Returns500(t *testing.T) {
	svc := &autoRollbackSvc{err: errors.New("db unreachable")}
	mux := newInternalAutoRollbackMux(svc)

	body := strings.NewReader(`{"tenant_id":"t_test","app_name":"myapp"}`)
	req := httptest.NewRequest("POST", "/api/internal/apps/myapp/auto-rollback", body)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body: %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "db unreachable") {
		t.Errorf("body must not leak raw error, got %s", rr.Body.String())
	}
}
