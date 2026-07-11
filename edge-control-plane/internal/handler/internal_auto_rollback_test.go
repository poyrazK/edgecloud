package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// stubAutoRollbacker is the minimum implementation of the production
// autoRollbacker interface (declared in internal.go) that
// InternalHandler.AutoRollback needs. Mirrors stubRollbacker /
// stubActivator in the sibling Activate and Rollback test files.
//
// The production interface also requires GetDeployment + GetArtifact
// because Download uses them. These stub methods fail loudly if any
// AutoRollback test ever accidentally exercises the Download path;
// AutoRollback tests should only assert on s.called / s.lastTenant /
// s.lastApp / s.resp / s.err.
type stubAutoRollbacker struct {
	resp   string
	err    error
	called bool
	// lastTenant / lastApp record the args the handler passed.
	lastTenant string
	lastApp    string
}

func (s *stubAutoRollbacker) RollbackDeployment(_ context.Context, tenantID, appName, idempotencyKey string) (string, error) {
	s.called = true
	s.lastTenant = tenantID
	s.lastApp = appName
	_ = idempotencyKey
	return s.resp, s.err
}

func (s *stubAutoRollbacker) GetDeployment(_ context.Context, _, _ string) (*domain.Deployment, error) {
	panic("stubAutoRollbacker.GetDeployment called from AutoRollback test — wrong code path exercised")
}

func (s *stubAutoRollbacker) GetArtifact(_ context.Context, _, _, _, _ string) (io.ReadCloser, error) {
	panic("stubAutoRollbacker.GetArtifact called from AutoRollback test — wrong code path exercised")
}

// newInternalAutoRollbackMux wires the route through a real
// *http.ServeMux AND the production InternalHandler. r.PathValue is
// populated the same way as production. No parallel stub handler
// to drift out of sync.
//
// This test bypasses the auth middleware that wraps /api/internal/
// in production (cmd/api/main.go applies middleware.WorkerAuth,
// which today is a no-op because the JWT signing path is dead code
// — see the comment in cmd/api/main.go). We assert handler-level
// contracts, not middleware behavior.
func newInternalAutoRollbackMux(svc *stubAutoRollbacker) *http.ServeMux {
	h := &InternalHandler{deploymentSvc: svc}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/internal/apps/{appName}/auto-rollback", h.AutoRollback)
	return mux
}

// ---------------------------------------------------------------------------
// 200 — happy path
// ---------------------------------------------------------------------------

func TestInternalAutoRollback_HappyPath_Returns200WithDeploymentID(t *testing.T) {
	svc := &stubAutoRollbacker{resp: "d_prev"}
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
	svc := &stubAutoRollbacker{err: service.ErrNoLastGood}
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
	svc := &stubAutoRollbacker{err: service.ErrNoActiveDeployment}
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
	svc := &stubAutoRollbacker{err: service.ErrAutoRollbackDisabled}
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
// 500 — unexpected service error
// ---------------------------------------------------------------------------
//
// Note: prior versions of this file asserted a 502 path for post-commit
// NATS publish failures. Issue #42 made that path obsolete: durable
// publish is now owned by service.OutboxDrainer (see CLAUDE.md schema
// table). The handler only surfaces pre-flight 4xx and unexpected 5xx
// errors; NATS unreachability becomes a backlogged `pending` outbox row.

func TestInternalAutoRollback_ServiceError_Returns500(t *testing.T) {
	svc := &stubAutoRollbacker{err: errors.New("db unreachable")}
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

// ---------------------------------------------------------------------------
// 400 — bad request body
// ---------------------------------------------------------------------------

func TestInternalAutoRollback_BadBody_Returns400(t *testing.T) {
	svc := &stubAutoRollbacker{}
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
	svc := &stubAutoRollbacker{}
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
	svc := &stubAutoRollbacker{}
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
