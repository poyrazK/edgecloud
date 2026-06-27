package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
)

// stubWorkerStatusSvc is the minimum implementation of
// AppWorkerStatusLookup the WorkerStatusHandler needs. The handler's
// contract is purely "call svc.GetAppStatus and encode the result";
// all the service-level behavior (nil → "unknown", cross-tenant
// guard, etc.) is covered by service/worker_test.go.
type stubWorkerStatusSvc struct {
	row       *domain.AppWorkerStatus
	err       error
	called    bool
	gotTenant string
	gotApp    string
}

func (s *stubWorkerStatusSvc) GetAppStatus(_ context.Context, tenantID, appName string) (*domain.AppWorkerStatus, error) {
	s.called = true
	s.gotTenant = tenantID
	s.gotApp = appName
	return s.row, s.err
}

func newWorkerStatusMux(svc *stubWorkerStatusSvc) *http.ServeMux {
	h := NewWorkerStatusHandler(svc)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/apps/{appName}/status", h.Get)
	return mux
}

// ---------------------------------------------------------------------------
// Get — 200 (happy path: running)
// ---------------------------------------------------------------------------

// TestWorkerStatus_Get_Running pins the happy path: a worker has
// reported on the app, status = "running", and the handler returns
// the full envelope (region, worker_id, last_heartbeat).
func TestWorkerStatus_Get_Running(t *testing.T) {
	hb := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	stub := &stubWorkerStatusSvc{
		row: &domain.AppWorkerStatus{
			AppName:       "myapp",
			Status:        "running",
			LastHeartbeat: &hb,
			Region:        "us-east-1",
			WorkerID:      "w_us-east-1_h01",
		},
	}
	mux := newWorkerStatusMux(stub)

	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/status", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if stub.gotTenant != "t_test" {
		t.Errorf("tenant passed to service = %q, want t_test", stub.gotTenant)
	}
	if stub.gotApp != "myapp" {
		t.Errorf("app passed to service = %q, want myapp", stub.gotApp)
	}

	var got domain.AppWorkerStatus
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Status != "running" {
		t.Errorf("Status = %q, want running", got.Status)
	}
	if got.Region != "us-east-1" {
		t.Errorf("Region = %q, want us-east-1", got.Region)
	}
	if got.LastHeartbeat == nil {
		t.Error("LastHeartbeat is nil, want a value")
	}
}

// ---------------------------------------------------------------------------
// Get — 200 (crashed: the path that drives the CLI hint)
// ---------------------------------------------------------------------------

// TestWorkerStatus_Get_Crashed pins the path the issue #77 §5
// hint depends on: status = "crashed" + exit_code reach the wire
// intact. The CLI's `edge logs` reads this envelope and decides
// whether to print the rollback hint.
func TestWorkerStatus_Get_Crashed(t *testing.T) {
	hb := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	var exit int32 = 137
	stub := &stubWorkerStatusSvc{
		row: &domain.AppWorkerStatus{
			AppName:       "myapp",
			Status:        "crashed",
			LastHeartbeat: &hb,
			Region:        "us-east-1",
			WorkerID:      "w_us-east-1_h01",
			ExitCode:      &exit,
		},
	}
	mux := newWorkerStatusMux(stub)

	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/status", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var got domain.AppWorkerStatus
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Status != "crashed" {
		t.Errorf("Status = %q, want crashed", got.Status)
	}
	if got.ExitCode == nil || *got.ExitCode != 137 {
		t.Errorf("ExitCode = %v, want 137", got.ExitCode)
	}
}

// ---------------------------------------------------------------------------
// Get — 200 (unknown: no data, never deployed, or cross-tenant)
// ---------------------------------------------------------------------------

// TestWorkerStatus_Get_Unknown pins the no-data path: when no
// worker has reported on the app (or a cross-tenant request for an
// app that exists but is not the caller's), the service returns
// {Status: "unknown"} and the handler returns 200 with that body.
// 200-not-404 is the contract that prevents a probing tenant from
// distinguishing "no such app" from "exists but is not yours".
func TestWorkerStatus_Get_Unknown(t *testing.T) {
	stub := &stubWorkerStatusSvc{
		row: &domain.AppWorkerStatus{AppName: "myapp", Status: "unknown"},
	}
	mux := newWorkerStatusMux(stub)

	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/status", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var got domain.AppWorkerStatus
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Status != "unknown" {
		t.Errorf("Status = %q, want unknown", got.Status)
	}
	if got.LastHeartbeat != nil {
		t.Errorf("LastHeartbeat = %v, want nil", got.LastHeartbeat)
	}
}

// ---------------------------------------------------------------------------
// Get — 400 (path traversal)
// ---------------------------------------------------------------------------

func TestWorkerStatus_Get_PathTraversal_Returns400(t *testing.T) {
	cases := []struct {
		name    string
		appName string
	}{
		{"backslash", `foo\bar`},
		{"percent-encoded dots", "%2E%2E"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			stub := &stubWorkerStatusSvc{}
			mux := newWorkerStatusMux(stub)
			url := "/api/v1/apps/" + c.appName + "/status"
			req := httptest.NewRequest("GET", url, nil)
			req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
			}
			if stub.called {
				t.Error("service should not have been called for traversal appName")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Get — 500 (service error)
// ---------------------------------------------------------------------------

// TestWorkerStatus_Get_ServiceError_Returns500 pins the contract
// that a real DB error (the only non-nil error the service can
// return) maps to 500, not 200-with-unknown. A tenant who hits a
// 500 here knows it's a real outage, not "we don't know your app".
func TestWorkerStatus_Get_ServiceError_Returns500(t *testing.T) {
	stub := &stubWorkerStatusSvc{err: errors.New("db unreachable")}
	mux := newWorkerStatusMux(stub)

	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/status", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body: %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "db unreachable") {
		t.Errorf("body must not leak raw error: %s", rr.Body.String())
	}
}
