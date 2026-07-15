package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/jmoiron/sqlx"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// listableStubDeploymentRepo is a focused stub for the
// deployments-list path. It implements the deploymentRepoInterface
// methods that are reachable from DeploymentService.ListDeploymentsPaginated
// (issue #58, hard-cut in #709) and panics on every other method —
// a deliberate tripwire if a future change routes the list path
// through an unrelated repo method.
//
// Kept here (not in deployment_test.go) to avoid coupling the list
// tests to the idempotency fixture scaffolding that already lives
// there; the list tests just need a service with a repo seam.
//
// Issue #709 — afterID is now a text PK (`d_<uuid>`), not int64.
type listableStubDeploymentRepo struct {
	countByAppFn         func(ctx context.Context, tenantID, appName string) (int, error)
	listByAppPaginatedFn func(ctx context.Context, tenantID, appName string, afterTS time.Time, afterID string, limit int) ([]domain.Deployment, error)
}

func (l *listableStubDeploymentRepo) GetByID(_ context.Context, _ string) (*domain.Deployment, error) {
	panic("listableStubDeploymentRepo.GetByID: list path should not reach here")
}
func (l *listableStubDeploymentRepo) ListByApp(_ context.Context, _, _ string) ([]domain.Deployment, error) {
	panic("listableStubDeploymentRepo.ListByApp: list path should not reach here")
}
func (l *listableStubDeploymentRepo) CountByApp(ctx context.Context, tenantID, appName string) (int, error) {
	if l.countByAppFn != nil {
		return l.countByAppFn(ctx, tenantID, appName)
	}
	return 0, nil
}
func (l *listableStubDeploymentRepo) ListByAppPaginated(
	ctx context.Context, tenantID, appName string, afterTS time.Time, afterID string, limit int,
) ([]domain.Deployment, error) {
	if l.listByAppPaginatedFn != nil {
		return l.listByAppPaginatedFn(ctx, tenantID, appName, afterTS, afterID, limit)
	}
	return nil, nil
}
func (l *listableStubDeploymentRepo) Create(_ context.Context, _ *domain.Deployment) error {
	panic("listableStubDeploymentRepo.Create: list path should not reach here")
}
func (l *listableStubDeploymentRepo) DeleteByID(_ context.Context, _ string) error {
	panic("listableStubDeploymentRepo.DeleteByID: list path should not reach here")
}
func (l *listableStubDeploymentRepo) WithTx(_ *sqlx.Tx) *repository.DeploymentRepository {
	panic("listableStubDeploymentRepo.WithTx: list path should not reach here")
}

// newDeploymentListMux wires a GET /api/v1/list/{appName} route on a
// DeploymentService whose deploymentRepo is the supplied stub. The
// service's other dependencies stay nil — the list path doesn't
// reach them.
func newDeploymentListMux(stub *listableStubDeploymentRepo) *http.ServeMux {
	svc := &service.DeploymentService{}
	setDeploymentRepoForTest(svc, stub)
	h := NewDeploymentHandler(svc, nil, nil, nil, "")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/list/{appName}", h.List)
	return mux
}

// setDeploymentRepoForTest is a duplicate of setUnexportedField's
// payload (`reflect.NewAt`) specialized to the deploymentRepo field.
// The original helper lives further down in deployment_test.go; this
// narrower copy keeps this file self-contained without renames.
func setDeploymentRepoForTest(svc *service.DeploymentService, stub *listableStubDeploymentRepo) {
	v := reflect.ValueOf(svc).Elem().FieldByName("deploymentRepo")
	if !v.IsValid() {
		panic("setDeploymentRepoForTest: no field deploymentRepo on service.DeploymentService")
	}
	reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).
		Elem().
		Set(reflect.ValueOf(stub))
}

// TestDeploymentHandler_List_FirstPage_HardCut pins the post-#709
// wire shape: no `next_offset` on any page; `next_cursor` is the
// only pagination field. `total` is preserved because the CLI
// renders "N deployments" in the header. `limit` is set even when
// the query string is absent (default 20).
func TestDeploymentHandler_List_FirstPage_HardCut(t *testing.T) {
	ts := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	stub := &listableStubDeploymentRepo{
		countByAppFn: func(_ context.Context, _, _ string) (int, error) { return 100, nil },
		listByAppPaginatedFn: func(_ context.Context, _, _ string, _ time.Time, _ string, _ int) ([]domain.Deployment, error) {
			return []domain.Deployment{{
				ID: "d_first", TenantID: "t_test", AppName: "myapp", Status: "deployed",
				Hash: "abc123", Regions: nil, CreatedAt: ts,
			}}, nil
		},
	}
	mux := newDeploymentListMux(stub)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/list/myapp", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if v, ok := resp["total"].(float64); !ok || int(v) != 100 {
		t.Errorf("total = %v, want 100", resp["total"])
	}
	if v, ok := resp["limit"].(float64); !ok || int(v) != 20 {
		t.Errorf("limit = %v, want 20 (default)", resp["limit"])
	}
	// #709 — next_offset is GONE from the wire. If a regression
	// re-introduces it, this assertion fails loudly.
	if _, hasOff := resp["next_offset"]; hasOff {
		t.Errorf("response has 'next_offset' on first page; #709 retired it")
	}
	items, ok := resp["items"].([]interface{})
	if !ok || len(items) != 1 {
		t.Fatalf("items = %v, want length-1 list", resp["items"])
	}
}

// TestDeploymentHandler_List_Offset_Returns_400 pins the #709
// hard-cut on the request side: any non-empty `?offset=` returns
// 400 immediately. The compat release from #58 is over; stale CLI
// invocations fail loudly instead of silently restarting the page
// from zero.
func TestDeploymentHandler_List_Offset_Returns_400(t *testing.T) {
	stub := &listableStubDeploymentRepo{} // Repo MUST NOT be called.
	mux := newDeploymentListMux(stub)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/list/myapp?offset=2&limit=3", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (offset retired in #709); body: %s", rr.Code, rr.Body.String())
	}
}

// TestDeploymentHandler_List_CursorDriven_OmitsNextOffset pins the
// cursor-only contract: when `?cursor=` is supplied, the response
// MUST NOT carry `next_offset` (the field is gone from the wire).
// The cursor decodes successfully, the repo returns one row, and
// the handler response is what we're asserting on — not whether the
// cursor actually points at a row.
//
// Issue #709 — the embedded id is now a text PK, so the cursor
// fixture is the v1 envelope `{"v":1,"p":{"ts":"...","id":"d_42"}}`.
func TestDeploymentHandler_List_CursorDriven_OmitsNextOffset(t *testing.T) {
	ts := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	stub := &listableStubDeploymentRepo{
		countByAppFn: func(_ context.Context, _, _ string) (int, error) { return 7, nil },
		listByAppPaginatedFn: func(_ context.Context, _, _ string, _ time.Time, _ string, _ int) ([]domain.Deployment, error) {
			return []domain.Deployment{{
				ID: "d_after", TenantID: "t_test", AppName: "myapp", CreatedAt: ts,
			}}, nil
		},
	}
	mux := newDeploymentListMux(stub)

	// {"v":1,"p":{"ts":"2026-07-15T12:34:56Z","id":"d_42"}} — base64url
	// (RawURLEncoding, no padding).
	cursor := "eyJ2IjoxLCJwIjp7InRzIjoiMjAzMS0wOC0yMFQwMTowMTowMVoiLCJpZCI6ImRfNTAifX0"

	req := httptest.NewRequest(http.MethodGet, "/api/v1/list/myapp?cursor="+cursor, nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, hasOff := resp["next_offset"]; hasOff {
		t.Errorf("response has 'next_offset' on cursor-driven page; #709 retired it from the wire")
	}
}

// TestDeploymentHandler_List_BadCursor_400 — a malformed cursor
// surfaces as 400 with the generic message, never 500.
func TestDeploymentHandler_List_BadCursor_400(t *testing.T) {
	stub := &listableStubDeploymentRepo{
		countByAppFn: func(_ context.Context, _, _ string) (int, error) { return 0, nil },
		// Repo methods won't be reached — the service decodes the cursor first.
	}
	mux := newDeploymentListMux(stub)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/list/myapp?cursor=bm90LWpzb24tYXQtYWxs", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
}

// TestDeploymentHandler_List_LimitClampedAt100 verifies the upper
// clamp: requesting limit=999 yields an effective limit=100 on the
// wire. Mirrors the apps handler's clamp test in issue #58 commit 4.
func TestDeploymentHandler_List_LimitClampedAt100(t *testing.T) {
	var seenLimit int
	stub := &listableStubDeploymentRepo{
		countByAppFn: func(_ context.Context, _, _ string) (int, error) { return 0, nil },
		listByAppPaginatedFn: func(_ context.Context, _, _ string, _ time.Time, _ string, limit int) ([]domain.Deployment, error) {
			seenLimit = limit
			return nil, nil
		},
	}
	mux := newDeploymentListMux(stub)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/list/myapp?limit=999", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	// service fetches limit+1 internally, so the captured limit is 101.
	if seenLimit != 101 {
		t.Errorf("captured repo limit = %d, want 101 (100+1)", seenLimit)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if v, ok := resp["limit"].(float64); !ok || int(v) != 100 {
		t.Errorf("response limit = %v, want 100 (clamped)", resp["limit"])
	}
}

// TestDeploymentHandler_List_CursorWithLimit_200 — combining
// `?cursor=` and `?limit=` is allowed post-#709; only `?offset=`
// is rejected. This pins the cursor chain still works alongside
// the explicit limit override.
func TestDeploymentHandler_List_CursorWithLimit_200(t *testing.T) {
	ts := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	stub := &listableStubDeploymentRepo{
		countByAppFn: func(_ context.Context, _, _ string) (int, error) { return 3, nil },
		listByAppPaginatedFn: func(_ context.Context, _, _ string, _ time.Time, _ string, _ int) ([]domain.Deployment, error) {
			return []domain.Deployment{{
				ID: "d_after", TenantID: "t_test", AppName: "myapp", CreatedAt: ts,
			}}, nil
		},
	}
	mux := newDeploymentListMux(stub)

	// Same cursor fixture as the sibling cursor-driven test.
	cursor := "eyJ2IjoxLCJwIjp7InRzIjoiMjAzMS0wOC0yMFQwMTowMTowMVoiLCJpZCI6ImRfNTAifX0"

	req := httptest.NewRequest(http.MethodGet, "/api/v1/list/myapp?cursor="+cursor+"&limit=5", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if v, ok := resp["limit"].(float64); !ok || int(v) != 5 {
		t.Errorf("limit = %v, want 5 (caller override)", resp["limit"])
	}
	if _, hasOff := resp["next_offset"]; hasOff {
		t.Errorf("response has 'next_offset'; #709 retired it")
	}
}
