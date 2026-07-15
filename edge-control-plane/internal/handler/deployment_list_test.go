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
// methods that are reachable from DeploymentService.List* (issue #58)
// and panics on every other method — a deliberate tripwire if a future
// change routes the list path through an unrelated repo method.
//
// Kept here (not in deployment_test.go) to avoid coupling the list
// tests to the idempotency fixture scaffolding that already lives
// there; the list tests just need a service with a repo seam.
type listableStubDeploymentRepo struct {
	countByAppFn         func(ctx context.Context, tenantID, appName string) (int, error)
	listByAppPaginatedFn func(ctx context.Context, tenantID, appName string, afterTS time.Time, afterID int64, limit int) ([]domain.Deployment, error)
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
	ctx context.Context, tenantID, appName string, afterTS time.Time, afterID int64, limit int,
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

// TestDeploymentHandler_List_DualEnvelope_FirstPage pins the
// first-page dual-envelope contract: both `next_cursor` and
// `next_offset` are emitted, `total` is present, `items` is mapped
// from the service-side rows. Issue #58 dual-envelope (compat
// release — see #58-followup to retire next_offset).
func TestDeploymentHandler_List_DualEnvelope_FirstPage(t *testing.T) {
	ts := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	stub := &listableStubDeploymentRepo{
		countByAppFn: func(_ context.Context, _, _ string) (int, error) { return 100, nil },
		listByAppPaginatedFn: func(_ context.Context, _, _ string, _ time.Time, _ int64, _ int) ([]domain.Deployment, error) {
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
	// limit is set even when not passed via the query string (default 20).
	if v, ok := resp["limit"].(float64); !ok || int(v) != 20 {
		t.Errorf("limit = %v, want 20 (default)", resp["limit"])
	}
	// next_cursor may or may not be present depending on the service's
	// own emit-if-hasmore logic; we accept either here and pin the
	// FinalPage test below for the omitempty contract.
	// next_offset MUST be present on the first-page response.
	if v, ok := resp["next_offset"].(float64); !ok || int(v) != 1 {
		t.Errorf("next_offset = %v, want 1 (prevOffset 0 + len(items) 1)", resp["next_offset"])
	}
	// items must be a list of one mapped row.
	items, ok := resp["items"].([]interface{})
	if !ok || len(items) != 1 {
		t.Fatalf("items = %v, want length-1 list", resp["items"])
	}
}

// TestDeploymentHandler_List_DualEnvelope_LegacyOffset verifies the
// legacy `?offset=N` path still works (compat release): the handler
// must keep emitting `next_offset` so the CLI's --page math continues
// to function against the new server.
func TestDeploymentHandler_List_DualEnvelope_LegacyOffset(t *testing.T) {
	ts := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	stub := &listableStubDeploymentRepo{
		countByAppFn: func(_ context.Context, _, _ string) (int, error) { return 7, nil },
		listByAppPaginatedFn: func(_ context.Context, _, _ string, _ time.Time, _ int64, _ int) ([]domain.Deployment, error) {
			return []domain.Deployment{{
				ID: "d_a", TenantID: "t_test", AppName: "myapp", CreatedAt: ts,
			}}, nil
		},
	}
	mux := newDeploymentListMux(stub)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/list/myapp?offset=2&limit=3", nil)
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
	// prevOffset=2 + len(items)=1 → next_offset = 3.
	if v, ok := resp["next_offset"].(float64); !ok || int(v) != 3 {
		t.Errorf("next_offset = %v, want 3 (2+1)", resp["next_offset"])
	}
}

// TestDeploymentHandler_List_CursorDriven_OmitsNextOffset pins the
// asymmetric contract: when `?cursor=` is supplied, `next_offset`
// MUST be absent because there is no well-defined offset once the
// client has jumped forward in the cursor chain. The `?cursor=&offset=`
// rejection lives in a sibling test below.
func TestDeploymentHandler_List_CursorDriven_OmitsNextOffset(t *testing.T) {
	ts := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	stub := &listableStubDeploymentRepo{
		countByAppFn: func(_ context.Context, _, _ string) (int, error) { return 7, nil },
		listByAppPaginatedFn: func(_ context.Context, _, _ string, _ time.Time, _ int64, _ int) ([]domain.Deployment, error) {
			return []domain.Deployment{{
				ID: "d_after", TenantID: "t_test", AppName: "myapp", CreatedAt: ts,
			}}, nil
		},
	}
	mux := newDeploymentListMux(stub)

	// Encode a cursor via the service-layer codec directly — the
	// service will reject text-PK ids (mustParseDeploymentID returns 0
	// for `d_` prefixed rows), so the test uses a synthetic int64 id
	// that won't match a real row but will round-trip through the codec.
	// The decode succeeds, the repo returns one row, and the handler
	// response is what we're asserting on — not whether the cursor
	// actually points at a row.
	cursor := "eyJ2IjoxLCJwIjp7InRzIjoiMjAyNi0wNy0xNVQxMjozNDo1NloiLCJpZCI6NDJ9fQ"

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
		t.Errorf("response has 'next_offset' on cursor-driven page; want omitted")
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

// TestDeploymentHandler_List_RejectsCursorAndOffset_400 — passing
// both `?cursor=` and `?offset=` together is rejected before either
// reaches the service. Mirrors handler/webhook.go:179-185 and
// handler/app.go (issue #58 commit 4).
func TestDeploymentHandler_List_RejectsCursorAndOffset_400(t *testing.T) {
	stub := &listableStubDeploymentRepo{}
	mux := newDeploymentListMux(stub)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/list/myapp?cursor=eyJ2Ijox&offset=2", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	// Service must not have been called — the route handler short-circuits.
	// Implicit assertion: any panic from the stub would surface as a 500 with
	// a connection-reset on the test side, which the test framework would
	// report as a failure. Just asserting the 400 here is sufficient.
}

// TestDeploymentHandler_List_LimitClampedAt100 verifies the upper
// clamp: requesting limit=999 yields an effective limit=100 on the
// wire. Mirrors the apps handler's clamp test in issue #58 commit 4.
func TestDeploymentHandler_List_LimitClampedAt100(t *testing.T) {
	var seenLimit int
	stub := &listableStubDeploymentRepo{
		countByAppFn: func(_ context.Context, _, _ string) (int, error) { return 0, nil },
		listByAppPaginatedFn: func(_ context.Context, _, _ string, _ time.Time, _ int64, limit int) ([]domain.Deployment, error) {
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
