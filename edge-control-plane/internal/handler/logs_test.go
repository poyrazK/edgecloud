package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// stubLogService is the minimum implementation of *service.LogService
// the LogHandler needs. We wrap a real *service.LogService whose repo
// is a stub so the handler's own call path (svc.ListByTenantApp) is
// exercised.
type stubLogService struct {
	entries []domain.LogEntry
	err     error
	called  bool
	// lastQuery records the LogQuery the handler passed so tests can
	// assert the query string was parsed + defaulted correctly.
	lastQuery service.LogQuery
}

// newLogsMux wires a single GET /api/v1/apps/{appName}/logs route.
func newLogsMux(stub *stubLogService) *http.ServeMux {
	realSvc := service.NewLogService(stubListerAdapter{svc: stub})
	h := NewLogHandler(realSvc)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/apps/{appName}/logs", h.List)
	return mux
}

// stubListerAdapter bridges stubLogService to repository.LogListFilter.
type stubListerAdapter struct {
	svc *stubLogService
}

func (a stubListerAdapter) ListByTenantApp(
	_ context.Context, _, _ string, filter repository.LogListFilter,
) ([]domain.LogEntry, error) {
	a.svc.called = true
	a.svc.lastQuery = service.LogQuery{
		Since:  filter.Since,
		Limit:  filter.Limit,
		Levels: filter.Levels,
		Offset: filter.Offset,
	}
	return a.svc.entries, a.svc.err
}

// ---------------------------------------------------------------------------
// List — 200 (happy path, all defaults)
// ---------------------------------------------------------------------------

func TestLogsList_HappyPath_Returns200(t *testing.T) {
	stub := &stubLogService{
		entries: []domain.LogEntry{
			{
				ID:       1,
				TenantID: "t_test",
				AppName:  "myapp",
				Level:    "info",
				Message:  "hello",
				TS:       time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC),
			},
		},
	}
	mux := newLogsMux(stub)

	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/logs", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var got LogListResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Items) != 1 {
		t.Errorf("len(items) = %d, want 1", len(got.Items))
	}
	if got.Limit != service.DefaultLogLimit {
		t.Errorf("limit = %d, want %d (default)", got.Limit, service.DefaultLogLimit)
	}
	if !stub.called {
		t.Error("service was not called")
	}
}

// ---------------------------------------------------------------------------
// List — 200 (envelope shape)
// ---------------------------------------------------------------------------

func TestLogsList_EnvelopeShape(t *testing.T) {
	stub := &stubLogService{
		entries: []domain.LogEntry{
			{ID: 1, AppName: "myapp", Level: "info", Message: "x"},
		},
	}
	mux := newLogsMux(stub)

	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/logs", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var raw map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"items", "limit", "since"} {
		if _, ok := raw[key]; !ok {
			t.Errorf("response missing top-level key %q (got %v)", key, raw)
		}
	}
}

// ---------------------------------------------------------------------------
// List — 200 (next_offset appears when more results exist)
// ---------------------------------------------------------------------------

func TestLogsList_NextOffsetInResponse(t *testing.T) {
	// 5 entries, limit 3 → 2 pages. First page should have next_offset=3.
	stub := &stubLogService{
		entries: make([]domain.LogEntry, 3),
	}
	mux := newLogsMux(stub)

	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/logs?limit=3", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var got LogListResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.NextOffset == nil {
		t.Fatal("expected next_offset in response when page is full")
	}
	if *got.NextOffset != 3 {
		t.Errorf("next_offset = %d, want 3", *got.NextOffset)
	}
}

// ---------------------------------------------------------------------------
// List — 200 (next_offset omitted when last page)
// ---------------------------------------------------------------------------

func TestLogsList_NoNextOffsetOnLastPage(t *testing.T) {
	stub := &stubLogService{
		entries: make([]domain.LogEntry, 2),
	}
	mux := newLogsMux(stub)

	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/logs?limit=10", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var got LogListResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.NextOffset != nil {
		t.Errorf("next_offset = %d, want nil (last page)", *got.NextOffset)
	}
}

// ---------------------------------------------------------------------------
// List — 200 (query params forwarded correctly)
// ---------------------------------------------------------------------------

func TestLogsList_ForwardsQueryParams(t *testing.T) {
	stub := &stubLogService{}
	mux := newLogsMux(stub)

	sinceRFC := "2020-01-01T00:00:00Z"
	url := "/api/v1/apps/myapp/logs?level=warn&limit=50&since=" + sinceRFC + "&offset=100"
	req := httptest.NewRequest("GET", url, nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	if !reflect.DeepEqual(stub.lastQuery.Levels, []string{"warn", "error"}) {
		t.Errorf("Levels = %v, want [warn error]", stub.lastQuery.Levels)
	}
	if stub.lastQuery.Limit != 50 {
		t.Errorf("Limit = %d, want 50", stub.lastQuery.Limit)
	}
	if stub.lastQuery.Since <= 0 {
		t.Errorf("Since = %s, want > 0 (parsed from RFC3339)", stub.lastQuery.Since)
	}
	if stub.lastQuery.Offset != 100 {
		t.Errorf("Offset = %d, want 100", stub.lastQuery.Offset)
	}
}

// ---------------------------------------------------------------------------
// List — 400 tests
// ---------------------------------------------------------------------------

func TestLogsList_PathTraversal_Returns400(t *testing.T) {
	cases := []struct {
		name    string
		appName string
	}{
		{"backslash", `foo\bar`},
		{"percent-encoded dots", "%2E%2E"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			stub := &stubLogService{}
			mux := newLogsMux(stub)
			url := "/api/v1/apps/" + c.appName + "/logs"
			req := httptest.NewRequest("GET", url, nil)
			req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", rr.Code)
			}
			if stub.called {
				t.Error("service should not have been called for traversal appName")
			}
		})
	}
}

func TestLogsList_RejectsInvalidSince(t *testing.T) {
	stub := &stubLogService{}
	mux := newLogsMux(stub)
	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/logs?since=not-a-time", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if stub.called {
		t.Error("service should not have been called for invalid since")
	}
}

func TestLogsList_RejectsFutureDatedSince(t *testing.T) {
	stub := &stubLogService{}
	mux := newLogsMux(stub)
	future := time.Date(9000, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/logs?since="+future, nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if stub.called {
		t.Error("service should not have been called for future-dated since")
	}
}

func TestLogsList_RejectsInvalidLevel(t *testing.T) {
	stub := &stubLogService{}
	mux := newLogsMux(stub)
	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/logs?level=critical", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if stub.called {
		t.Error("service should not have been called for invalid level")
	}
}

func TestLogsList_RejectsInvalidLimit(t *testing.T) {
	stub := &stubLogService{}
	mux := newLogsMux(stub)
	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/logs?limit=notanumber", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if stub.called {
		t.Error("service should not have been called for invalid limit")
	}
}

func TestLogsList_RejectsInvalidOffset(t *testing.T) {
	stub := &stubLogService{}
	mux := newLogsMux(stub)
	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/logs?offset=notanumber", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if stub.called {
		t.Error("service should not have been called for invalid offset")
	}
}

func TestLogsList_ServiceError_Returns500(t *testing.T) {
	stub := &stubLogService{err: errors.New("db unreachable")}
	mux := newLogsMux(stub)
	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/logs", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "db unreachable") {
		t.Errorf("body must not leak raw error: %s", rr.Body.String())
	}
}
