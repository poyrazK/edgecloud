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
// It propagates the cursor + until fields the new contract adds.
type stubListerAdapter struct {
	svc *stubLogService
}

func (a stubListerAdapter) ListByTenantApp(
	_ context.Context, _, _ string, filter repository.LogListFilter,
) ([]domain.LogEntry, error) {
	a.svc.called = true
	a.svc.lastQuery = service.LogQuery{
		Since:  filter.Since,
		Until:  filter.Until,
		Levels: filter.Levels,
		Limit:  filter.Limit,
		Offset: filter.Offset,
	}
	if !filter.CursorTS.IsZero() {
		a.svc.lastQuery.Cursor = "stub-cursor"
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
			{
				ID:       1,
				TenantID: "t_test",
				AppName:  "myapp",
				Level:    "info",
				Message:  "x",
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
	var raw map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"items", "limit", "since", "next_offset", "next_cursor"} {
		if _, ok := raw[key]; !ok {
			t.Errorf("response missing top-level key %q (got %v)", key, raw)
		}
	}
}

// ---------------------------------------------------------------------------
// List — 200 (next_offset + next_cursor appear when probe row detects
// another page on the offset/first-page path)
// ---------------------------------------------------------------------------

func TestLogsList_NextOffsetAndCursorInResponse(t *testing.T) {
	stub := &stubLogService{
		entries: makeLogTestEntries(4, "2026-07-14T12:00:00Z"),
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
	if len(got.Items) != 3 {
		t.Errorf("len(items) = %d, want 3 (probe trimmed)", len(got.Items))
	}
	if got.NextOffset == nil {
		t.Fatal("expected next_offset set when probe detects another page")
	} else if *got.NextOffset != 3 {
		t.Errorf("next_offset = %d, want 3", *got.NextOffset)
	}
	if got.NextCursor == nil {
		t.Fatal("expected next_cursor set when probe detects another page")
	}
}

// ---------------------------------------------------------------------------
// List — 200 (final page exactly equal to limit omits both hints)
// ---------------------------------------------------------------------------

func TestLogsList_NoNextOnFinalFullPage(t *testing.T) {
	stub := &stubLogService{
		entries: makeLogTestEntries(3, "2026-07-14T12:00:00Z"),
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
	if got.NextOffset != nil {
		t.Errorf("next_offset = %d, want nil (final page)", *got.NextOffset)
	}
	if got.NextCursor != nil {
		t.Errorf("next_cursor = %q, want nil (final page)", *got.NextCursor)
	}
}

// ---------------------------------------------------------------------------
// List — 200 (partial page omits both hints)
// ---------------------------------------------------------------------------

func TestLogsList_NoNextOnPartialPage(t *testing.T) {
	stub := &stubLogService{
		entries: makeLogTestEntries(2, "2026-07-14T12:00:00Z"),
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
		t.Errorf("next_offset = %d, want nil (partial page)", *got.NextOffset)
	}
	if got.NextCursor != nil {
		t.Errorf("next_cursor = %q, want nil (partial page)", *got.NextCursor)
	}
}

// ---------------------------------------------------------------------------
// List — 200 (query params forwarded correctly, including until)
// ---------------------------------------------------------------------------

func TestLogsList_ForwardsQueryParams(t *testing.T) {
	stub := &stubLogService{}
	mux := newLogsMux(stub)

	sinceRFC := "2020-01-01T00:00:00Z"
	untilRFC := "2026-01-01T00:00:00Z"
	url := "/api/v1/apps/myapp/logs?level=warn&limit=50&since=" + sinceRFC +
		"&until=" + untilRFC + "&offset=100"
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
	// The handler parses limit=50; the service asks the repository for
	// limit+1 = 51 to detect the next page. The repo filter carries 51.
	if stub.lastQuery.Limit != 51 {
		t.Errorf("Limit = %d, want 51 (limit+1 probe)", stub.lastQuery.Limit)
	}
	// The response envelope must echo the visible limit (50).
	var got LogListResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Limit != 50 {
		t.Errorf("response limit = %d, want 50 (visible limit)", got.Limit)
	}
	if stub.lastQuery.Since <= 0 {
		t.Errorf("Since = %s, want > 0 (parsed from RFC3339)", stub.lastQuery.Since)
	}
	if stub.lastQuery.Offset != 100 {
		t.Errorf("Offset = %d, want 100", stub.lastQuery.Offset)
	}
	wantUntil, _ := time.Parse(time.RFC3339, untilRFC)
	if !stub.lastQuery.Until.Equal(wantUntil) {
		t.Errorf("Until = %s, want %s", stub.lastQuery.Until, wantUntil)
	}
}

// ---------------------------------------------------------------------------
// List — 200 (explicit since produces a positive lookback — regression
// for the time.Until() inversion bug where a past RFC3339 produced a
// negative duration and the service silently substituted the default)
// ---------------------------------------------------------------------------

func TestLogsList_ExplicitSinceProducesPositiveLookback(t *testing.T) {
	stub := &stubLogService{}
	mux := newLogsMux(stub)

	// An explicit RFC3339 1h ago must yield a Since around 1h, not 0.
	sinceRFC := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
	url := "/api/v1/apps/myapp/logs?since=" + sinceRFC
	req := httptest.NewRequest("GET", url, nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	if stub.lastQuery.Since <= 0 {
		t.Fatalf("Since = %s, want > 0 — explicit since must translate to positive lookback",
			stub.lastQuery.Since)
	}
	// 1h ± generous slack for clock + parsing.
	if stub.lastQuery.Since < 55*time.Minute ||
		stub.lastQuery.Since > 65*time.Minute {
		t.Errorf("Since = %s, want ~1h", stub.lastQuery.Since)
	}
	// Response must echo the explicit lower bound, not the service default.
	var got LogListResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Since == "" {
		t.Error("response since is empty — must echo the resolved bound")
	}
}

// ---------------------------------------------------------------------------
// List — 400 (cursor + offset mutually exclusive — including offset=0)
// ---------------------------------------------------------------------------

func TestLogsList_RejectsCursorAndOffsetTogether(t *testing.T) {
	cases := []string{
		"offset=100",
		"offset=0",
	}
	for _, off := range cases {
		t.Run(off, func(t *testing.T) {
			stub := &stubLogService{}
			mux := newLogsMux(stub)
			url := "/api/v1/apps/myapp/logs?cursor=abc&" + off
			req := httptest.NewRequest("GET", url, nil)
			req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", rr.Code)
			}
			if stub.called {
				t.Error("service should not have been called for cursor+offset")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// List — 400 (malformed cursor maps to 400, not 500)
// ---------------------------------------------------------------------------

func TestLogsList_RejectsMalformedCursor(t *testing.T) {
	stub := &stubLogService{}
	mux := newLogsMux(stub)
	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/logs?cursor=not-a-cursor", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "invalid cursor") {
		t.Errorf("body = %s, want substring 'invalid cursor'", rr.Body.String())
	}
}

// ---------------------------------------------------------------------------
// List — 400 (until-before-since)
// ---------------------------------------------------------------------------

func TestLogsList_RejectsUntilBeforeSince(t *testing.T) {
	stub := &stubLogService{}
	mux := newLogsMux(stub)
	url := "/api/v1/apps/myapp/logs?since=2026-01-01T00:00:00Z&until=2025-01-01T00:00:00Z"
	req := httptest.NewRequest("GET", url, nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if stub.called {
		t.Error("service should not have been called for until<since")
	}
}

func TestLogsList_RejectsInvalidUntil(t *testing.T) {
	stub := &stubLogService{}
	mux := newLogsMux(stub)
	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/logs?until=not-a-time", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

func TestLogsList_RejectsFutureDatedUntil(t *testing.T) {
	stub := &stubLogService{}
	mux := newLogsMux(stub)
	future := time.Date(9000, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/logs?until="+future, nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// List — 400 (path traversal and parameter-validation)
//
// The old suite kept these tests; reused as-is.
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

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func makeLogTestEntries(n int, baseTime string) []domain.LogEntry {
	base, err := time.Parse(time.RFC3339, baseTime)
	if err != nil {
		panic("invalid baseTime in test fixture: " + err.Error())
	}
	out := make([]domain.LogEntry, n)
	for i := 0; i < n; i++ {
		out[i] = domain.LogEntry{
			ID:       int64(i + 1),
			TenantID: "t_test",
			AppName:  "myapp",
			Level:    "info",
			Message:  "hello",
			TS:       base.Add(time.Duration(i) * time.Microsecond).UTC(),
		}
	}
	return out
}
