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
// the LogHandler needs. We point the handler at the production type so
// the handler's real call site (svc.ListByTenantApp) is exercised; the
// service package's own tests cover the service's own logic.
type stubLogService struct {
	entries []domain.LogEntry
	err     error
	called  bool
	// lastQuery records the LogQuery the handler passed so tests can
	// assert the query string was parsed + defaulted correctly.
	// Levels carries the post-translation level set (or nil when no
	// level filter was requested).
	lastQuery service.LogQuery
}

func (s *stubLogService) ListByTenantApp(
	_ context.Context, _, _ string, q service.LogQuery,
) ([]domain.LogEntry, int, error) {
	s.called = true
	s.lastQuery = q
	// Echo the limit through. Handler tests assert the *echoed* limit
	// matched the requested one, so for the happy path we just pass
	// the requested value back — production behavior is "echo the
	// post-clamp limit", and the clamp is exercised in service tests.
	// If a test wants to drive the service's clamp directly, it
	// should use a real *service.LogService.
	return s.entries, q.Limit, s.err
}

// newLogsMux wires a single GET /api/v1/apps/{appName}/logs route
// through a real *http.ServeMux so r.PathValue("appName") populates
// the same way it does in production. We use a hand-rolled stub
// service (not a real *service.LogService) because the handler's
// contract is purely "call svc.ListByTenantApp and encode the
// result"; all the service-level behavior (defaults, clamps, level
// validation) is covered by service/logs_test.go.
func newLogsMux(svc *stubLogService) *http.ServeMux {
	// The handler takes a *service.LogService typed value, not the
	// LogEntryLister interface — that is what the production wiring
	// does. To inject our stub without a real DB, we wrap it in a
	// thin shim that satisfies the service struct's *implicit*
	// dependency on the repo by re-implementing the single method
	// the handler calls. We do this by constructing a real
	// LogService with our stub as its repo, then handing that
	// service to the handler.
	realSvc := service.NewLogService(stubListerAdapter{svc: svc})
	h := NewLogHandler(realSvc)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/apps/{appName}/logs", h.List)
	return mux
}

// stubListerAdapter bridges stubLogService (which records the query)
// to repository.LogListFilter (which is what *service.LogService
// consumes). Without this, the service would reject the call because
// the underlying repo would never get hit.
type stubListerAdapter struct {
	svc *stubLogService
}

func (a stubListerAdapter) ListByTenantApp(
	_ context.Context, _, _ string, filter repository.LogListFilter,
) ([]domain.LogEntry, error) {
	// Capture everything the service handed the repo: the handler
	// tests assert that the parsed query made it through. Note we
	// can't recover the original MinLvl string (the service expanded
	// it into a Levels set), so the level assertion in
	// TestLogsList_ForwardsQueryParams runs against the Levels slice
	// — the same observable effect.
	a.svc.called = true
	a.svc.lastQuery = service.LogQuery{
		Since:  filter.Since,
		Limit:  filter.Limit,
		Levels: filter.Levels,
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

// TestLogsList_EnvelopeShape pins the JSON wire format. The CLI parses
// this; a regression that renamed a field would silently break the
// read path.
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
// List — 200 (query params forwarded correctly)
// ---------------------------------------------------------------------------

// TestLogsList_ForwardsQueryParams pins the handler's parse → service
// contract: every query param must be parsed and passed to the
// service so the service's validation can reject bad input. We
// assert on the *service* query (not the repo filter) so a future
// service refactor doesn't break this test.
func TestLogsList_ForwardsQueryParams(t *testing.T) {
	stub := &stubLogService{}
	mux := newLogsMux(stub)

	// Past timestamp: 2020-01-01, unambiguously behind any clock the
	// test runner could have. The handler's parseSinceParam rejects
	// future-dated values with 400, so picking a date "today" risks
	// flakiness around midnight UTC.
	sinceRFC := "2020-01-01T00:00:00Z"
	url := "/api/v1/apps/myapp/logs?level=warn&limit=50&since=" + sinceRFC
	req := httptest.NewRequest("GET", url, nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	// The service translates MinLvl=warn into Levels=[warn, error]
	// before the repo sees it, so we assert on the post-translation
	// slice. The MinLvl→Levels mapping itself is covered by
	// service/logs_test.go.
	if !reflect.DeepEqual(stub.lastQuery.Levels, []string{"warn", "error"}) {
		t.Errorf("Levels = %v, want [warn error]", stub.lastQuery.Levels)
	}
	if stub.lastQuery.Limit != 50 {
		t.Errorf("Limit = %d, want 50", stub.lastQuery.Limit)
	}
	if stub.lastQuery.Since <= 0 {
		t.Errorf("Since = %s, want > 0 (parsed from RFC3339)", stub.lastQuery.Since)
	}
}

// ---------------------------------------------------------------------------
// List — 400 (path traversal in appName)
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
				t.Errorf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
			}
			if stub.called {
				t.Error("service should not have been called for traversal appName")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// List — 400 (invalid since)
// ---------------------------------------------------------------------------

func TestLogsList_RejectsInvalidSince(t *testing.T) {
	stub := &stubLogService{}
	mux := newLogsMux(stub)

	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/logs?since=not-a-time", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "since") {
		t.Errorf("body should mention 'since', got: %s", rr.Body.String())
	}
	if stub.called {
		t.Error("service should not have been called for invalid since")
	}
}

// ---------------------------------------------------------------------------
// List — 400 (future-dated since)
// ---------------------------------------------------------------------------

// TestLogsList_RejectsFutureDatedSince pins the contract change from
// PR #138 review finding #7: a `since` whose RFC3339 value lies after
// `time.Now()` must be rejected with 400 (not silently clamped to 0,
// which would let the request succeed against the default 5m window
// while the client thought it was pinning a specific past bound).
func TestLogsList_RejectsFutureDatedSince(t *testing.T) {
	stub := &stubLogService{}
	mux := newLogsMux(stub)

	// Year 9000 — far enough in the future that RFC3339's
	// seconds-precision rounding (which floors to the whole
	// second on Format→Parse round-trip) cannot cause the parsed
	// value to land in the past. Using "now + small offset" is a
	// footgun: sub-second drift across the format/parse round
	// trip can flip a +1h offset into a value that's effectively
	// already past when the handler measures `time.Until(t)`.
	// RFC3339 accepts any year in 0001..9999.
	future := time.Date(9000, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/logs?since="+future, nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "future") {
		t.Errorf("body should mention 'future', got: %s", rr.Body.String())
	}
	if stub.called {
		t.Error("service should not have been called for future-dated since")
	}
}

// ---------------------------------------------------------------------------
// List — 400 (invalid level)
// ---------------------------------------------------------------------------

func TestLogsList_RejectsInvalidLevel(t *testing.T) {
	stub := &stubLogService{}
	mux := newLogsMux(stub)

	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/logs?level=critical", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "level") {
		t.Errorf("body should mention 'level', got: %s", rr.Body.String())
	}
	if stub.called {
		t.Error("service should not have been called for invalid level")
	}
}

// ---------------------------------------------------------------------------
// List — 400 (invalid limit)
// ---------------------------------------------------------------------------

func TestLogsList_RejectsInvalidLimit(t *testing.T) {
	stub := &stubLogService{}
	mux := newLogsMux(stub)

	// Non-integer — the handler's parseLimitParam is the gate.
	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/logs?limit=notanumber", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	if stub.called {
		t.Error("service should not have been called for invalid limit")
	}
}

// ---------------------------------------------------------------------------
// List — 500 (repo / service error)
// ---------------------------------------------------------------------------

func TestLogsList_ServiceError_Returns500(t *testing.T) {
	stub := &stubLogService{err: errors.New("db unreachable")}
	mux := newLogsMux(stub)

	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/logs", nil)
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
