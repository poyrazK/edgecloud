package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
)

// usageWire mirrors handler.usage.go's response shape for tests.
// Defined locally so the handler can change its internals (e.g. add
// response headers, rename a field) without breaking the decode in
// every test.
type usageWire struct {
	domain.TenantUsage
}

// usageMockSvc satisfies UsageServiceInterface. Fields map 1:1 to
// the service contract: pass usage=nil + err=nil to simulate a
// missing tenant (handler returns 404); pass err=non-nil to simulate
// a DB outage (handler returns 500).
type usageMockSvc struct {
	usage *domain.TenantUsage
	err   error
	calls int
}

func (m *usageMockSvc) GetUsage(_ context.Context, _ string, _, _ time.Time, _ int) (*domain.TenantUsage, error) {
	m.calls++
	return m.usage, m.err
}

func newUsageHandlerFor(svc UsageServiceInterface) *UsageHandler {
	return NewUsageHandler(svc)
}

// reqWithTenant attaches a tenant ID to the request context, mirroring
// what authMiddleware does on real requests. Usage handler reads it
// via middleware.GetTenantID.
func reqWithTenant(r *http.Request, tenantID string) *http.Request {
	return r.WithContext(middleware.WithTenantID(r.Context(), tenantID))
}

// TestUsageHandler_GetUsage_Success exercises the happy path: a paid
// tenant in good standing sees all four fields populated and the
// echoed from/to in the response.
func TestUsageHandler_GetUsage_Success(t *testing.T) {
	from := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	want := &domain.TenantUsage{
		TenantID:      "t_pro",
		BillingStatus: domain.BillingActive,
		CurrentPeriod: domain.CurrentPeriodUsage{
			PeriodStart: from, PeriodEnd: to,
			RequestsUsed: 1000, RequestsCap: 5_000_000,
		},
		From: from, To: to,
	}
	svc := &usageMockSvc{usage: want}
	h := newUsageHandlerFor(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage", nil)
	req = reqWithTenant(req, "t_pro")
	rr := httptest.NewRecorder()
	h.GetUsage(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	var got usageWire
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.TenantID != "t_pro" {
		t.Errorf("TenantID = %q, want t_pro", got.TenantID)
	}
	if got.BillingStatus != domain.BillingActive {
		t.Errorf("BillingStatus = %q, want active", got.BillingStatus)
	}
	if got.CurrentPeriod.RequestsUsed != 1000 {
		t.Errorf("RequestsUsed = %d, want 1000", got.CurrentPeriod.RequestsUsed)
	}
}

// TestUsageHandler_GetUsage_DefaultWindow verifies that omitting
// from/to yields a 30-day window ending now. We assert on the values
// the SERVICE received (captured via captureWindowSvc), since the
// handler doesn't transform the response — it just unwraps the
// service's return value.
func TestUsageHandler_GetUsage_DefaultWindow(t *testing.T) {
	var capturedFrom, capturedTo time.Time
	svc := &captureWindowSvc{
		usage:   &domain.TenantUsage{TenantID: "t_free"},
		fromOut: &capturedFrom,
		toOut:   &capturedTo,
	}
	h := newUsageHandlerFor(svc)

	before := time.Now().UTC()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage", nil)
	req = reqWithTenant(req, "t_free")
	rr := httptest.NewRecorder()
	h.GetUsage(rr, req)
	after := time.Now().UTC()

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if capturedTo.Before(before) || capturedTo.After(after.Add(time.Second)) {
		t.Errorf("to arg = %v, want between %v and %v", capturedTo, before, after)
	}
	span := capturedTo.Sub(capturedFrom)
	wantSpan := 30 * 24 * time.Hour
	if span < wantSpan-time.Minute || span > wantSpan+time.Minute {
		t.Errorf("default span = %v, want ~%v", span, wantSpan)
	}
}

// captureWindowSvc records the from/to the handler passed so the
// default-window test can assert on them without going through the
// full service stack.
type captureWindowSvc struct {
	usage   *domain.TenantUsage
	fromOut *time.Time
	toOut   *time.Time
}

func (m *captureWindowSvc) GetUsage(_ context.Context, _ string, from, to time.Time, _ int) (*domain.TenantUsage, error) {
	if m.fromOut != nil {
		*m.fromOut = from
	}
	if m.toOut != nil {
		*m.toOut = to
	}
	return m.usage, nil
}

// TestUsageHandler_GetUsage_NotFound confirms the 404 path: the
// service returned (nil, nil) because the tenant has no quota row.
// Mirrors QuotaHandler.GetQuota at handler/quota.go:47-49.
func TestUsageHandler_GetUsage_NotFound(t *testing.T) {
	svc := &usageMockSvc{usage: nil, err: nil}
	h := newUsageHandlerFor(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage", nil)
	req = reqWithTenant(req, "t_missing")
	rr := httptest.NewRecorder()
	h.GetUsage(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404; body=%s", rr.Code, rr.Body.String())
	}
}

// TestUsageHandler_GetUsage_MissingTenantContext confirms the defense-in-depth
// 401 path: middleware.GetTenantID returns "" when no tenant is stamped on
// the context, and the handler surfaces that as 401 rather than silently
// looking up quotas for an empty tenant ID (which would 404).
func TestUsageHandler_GetUsage_MissingTenantContext(t *testing.T) {
	svc := &usageMockSvc{}
	h := newUsageHandlerFor(svc)

	// No reqWithTenant — the request context has no tenant.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage", nil)
	rr := httptest.NewRecorder()
	h.GetUsage(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401; body=%s", rr.Code, rr.Body.String())
	}
	if svc.calls != 0 {
		t.Errorf("svc called %d times, want 0 (auth must short-circuit)", svc.calls)
	}
}

// TestUsageHandler_GetUsage_ServiceError confirms the 500 path: any
// non-nil error from the service becomes 500. The error body is the
// generic httperror.InternalErrorCtx body — not the service's error
// string, which might leak SQL details.
func TestUsageHandler_GetUsage_ServiceError(t *testing.T) {
	svc := &usageMockSvc{err: errMockDBDown}
	h := newUsageHandlerFor(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage", nil)
	req = reqWithTenant(req, "t_db_dead")
	rr := httptest.NewRecorder()
	h.GetUsage(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rr.Code)
	}
}

// errMockDBDown is a typed sentinel for the service-error test so we
// don't conflate it with a parse error.
var errMockDBDown = &mockServiceError{msg: "db connection refused"}

type mockServiceError struct{ msg string }

func (e *mockServiceError) Error() string { return e.msg }

// TestUsageHandler_GetUsage_BadFrom exercises the 400 path: from is
// not RFC3339.
func TestUsageHandler_GetUsage_BadFrom(t *testing.T) {
	svc := &usageMockSvc{}
	h := newUsageHandlerFor(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage?from=not-a-date", nil)
	req = reqWithTenant(req, "t_1")
	rr := httptest.NewRecorder()
	h.GetUsage(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "from") {
		t.Errorf("body = %q, want error mentioning 'from'", rr.Body.String())
	}
	if svc.calls != 0 {
		t.Errorf("svc called %d times on bad request, want 0", svc.calls)
	}
}

// TestUsageHandler_GetUsage_BadTo exercises the 400 path: to is not
// RFC3339.
func TestUsageHandler_GetUsage_BadTo(t *testing.T) {
	svc := &usageMockSvc{}
	h := newUsageHandlerFor(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage?to=2026-99-99", nil)
	req = reqWithTenant(req, "t_1")
	rr := httptest.NewRecorder()
	h.GetUsage(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

// TestUsageHandler_GetUsage_FromAfterTo exercises the 400 path:
// from > to.
func TestUsageHandler_GetUsage_FromAfterTo(t *testing.T) {
	svc := &usageMockSvc{}
	h := newUsageHandlerFor(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage?from=2026-07-10T00:00:00Z&to=2026-07-01T00:00:00Z", nil)
	req = reqWithTenant(req, "t_1")
	rr := httptest.NewRecorder()
	h.GetUsage(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
	if svc.calls != 0 {
		t.Errorf("svc called %d times on bad request, want 0", svc.calls)
	}
}

// TestUsageHandler_GetUsage_BadLimit exercises the 400 path: limit
// is not a positive integer.
func TestUsageHandler_GetUsage_BadLimit(t *testing.T) {
	svc := &usageMockSvc{}
	h := newUsageHandlerFor(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage?limit=abc", nil)
	req = reqWithTenant(req, "t_1")
	rr := httptest.NewRecorder()
	h.GetUsage(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

// TestUsageHandler_GetUsage_LimitOverMax confirms that a limit above
// maxUsageLimit is rejected with 400 — not silently clamped. The
// dashboard asks for too much; we surface the error rather than return
// a truncated response. Matches the OpenAPI `maximum: 200`.
func TestUsageHandler_GetUsage_LimitOverMax(t *testing.T) {
	svc := &usageMockSvc{}
	h := newUsageHandlerFor(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage?limit=10000", nil)
	req = reqWithTenant(req, "t_1")
	rr := httptest.NewRecorder()
	h.GetUsage(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
	if svc.calls != 0 {
		t.Errorf("svc called %d times on bad request, want 0", svc.calls)
	}
	if !strings.Contains(rr.Body.String(), "limit") {
		t.Errorf("body = %q, want error mentioning 'limit'", rr.Body.String())
	}
}

// TestParseUsageParams_Defaults exercises the param parser directly
// without going through the handler — confirms the service receives
// the right values when the request omits all params.
func TestParseUsageParams_Defaults(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage", nil)
	from, to, limit, err := parseUsageParams(req)
	if err != nil {
		t.Fatalf("parseUsageParams: %v", err)
	}
	if limit != defaultUsageLimit {
		t.Errorf("limit = %d, want %d", limit, defaultUsageLimit)
	}
	if to.Sub(from) != defaultUsageWindow {
		t.Errorf("window = %v, want %v", to.Sub(from), defaultUsageWindow)
	}
}

// TestParseUsageParams_BadInputs is a table-driven check of every
// 400-able input. Each row is a (query string, wantErrSubstr) pair.
func TestParseUsageParams_BadInputs(t *testing.T) {
	cases := []struct {
		name    string
		query   string
		wantSub string // substring expected in err.Error()
	}{
		{"bad-from", "?from=not-rfc3339", "from"},
		{"bad-to", "?to=2026-13-99", "to"},
		{"from-after-to", "?from=2026-07-10T00:00:00Z&to=2026-07-01T00:00:00Z", "from"},
		{"bad-limit", "?limit=abc", "limit"},
		{"zero-limit", "?limit=0", "limit"},
		{"negative-limit", "?limit=-5", "limit"},
		{"over-max-limit", "?limit=201", "limit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/usage"+tc.query, nil)
			_, _, _, err := parseUsageParams(req)
			if err == nil {
				t.Fatal("err = nil, want error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("err = %q, want substring %q", err.Error(), tc.wantSub)
			}
		})
	}
}
