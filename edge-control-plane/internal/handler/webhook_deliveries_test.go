package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// newDeliveriesMux mirrors newWebhookMux but routes ONLY the deliveries
// endpoint — kept in this file because no other test in the suite
// needs it.
func newDeliveriesMux(svc *mockWebhookSvc) *http.ServeMux {
	mux := http.NewServeMux()
	h := NewWebhookHandler(svc)
	mux.HandleFunc("GET /api/v1/webhooks/{webhookID}/deliveries", h.ListDeliveries)
	return mux
}

// withDeliveriesTenantCtx mirrors withTenant but lives in this file
// (the existing helper is defined further down in webhook_test.go and
// we keep it untouched to avoid breaking the existing test imports).
func withDeliveriesTenantCtx(r *http.Request, tenantID string) *http.Request {
	ctx := r.Context()
	ctx = middleware.WithTenantID(ctx, tenantID)
	ctx = middleware.WithAPIKeyID(ctx, "ak_test")
	ctx = middleware.WithRole(ctx, "owner")
	return r.WithContext(ctx)
}

// TestWebhookHandler_ListDeliveries_Success pins the 200 path:
// the handler decodes the cursor, parses the limit, calls the service
// with the right (tenantID, webhookID) tuple, and emits the
// {"deliveries", "limit", "next_cursor"} envelope.
func TestWebhookHandler_ListDeliveries_Success(t *testing.T) {
	svc := &mockWebhookSvc{
		listDeliveriesResult: &service.WebhookDeliveriesResult{
			Deliveries: []domain.WebhookDelivery{
				{ID: 1, WebhookID: "wh_1", EventType: "deploy", Status: "success", StatusCode: intPtr(200), Attempt: 1, MaxAttempts: 3, CreatedAt: time.Now()},
				{ID: 2, WebhookID: "wh_1", EventType: "deploy", Status: "failed", ErrorMsg: "HTTP 503", Attempt: 3, MaxAttempts: 3, CreatedAt: time.Now().Add(-time.Second)},
			},
			Limit:      50,
			NextCursor: nil,
		},
	}
	mux := newDeliveriesMux(svc)

	r := httptest.NewRequest("GET", "/api/v1/webhooks/wh_1/deliveries?limit=50", nil)
	r = withDeliveriesTenantCtx(r, "t_1")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Deliveries []domain.WebhookDelivery `json:"deliveries"`
		Limit      int                     `json:"limit"`
		NextCursor *string                 `json:"next_cursor"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v; body=%s", err, w.Body.String())
	}
	if len(resp.Deliveries) != 2 {
		t.Errorf("len(deliveries) = %d, want 2", len(resp.Deliveries))
	}
	if resp.Limit != 50 {
		t.Errorf("limit = %d, want 50", resp.Limit)
	}
	if resp.NextCursor != nil {
		t.Errorf("next_cursor = %v, want null", *resp.NextCursor)
	}
	// Service must have been called once with the right args.
	if svc.listDeliveriesCalls != 1 {
		t.Errorf("service calls = %d, want 1", svc.listDeliveriesCalls)
	}
	if svc.lastDeliveriesLimit != 50 {
		t.Errorf("limit passed to service = %d, want 50", svc.lastDeliveriesLimit)
	}
}

// TestWebhookHandler_ListDeliveries_NextCursorEncoded pins that a
// non-nil NextCursor from the service makes it into the wire response
// as a non-null JSON string.
func TestWebhookHandler_ListDeliveries_NextCursorEncoded(t *testing.T) {
	next := "eyJ2IjoxLCJ0cyI6IjIwMjYtMDctMTRUMTM6MDA6MDBaIiwiaWQiOjk5fQ"
	svc := &mockWebhookSvc{
		listDeliveriesResult: &service.WebhookDeliveriesResult{
			Deliveries: []domain.WebhookDelivery{{ID: 99, WebhookID: "wh_1"}},
			Limit:      1,
			NextCursor: &next,
		},
	}
	mux := newDeliveriesMux(svc)
	r := httptest.NewRequest("GET", "/api/v1/webhooks/wh_1/deliveries?limit=1", nil)
	r = withDeliveriesTenantCtx(r, "t_1")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp struct {
		NextCursor *string `json:"next_cursor"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.NextCursor == nil || *resp.NextCursor != next {
		t.Errorf("next_cursor = %v, want %q", resp.NextCursor, next)
	}
}

// TestWebhookHandler_ListDeliveries_PassesCursorQueryParam pins that
// the cursor query parameter is forwarded to the service verbatim.
func TestWebhookHandler_ListDeliveries_PassesCursorQueryParam(t *testing.T) {
	svc := &mockWebhookSvc{
		listDeliveriesResult: &service.WebhookDeliveriesResult{Limit: 50},
	}
	mux := newDeliveriesMux(svc)
	r := httptest.NewRequest("GET", "/api/v1/webhooks/wh_1/deliveries?cursor=eyJ2IjoxfQ&limit=50", nil)
	r = withDeliveriesTenantCtx(r, "t_1")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if svc.lastDeliveriesCursor != "eyJ2IjoxfQ" {
		t.Errorf("cursor passed to service = %q, want %q", svc.lastDeliveriesCursor, "eyJ2IjoxfQ")
	}
}

// TestWebhookHandler_ListDeliveries_RejectsCursorAndOffset pins that
// the handler rejects any request that supplies BOTH `cursor` and
// `offset` — defensive: the endpoint does not advertise `offset` but
// a confused client might still send one alongside a cursor.
func TestWebhookHandler_ListDeliveries_RejectsCursorAndOffset(t *testing.T) {
	svc := &mockWebhookSvc{}
	mux := newDeliveriesMux(svc)
	r := httptest.NewRequest("GET", "/api/v1/webhooks/wh_1/deliveries?cursor=eyJ2IjoxfQ&offset=10", nil)
	r = withDeliveriesTenantCtx(r, "t_1")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
	if svc.listDeliveriesCalls != 0 {
		t.Errorf("service was called; expected cursor+offset to short-circuit")
	}
}

// TestWebhookHandler_ListDeliveries_RejectsInvalidCursor pins that a
// malformed cursor surfaces as a 400 (not a 500), AND the
// ErrInvalidWebhookDeliveryCursor typed error is what the handler
// maps — distinguishing "malformed" from "unsupported version" only
// matters for the operator log path.
func TestWebhookHandler_ListDeliveries_RejectsInvalidCursor(t *testing.T) {
	svc := &mockWebhookSvc{
		listDeliveriesErr: service.ErrInvalidWebhookDeliveryCursor,
	}
	mux := newDeliveriesMux(svc)
	r := httptest.NewRequest("GET", "/api/v1/webhooks/wh_1/deliveries?cursor=garbage", nil)
	r = withDeliveriesTenantCtx(r, "t_1")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
	if svc.listDeliveriesCalls != 1 {
		t.Errorf("service calls = %d, want 1", svc.listDeliveriesCalls)
	}
}

// TestWebhookHandler_ListDeliveries_RejectsUnsupportedCursorVersion
// pins that the typed "unsupported version" error also maps to 400
// (same wire as malformed) but is logged differently in the operator
// path. Both errors collapse to the same wire response — that's
// intentional (no decoder internals leaked to clients).
func TestWebhookHandler_ListDeliveries_RejectsUnsupportedCursorVersion(t *testing.T) {
	svc := &mockWebhookSvc{
		listDeliveriesErr: service.ErrUnsupportedWebhookDeliveryCursorVersion,
	}
	mux := newDeliveriesMux(svc)
	r := httptest.NewRequest("GET", "/api/v1/webhooks/wh_1/deliveries?cursor=eyJ2IjoyfQ", nil)
	r = withDeliveriesTenantCtx(r, "t_1")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

// TestWebhookHandler_ListDeliveries_NotFound pins that a
// ErrWebhookNotFound from the service (missing OR wrong-tenant
// webhook) maps to 404 — collapsing both cases prevents enumeration
// of webhook IDs across tenants.
func TestWebhookHandler_ListDeliveries_NotFound(t *testing.T) {
	svc := &mockWebhookSvc{
		listDeliveriesErr: service.ErrWebhookNotFound,
	}
	mux := newDeliveriesMux(svc)
	r := httptest.NewRequest("GET", "/api/v1/webhooks/wh_other/deliveries", nil)
	r = withDeliveriesTenantCtx(r, "t_1")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

// TestWebhookHandler_ListDeliveries_InvalidLimit pins that
// non-integer / negative limits return 400 and never reach the
// service.
func TestWebhookHandler_ListDeliveries_InvalidLimit(t *testing.T) {
	cases := []string{"abc", "-5"}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			svc := &mockWebhookSvc{}
			mux := newDeliveriesMux(svc)
			r := httptest.NewRequest("GET", "/api/v1/webhooks/wh_1/deliveries?limit="+raw, nil)
			r = withDeliveriesTenantCtx(r, "t_1")
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, r)

			if w.Code != http.StatusBadRequest {
				t.Errorf("limit=%q status = %d, want 400", raw, w.Code)
			}
			if svc.listDeliveriesCalls != 0 {
				t.Errorf("limit=%q: service was called; expected handler to short-circuit", raw)
			}
		})
	}
}

// TestWebhookHandler_ListDeliveries_DefaultLimitWhenAbsent pins that
// omitting `limit` passes 0 to the service (which clamps to the
// default 50 inside the service layer).
func TestWebhookHandler_ListDeliveries_DefaultLimitWhenAbsent(t *testing.T) {
	svc := &mockWebhookSvc{
		listDeliveriesResult: &service.WebhookDeliveriesResult{Limit: 50},
	}
	mux := newDeliveriesMux(svc)
	r := httptest.NewRequest("GET", "/api/v1/webhooks/wh_1/deliveries", nil)
	r = withDeliveriesTenantCtx(r, "t_1")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if svc.lastDeliveriesLimit != 0 {
		t.Errorf("limit passed to service = %d, want 0 (handler passes through; service applies default)", svc.lastDeliveriesLimit)
	}
}

// intPtr is a tiny helper used by tests to set *int fields like
// StatusCode without declaring a package-level var.
func intPtr(i int) *int { return &i }

// Sanity guard: if the package's mockWebhookSvc is ever refactored
// away from the WebhookServiceInterface shape, the build will fail
// here before the integration tests do — gives a faster signal than
// the integration suite.
var _ service.WebhookServiceInterface = (*mockWebhookSvc)(nil)