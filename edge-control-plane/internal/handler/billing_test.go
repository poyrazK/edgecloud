package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/billing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
)

// mockBillingSvc implements billing.BillingServiceInterface for the
// handler tests. Each method records its call and returns whatever
// the test set.
type mockBillingSvc struct {
	startCheckoutResp  billing.CheckoutSession
	startCheckoutErr   error
	startCheckoutCalls int

	portalResp  billing.PortalSession
	portalErr   error
	portalCalls int

	getSub      domain.BillingSubscription
	getSubErr   error
	getSubCalls int

	handleWebhookErr   error
	handleWebhookCalls int
	lastBody           []byte
	lastHeaders        http.Header
}

func (m *mockBillingSvc) StartCheckout(_ context.Context, tenantID, plan string) (billing.CheckoutSession, error) {
	m.startCheckoutCalls++
	return m.startCheckoutResp, m.startCheckoutErr
}
func (m *mockBillingSvc) OpenPortal(_ context.Context, _ string, _ string) (billing.PortalSession, error) {
	m.portalCalls++
	return m.portalResp, m.portalErr
}
func (m *mockBillingSvc) GetSubscription(_ context.Context, _ string) (domain.BillingSubscription, error) {
	m.getSubCalls++
	return m.getSub, m.getSubErr
}
func (m *mockBillingSvc) HandleWebhook(_ context.Context, headers http.Header, body []byte) error {
	m.handleWebhookCalls++
	m.lastBody = body
	m.lastHeaders = headers
	return m.handleWebhookErr
}

// ctxWithTenant returns a context with tenantID stamped — mirrors
// what the auth middleware does in production.
func ctxWithTenant(tenantID string) context.Context {
	return middleware.WithTenantID(context.Background(), tenantID)
}

// TestStartCheckout_HappyPath verifies the auth-required handler
// delegates to the service and returns the CheckoutResponse with
// the URL/session id.
func TestStartCheckout_HappyPath(t *testing.T) {
	svc := &mockBillingSvc{
		startCheckoutResp: billing.CheckoutSession{
			ID:        "cs_test_abc",
			URL:       "https://checkout.stripe.com/c/pay/cs_test_abc",
			ExpiresAt: time.Now().Add(24 * time.Hour),
		},
	}
	h := NewBillingHandler(svc)

	body, _ := json.Marshal(CheckoutRequest{Plan: "pro"})
	req := httptest.NewRequest("POST", "/api/v1/billing/checkout", bytes.NewReader(body))
	req = req.WithContext(ctxWithTenant("t_1"))
	rr := httptest.NewRecorder()
	h.StartCheckout(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rr.Code)
	}
	if svc.startCheckoutCalls != 1 {
		t.Errorf("StartCheckout calls = %d, want 1", svc.startCheckoutCalls)
	}
	var resp CheckoutResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if resp.CheckoutURL != "https://checkout.stripe.com/c/pay/cs_test_abc" {
		t.Errorf("checkout_url = %q, want stripe URL", resp.CheckoutURL)
	}
	if resp.SessionID != "cs_test_abc" {
		t.Errorf("session_id = %q, want cs_test_abc", resp.SessionID)
	}
}

// TestStartCheckout_RejectsFree: the free tier doesn't go through
// checkout; only paid plans do.
func TestStartCheckout_RejectsFree(t *testing.T) {
	svc := &mockBillingSvc{}
	h := NewBillingHandler(svc)

	body, _ := json.Marshal(CheckoutRequest{Plan: "free"})
	req := httptest.NewRequest("POST", "/api/v1/billing/checkout", bytes.NewReader(body))
	req = req.WithContext(ctxWithTenant("t_1"))
	rr := httptest.NewRecorder()
	h.StartCheckout(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if svc.startCheckoutCalls != 0 {
		t.Errorf("StartCheckout calls = %d, want 0 (free rejected at handler)", svc.startCheckoutCalls)
	}
}

// TestStartCheckout_RejectsUnknownPlan: 400 on bogus plan name.
func TestStartCheckout_RejectsUnknownPlan(t *testing.T) {
	svc := &mockBillingSvc{}
	h := NewBillingHandler(svc)

	body, _ := json.Marshal(CheckoutRequest{Plan: "platinum"})
	req := httptest.NewRequest("POST", "/api/v1/billing/checkout", bytes.NewReader(body))
	req = req.WithContext(ctxWithTenant("t_1"))
	rr := httptest.NewRecorder()
	h.StartCheckout(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

// TestOpenPortal_HappyPath returns the portal URL.
func TestOpenPortal_HappyPath(t *testing.T) {
	svc := &mockBillingSvc{
		portalResp: billing.PortalSession{URL: "https://billing.stripe.com/session/abc"},
	}
	h := NewBillingHandler(svc)

	body, _ := json.Marshal(PortalRequest{ReturnURL: "https://app.example.com/account"})
	req := httptest.NewRequest("POST", "/api/v1/billing/portal", bytes.NewReader(body))
	req = req.WithContext(ctxWithTenant("t_1"))
	rr := httptest.NewRecorder()
	h.OpenPortal(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp PortalResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if resp.PortalURL != "https://billing.stripe.com/session/abc" {
		t.Errorf("portal_url = %q, want stripe URL", resp.PortalURL)
	}
}

// TestOpenPortal_NoSubscription: 404 when the tenant has no row.
func TestOpenPortal_NoSubscription(t *testing.T) {
	svc := &mockBillingSvc{portalErr: billing.ErrNoSubscription}
	h := NewBillingHandler(svc)

	body, _ := json.Marshal(PortalRequest{ReturnURL: "https://app.example.com/account"})
	req := httptest.NewRequest("POST", "/api/v1/billing/portal", bytes.NewReader(body))
	req = req.WithContext(ctxWithTenant("t_1"))
	rr := httptest.NewRecorder()
	h.OpenPortal(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "no subscription for tenant") {
		t.Errorf("body = %q, want error message about subscription", rr.Body.String())
	}
}

// TestGetSubscription_HappyPath returns the local row.
func TestGetSubscription_HappyPath(t *testing.T) {
	periodEnd := time.Now().Add(30 * 24 * time.Hour)
	svc := &mockBillingSvc{
		getSub: domain.BillingSubscription{
			TenantID:           "t_1",
			Provider:           domain.ProviderStripe,
			ProviderCustomerID: "cus_abc",
			Plan:               "pro",
			Status:             domain.SubscriptionActive,
			CurrentPeriodEnd:   &periodEnd,
		},
	}
	h := NewBillingHandler(svc)

	req := httptest.NewRequest("GET", "/api/v1/billing/subscription", nil)
	req = req.WithContext(ctxWithTenant("t_1"))
	rr := httptest.NewRecorder()
	h.GetSubscription(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var got domain.BillingSubscription
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if got.Plan != "pro" || got.Status != domain.SubscriptionActive {
		t.Errorf("got %+v, want plan=pro status=active", got)
	}
}

// TestGetSubscription_NoRow: 404.
func TestGetSubscription_NoRow(t *testing.T) {
	svc := &mockBillingSvc{getSubErr: billing.ErrNoSubscription}
	h := NewBillingHandler(svc)

	req := httptest.NewRequest("GET", "/api/v1/billing/subscription", nil)
	req = req.WithContext(ctxWithTenant("t_1"))
	rr := httptest.NewRecorder()
	h.GetSubscription(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rr.Code)
	}
}

// TestStripeWebhook_HappyPath: 200 on successful dispatch.
func TestStripeWebhook_HappyPath(t *testing.T) {
	svc := &mockBillingSvc{}
	h := NewBillingHandler(svc)

	body := []byte(`{"id":"evt_1","type":"customer.subscription.updated"}`)
	req := httptest.NewRequest("POST", "/api/v1/billing/webhook", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", "t=123,v1=abc")
	rr := httptest.NewRecorder()
	h.StripeWebhook(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if svc.handleWebhookCalls != 1 {
		t.Errorf("HandleWebhook calls = %d, want 1", svc.handleWebhookCalls)
	}
	if string(svc.lastBody) != string(body) {
		t.Errorf("body = %q, want %q", svc.lastBody, body)
	}
	if svc.lastHeaders.Get("Stripe-Signature") != "t=123,v1=abc" {
		t.Errorf("Stripe-Signature header = %q, want forwarded", svc.lastHeaders.Get("Stripe-Signature"))
	}
}

// TestStripeWebhook_BadSignature: 400 when the service returns a
// signature-mismatch error. The error message contains "signature"
// so isSignatureFailure sniffs it correctly.
func TestStripeWebhook_BadSignature(t *testing.T) {
	svc := &mockBillingSvc{handleWebhookErr: errors.New("stripe: invalid webhook signature: bad hash")}
	h := NewBillingHandler(svc)

	req := httptest.NewRequest("POST", "/api/v1/billing/webhook", bytes.NewReader([]byte(`{"id":"x"}`)))
	rr := httptest.NewRecorder()
	h.StripeWebhook(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "invalid signature") {
		t.Errorf("body = %q, want 'invalid signature'", rr.Body.String())
	}
}

// TestStripeWebhook_TenantUnresolved: 422 status. Handler maps
// ErrTenantUnresolved to BillingTenantUnresolvedCtx, which emits
// http.StatusUnprocessableEntity with the BILLING_TENANT_UNRESOLVED
// error code. The distinction from 400 matters because Stripe's
// retry semantics treat 4xx as "stop retrying" — we want 422 (not
// 400) so the merchant knows the webhook was syntactically valid
// but the event cannot be attributed to any tenant (and therefore
// should be inspected, not auto-retried).
func TestStripeWebhook_TenantUnresolved(t *testing.T) {
	svc := &mockBillingSvc{handleWebhookErr: billing.ErrTenantUnresolved}
	h := NewBillingHandler(svc)

	req := httptest.NewRequest("POST", "/api/v1/billing/webhook", bytes.NewReader([]byte(`{"id":"x"}`)))
	rr := httptest.NewRecorder()
	h.StripeWebhook(rr, req)

	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "tenant unresolved") {
		t.Errorf("body = %q, want 'tenant unresolved'", rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "BILLING_TENANT_UNRESOLVED") {
		t.Errorf("body = %q, want code 'BILLING_TENANT_UNRESOLVED'", rr.Body.String())
	}
}

// TestStripeWebhook_DBError: 500 on a generic internal error.
func TestStripeWebhook_DBError(t *testing.T) {
	svc := &mockBillingSvc{handleWebhookErr: errors.New("db: connection refused")}
	h := NewBillingHandler(svc)

	req := httptest.NewRequest("POST", "/api/v1/billing/webhook", bytes.NewReader([]byte(`{"id":"x"}`)))
	rr := httptest.NewRecorder()
	h.StripeWebhook(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rr.Code)
	}
}

// TestStripeWebhook_UnknownEvent_200: an unhandled event type
// (signature verified, but the service doesn't dispatch on it) must
// return 200, not 500. Returning 5xx makes the merchant retry
// forever and burn its 3-day retry window on event classes we
// intentionally ignore (charge.succeeded, customer.created, etc.).
//
// Issue #419 review follow-up.
func TestStripeWebhook_UnknownEvent_200(t *testing.T) {
	svc := &mockBillingSvc{handleWebhookErr: billing.ErrUnknownEvent}
	h := NewBillingHandler(svc)

	req := httptest.NewRequest("POST", "/api/v1/billing/webhook", bytes.NewReader([]byte(`{"id":"evt_charge_succeeded"}`)))
	rr := httptest.NewRecorder()
	h.StripeWebhook(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (must not 5xx on unknown event type)", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "ignored") {
		t.Errorf("body = %q, want 'ignored' status", rr.Body.String())
	}
}
