package stripe

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/billing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	stripelib "github.com/stripe/stripe-go/v82"
	stripewebhook "github.com/stripe/stripe-go/v82/webhook"
)

func TestProviderName(t *testing.T) {
	p := New(billing.StripeConfig{})
	if got := p.Name(); got != domain.ProviderStripe {
		t.Fatalf("Name() = %q, want %q", got, domain.ProviderStripe)
	}
}

func TestCreateCheckoutSessionHappyPath(t *testing.T) {
	var createdCustomer atomic.Bool
	var createdSession atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/customers":
			if got := r.URL.Query().Get("limit"); got != "1" {
				t.Fatalf("customer list limit = %q, want 1", got)
			}
			_, _ = w.Write([]byte(`{"object":"list","data":[],"has_more":false}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/customers":
			mustParseForm(t, r)
			if got := r.Form.Get("metadata[tenant_id]"); got != "t_checkout" {
				t.Fatalf("created customer metadata[tenant_id] = %q", got)
			}
			createdCustomer.Store(true)
			_, _ = w.Write([]byte(`{"id":"cus_checkout","object":"customer","metadata":{"tenant_id":"t_checkout"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/checkout/sessions":
			mustParseForm(t, r)
			want := map[string]string{
				"mode":                    "subscription",
				"customer":                "cus_checkout",
				"client_reference_id":     "t_checkout",
				"success_url":             "https://app.example/success",
				"cancel_url":              "https://app.example/cancel",
				"line_items[0][price]":    "price_pro",
				"line_items[0][quantity]": "1",
			}
			for key, wantValue := range want {
				if got := r.Form.Get(key); got != wantValue {
					t.Fatalf("checkout form %s = %q, want %q", key, got, wantValue)
				}
			}
			createdSession.Store(true)
			_, _ = w.Write([]byte(`{"id":"cs_checkout","object":"checkout.session","url":"https://checkout.stripe.com/c/pay/cs_checkout","expires_at":1893456000}`))
		default:
			t.Fatalf("unexpected Stripe request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()
	useTestBackendProvider(t, srv.URL)

	p := New(billing.StripeConfig{
		SecretKey: "sk_test_checkout",
		PriceIDs:  map[string]string{"pro": "price_pro"},
	})
	sess, err := p.CreateCheckoutSession(context.Background(), billing.CheckoutInput{
		TenantID:   "t_checkout",
		Plan:       "pro",
		SuccessURL: "https://app.example/success",
		CancelURL:  "https://app.example/cancel",
	})
	if err != nil {
		t.Fatalf("CreateCheckoutSession returned error: %v", err)
	}
	if !createdCustomer.Load() {
		t.Fatal("CreateCheckoutSession did not create a missing customer")
	}
	if !createdSession.Load() {
		t.Fatal("CreateCheckoutSession did not create a checkout session")
	}
	if sess.ID != "cs_checkout" {
		t.Fatalf("session ID = %q", sess.ID)
	}
	if sess.URL != "https://checkout.stripe.com/c/pay/cs_checkout" {
		t.Fatalf("session URL = %q", sess.URL)
	}
	if want := time.Unix(1893456000, 0); !sess.ExpiresAt.Equal(want) {
		t.Fatalf("expires_at = %s, want %s", sess.ExpiresAt, want)
	}
}

func TestCreateCheckoutSessionRejectsUnknownPlanWithoutCallingStripe(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		t.Fatalf("unexpected Stripe request: %s %s", r.Method, r.URL.String())
	}))
	defer srv.Close()
	useTestBackendProvider(t, srv.URL)

	p := New(billing.StripeConfig{SecretKey: "sk_test_unknown", PriceIDs: map[string]string{"pro": "price_pro"}})
	_, err := p.CreateCheckoutSession(context.Background(), billing.CheckoutInput{TenantID: "t_checkout", Plan: "business"})
	if err == nil || !strings.Contains(err.Error(), `no price_id configured for plan "business"`) {
		t.Fatalf("CreateCheckoutSession error = %v, want unknown-plan error", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("Stripe request count = %d, want 0", got)
	}
}

func TestCreateCheckoutSessionSurfacesStripeErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/customers" {
			t.Fatalf("unexpected Stripe request: %s %s", r.Method, r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"message":"stripe unavailable","type":"api_error"}}`))
	}))
	defer srv.Close()
	useTestBackendProvider(t, srv.URL)

	p := New(billing.StripeConfig{
		SecretKey: "sk_test_error",
		PriceIDs:  map[string]string{"pro": "price_pro"},
	})
	_, err := p.CreateCheckoutSession(context.Background(), billing.CheckoutInput{TenantID: "t_checkout", Plan: "pro"})
	// The wrapped error must mention both our wrapper prefix
	// ("lookup/create customer") AND the upstream Stripe message
	// ("stripe unavailable"). Pinning the upstream substring prevents
	// a future refactor from accidentally swallowing the original
	// error in a way that hides the merchant-side root cause from
	// the operator.
	if err == nil ||
		!strings.Contains(err.Error(), "lookup/create customer") ||
		!strings.Contains(err.Error(), "stripe unavailable") {
		t.Fatalf("CreateCheckoutSession error = %v, want wrapped Stripe error mentioning both prefixes", err)
	}
}

func TestCreatePortalSessionReturnsErrNoSubscriptionWhenCustomerMissing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/customers" {
			t.Fatalf("unexpected Stripe request: %s %s", r.Method, r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[],"has_more":false}`))
	}))
	defer srv.Close()
	useTestBackendProvider(t, srv.URL)

	p := New(billing.StripeConfig{SecretKey: "sk_test_portal_missing"})
	_, err := p.CreatePortalSession(context.Background(), "t_missing", "https://app.example/account")
	if !errors.Is(err, ErrNoSubscription) {
		t.Fatalf("CreatePortalSession error = %v, want ErrNoSubscription", err)
	}
}

func TestCreatePortalSessionHappyPath(t *testing.T) {
	var createdPortal atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/customers":
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"cus_portal","object":"customer","metadata":{"tenant_id":"t_portal"}}],"has_more":false}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/billing_portal/sessions":
			mustParseForm(t, r)
			if got := r.Form.Get("customer"); got != "cus_portal" {
				t.Fatalf("portal customer = %q", got)
			}
			if got := r.Form.Get("return_url"); got != "https://app.example/account" {
				t.Fatalf("portal return_url = %q", got)
			}
			createdPortal.Store(true)
			_, _ = w.Write([]byte(`{"id":"bps_portal","object":"billing_portal.session","customer":"cus_portal","url":"https://billing.stripe.com/p/session"}`))
		default:
			t.Fatalf("unexpected Stripe request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()
	useTestBackendProvider(t, srv.URL)

	p := New(billing.StripeConfig{SecretKey: "sk_test_portal"})
	ps, err := p.CreatePortalSession(context.Background(), "t_portal", "https://app.example/account")
	if err != nil {
		t.Fatalf("CreatePortalSession returned error: %v", err)
	}
	if !createdPortal.Load() {
		t.Fatal("CreatePortalSession did not create a portal session")
	}
	if ps.URL != "https://billing.stripe.com/p/session" {
		t.Fatalf("portal URL = %q", ps.URL)
	}
}

func TestGetSubscriptionReturnsNoSubscriptionWhenCustomerMissing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/customers" {
			t.Fatalf("unexpected Stripe request: %s %s", r.Method, r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[],"has_more":false}`))
	}))
	defer srv.Close()
	useTestBackendProvider(t, srv.URL)

	p := New(billing.StripeConfig{SecretKey: "sk_test_subscription"})
	_, err := p.GetSubscription(context.Background(), "t_missing")
	if !errors.Is(err, ErrNoSubscription) {
		t.Fatalf("GetSubscription error = %v, want ErrNoSubscription", err)
	}
}

func TestVerifyWebhookHappyPath(t *testing.T) {
	const secret = "whsec_test_secret"
	body := []byte(fmt.Sprintf(`{"id":"evt_checkout","object":"event","api_version":%q,"type":"checkout.session.completed","data":{"object":{"id":"cs_checkout","object":"checkout.session","client_reference_id":"t_checkout"}}}`, stripelib.APIVersion))

	p := New(billing.StripeConfig{WebhookSecret: secret})
	evt, err := p.VerifyWebhook(http.Header{"Stripe-Signature": []string{stripeSignatureHeaderProvider(body, secret)}}, body)
	if err != nil {
		t.Fatalf("VerifyWebhook returned error: %v", err)
	}
	if evt.EventID() != "evt_checkout" {
		t.Fatalf("event ID = %q", evt.EventID())
	}
	if evt.Provider() != domain.ProviderStripe {
		t.Fatalf("provider = %q", evt.Provider())
	}
	if evt.EventType() != domain.EventCheckoutCompleted {
		t.Fatalf("event type = %q", evt.EventType())
	}
	if evt.TenantID() != "t_checkout" {
		t.Fatalf("tenant ID = %q", evt.TenantID())
	}
	if evt.Status() != string(domain.SubscriptionActive) {
		t.Fatalf("status = %q", evt.Status())
	}
}

func TestVerifyWebhookBadSignature(t *testing.T) {
	const secret = "whsec_test_secret"
	body := []byte(fmt.Sprintf(`{"id":"evt_checkout","object":"event","api_version":%q,"type":"checkout.session.completed","data":{"object":{"client_reference_id":"t_checkout"}}}`, stripelib.APIVersion))
	headers := http.Header{"Stripe-Signature": []string{stripeSignatureHeaderProvider(body, secret)}}

	p := New(billing.StripeConfig{WebhookSecret: secret})
	_, err := p.VerifyWebhook(headers, []byte(`{"id":"evt_checkout","object":"event","type":"checkout.session.completed","data":{"object":{"client_reference_id":"tampered"}}}`))
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("VerifyWebhook error = %v, want ErrInvalidSignature", err)
	}
}

// TestVerifyWebhookMissingSignature pins the stripe.go:191 guard: a
// request with NO Stripe-Signature header at all (not just a malformed
// one) must surface ErrInvalidSignature so the handler can return 400
// and let Stripe retry. Without this test, a future refactor could
// silently let a missing header reach ConstructEventWithOptions and
// produce a less-helpful error.
func TestVerifyWebhookMissingSignature(t *testing.T) {
	p := New(billing.StripeConfig{WebhookSecret: "whsec_test_secret"})
	_, err := p.VerifyWebhook(http.Header{}, []byte(`{"id":"evt_checkout","type":"checkout.session.completed"}`))
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("VerifyWebhook error = %v, want ErrInvalidSignature", err)
	}
}

func mustParseForm(t *testing.T, r *http.Request) {
	t.Helper()
	if err := r.ParseForm(); err != nil {
		t.Fatalf("ParseForm: %v", err)
	}
}

// useTestBackendProvider reroutes stripe-go's API calls to the test
// server. The metering tests on main define a same-named helper
// (useTestBackend in stripe_metering_test.go); this copy is scoped
// to this file so the package doesn't see two declarations when
// both _test.go files compile in one test binary. The shape is
// identical to the metering-side helper: NewBackendsWithConfig{URL: ...}
// on the API backend, restored in t.Cleanup.
func useTestBackendProvider(t *testing.T, srvURL string) {
	t.Helper()
	backends := stripelib.NewBackendsWithConfig(&stripelib.BackendConfig{
		URL: stripelib.String(srvURL),
	})
	stripelib.SetBackend(stripelib.APIBackend, backends.API)
	t.Cleanup(func() {
		prodBackends := stripelib.NewBackendsWithConfig(&stripelib.BackendConfig{})
		stripelib.SetBackend(stripelib.APIBackend, prodBackends.API)
	})
}

// stripeSignatureHeader mirrors the metering helper but is scoped to
// this file so the same package doesn't see two declarations.
func stripeSignatureHeaderProvider(body []byte, secret string) string {
	timestamp := time.Now()
	sig := stripewebhook.ComputeSignature(timestamp, body, secret)
	return fmt.Sprintf("t=%d,v1=%s", timestamp.Unix(), hex.EncodeToString(sig))
}
