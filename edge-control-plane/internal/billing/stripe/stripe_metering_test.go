package stripe

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	stripe "github.com/stripe/stripe-go/v82"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/billing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// recvBody is the minimal shape Stripe's billing_meter_event endpoint
// returns. We only need to satisfy the SDK's JSON-shape check (it
// looks for an `id` field), not the full BillingMeterEvent.
type recvBody struct {
	ID string `json:"id"`
}

// TestName_returns_provider_stripe asserts the row-stamp contract —
// every dispatched usage event must record provider="stripe" on the
// billing_usage_events row.
func TestName_returns_provider_stripe(t *testing.T) {
	p := NewMetering(billing.StripeConfig{}, nil)
	if got, want := p.Name(), domain.ProviderStripe; got != want {
		t.Fatalf("Name() = %q, want %q", got, want)
	}
}

// TestRecordUsage_dispatches_with_idempotency_key covers the happy path:
// the impl POSTs to /v1/billing/meter_events with the right event_name,
// Identifier = idempotency_key, and the Idempotency-Key header set
// (belt-and-suspenders alongside the Identifier field).
func TestRecordUsage_dispatches_with_idempotency_key(t *testing.T) {
	var gotIDKey string
	var gotEventName string
	var gotIdentifier string
	var gotTimestamp int64
	var gotPayload map[string]string
	var gotPath string
	var gotMethod string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotMethod = r.Method
		gotIDKey = r.Header.Get("Idempotency-Key")
		// Decode the form-encoded body Stripe sends for /v1/...
		// endpoints — the SDK uses form encoding, not JSON, on the
		// POST path. Form values may appear multiple times, but for
		// BillingMeterEventParams we expect each scalar once.
		_ = r.ParseForm()
		gotEventName = r.FormValue("event_name")
		gotIdentifier = r.FormValue("identifier")
		if ts := r.FormValue("timestamp"); ts != "" {
			n, _ := strconv.ParseInt(ts, 10, 64)
			gotTimestamp = n
		}
		gotPayload = map[string]string{
			"stripe_customer_id": r.FormValue("payload[stripe_customer_id]"),
			"value":              r.FormValue("payload[value]"),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(recvBody{ID: "evt_test_123"})
	}))
	defer srv.Close()

	useTestBackend(t, srv.URL)

	recorded := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	p := NewMetering(billing.StripeConfig{
		SecretKey: "sk_test_xyz",
		MeterSubscriptionItemIDs: map[string]map[string]string{
			"t_demo": {
				"resident_seconds": "si_test_demo_resident",
			},
		},
	}, map[domain.MeterKind]string{
		domain.MeterKindResidentSeconds: "resident_seconds_meter",
	})

	err := p.RecordUsage(context.Background(), billing.MeterUsage{
		TenantID:       "t_demo",
		Kind:           domain.MeterKindResidentSeconds,
		Quantity:       30,
		IdempotencyKey: "t_demo:resident_seconds:dedupe-abc",
		RecordedAt:     recorded,
	})
	if err != nil {
		t.Fatalf("RecordUsage returned %v, want nil", err)
	}

	if gotPath != "/v1/billing/meter_events" {
		t.Errorf("path = %q, want /v1/billing/meter_events", gotPath)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotEventName != "resident_seconds_meter" {
		t.Errorf("event_name = %q, want resident_seconds_meter", gotEventName)
	}
	if gotIdentifier != "t_demo:resident_seconds:dedupe-abc" {
		t.Errorf("identifier = %q, want t_demo:resident_seconds:dedupe-abc", gotIdentifier)
	}
	if gotIDKey != "t_demo:resident_seconds:dedupe-abc" {
		t.Errorf("Idempotency-Key header = %q, want the same string", gotIDKey)
	}
	if gotTimestamp != recorded.Unix() {
		t.Errorf("timestamp = %d, want %d", gotTimestamp, recorded.Unix())
	}
	if gotPayload["stripe_customer_id"] != "si_test_demo_resident" {
		t.Errorf("payload[stripe_customer_id] = %q, want si_test_demo_resident", gotPayload["stripe_customer_id"])
	}
	if gotPayload["value"] != "30" {
		t.Errorf("payload[value] = %q, want 30", gotPayload["value"])
	}
}

// TestRecordUsage_missing_subscription_item_id_is_terminal covers the
// "operator hasn't configured this tenant yet" case. The Stripe API
// would 4xx the call, so we short-circuit with ErrNoSubscription and
// return a wrapped ErrTerminal so the drainer stops retrying.
func TestRecordUsage_missing_subscription_item_id_is_terminal(t *testing.T) {
	// No Stripe call should land — assert by leaving APIBase unset
	// and tracking any attempts via a counter that stays zero.
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	useTestBackend(t, srv.URL)

	p := NewMetering(billing.StripeConfig{SecretKey: "sk_test"}, nil)
	err := p.RecordUsage(context.Background(), billing.MeterUsage{
		TenantID:       "t_unconfigured",
		Kind:           domain.MeterKindRequestCount,
		Quantity:       1,
		IdempotencyKey: "t_unconfigured:request_count:x",
		RecordedAt:     time.Now(),
	})
	if err == nil {
		t.Fatal("RecordUsage returned nil, want ErrNoSubscription")
	}
	if !errorIs(err, ErrNoSubscription) {
		t.Errorf("err = %v, want wrap of ErrNoSubscription", err)
	}
	if !errorIs(err, billing.ErrTerminal) {
		t.Errorf("err = %v, want wrap of billing.ErrTerminal", err)
	}
	if atomic.LoadInt32(&attempts) != 0 {
		t.Errorf("expected zero Stripe calls, got %d", attempts)
	}
}

// TestRecordUsage_missing_event_name_is_terminal covers the case
// where the operator configured SubscriptionItemIDs but forgot to map
// the domain MeterKind onto a Stripe meter event_name. Same terminal
// posture as the missing-id case — a retry can't conjure config.
func TestRecordUsage_missing_event_name_is_terminal(t *testing.T) {
	p := NewMetering(billing.StripeConfig{
		SecretKey: "sk_test",
		MeterSubscriptionItemIDs: map[string]map[string]string{
			"t_demo": {"resident_seconds": "si_test_demo_resident"},
		},
	}, nil) // meterEventNames empty
	err := p.RecordUsage(context.Background(), billing.MeterUsage{
		TenantID:       "t_demo",
		Kind:           domain.MeterKindResidentSeconds,
		Quantity:       30,
		IdempotencyKey: "t_demo:resident_seconds:x",
		RecordedAt:     time.Now(),
	})
	if err == nil {
		t.Fatal("RecordUsage returned nil, want missing-event-name error")
	}
	if !errorIs(err, ErrNoSubscription) {
		t.Errorf("err = %v, want wrap of ErrNoSubscription", err)
	}
	if !errorIs(err, billing.ErrTerminal) {
		t.Errorf("err = %v, want wrap of billing.ErrTerminal", err)
	}
}

// TestRecordUsage_stripe_4xx_wraps_terminal covers the case where
// Stripe rejects the request itself (e.g. invalid API key, missing
// meter). The drainer must see ErrTerminal so it routes to
// MarkProcessed-with-warn rather than retry-with-backoff.
func TestRecordUsage_stripe_4xx_wraps_terminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"type":"invalid_request_error","code":"resource_missing","message":"No such meter","param":"event_name"}}`))
	}))
	defer srv.Close()
	useTestBackend(t, srv.URL)

	p := NewMetering(billing.StripeConfig{
		SecretKey: "sk_test",
		MeterSubscriptionItemIDs: map[string]map[string]string{
			"t_demo": {"resident_seconds": "si_test_demo_resident"},
		},
	}, map[domain.MeterKind]string{domain.MeterKindResidentSeconds: "meter_x"})

	err := p.RecordUsage(context.Background(), billing.MeterUsage{
		TenantID:       "t_demo",
		Kind:           domain.MeterKindResidentSeconds,
		Quantity:       30,
		IdempotencyKey: "t_demo:resident_seconds:x",
		RecordedAt:     time.Now(),
	})
	if err == nil {
		t.Fatal("RecordUsage returned nil, want terminal error on 400")
	}
	if !errorIs(err, billing.ErrTerminal) {
		t.Errorf("err = %v, want wrap of billing.ErrTerminal (4xx must be terminal)", err)
	}
}

// TestRecordUsage_stripe_5xx_does_not_wrap_terminal covers the case
// where Stripe is having a bad day. The drainer must see a non-
// terminal error so it retries with exponential backoff. We don't
// wrap ErrTerminal here — that's the contract that drives the
// drainer's terminal/transient decision.
func TestRecordUsage_stripe_5xx_does_not_wrap_terminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"type":"api_error","message":"Service unavailable"}}`))
	}))
	defer srv.Close()
	useTestBackend(t, srv.URL)

	p := NewMetering(billing.StripeConfig{
		SecretKey: "sk_test",
		MeterSubscriptionItemIDs: map[string]map[string]string{
			"t_demo": {"resident_seconds": "si_test_demo_resident"},
		},
	}, map[domain.MeterKind]string{domain.MeterKindResidentSeconds: "meter_x"})

	err := p.RecordUsage(context.Background(), billing.MeterUsage{
		TenantID:       "t_demo",
		Kind:           domain.MeterKindResidentSeconds,
		Quantity:       30,
		IdempotencyKey: "t_demo:resident_seconds:x",
		RecordedAt:     time.Now(),
	})
	if err == nil {
		t.Fatal("RecordUsage returned nil, want transient error on 500")
	}
	if errorIs(err, billing.ErrTerminal) {
		t.Errorf("err = %v, must NOT wrap billing.ErrTerminal (5xx must be transient)", err)
	}
}

// errorIs is a thin wrapper over errors.Is so the assertion lines
// read consistently. The test bodies use it in chains that wrap
// both billing.ErrTerminal (outer) and ErrNoSubscription (inner)
// — errors.Is handles the multi-wrap case directly.
func errorIs(err, target error) bool { return errors.Is(err, target) }

// IMPORTANT — shared-helper convention for this package:
// When you add a new *_test.go file in package stripe that needs its
// own backend override (or any other helper that lives at package
// scope in a _test.go file), suffix the helper with a per-file tag
// (e.g. `useTestBackendProvider`, `useTestBackendMetering`). The Go
// test build compiles every _test.go in the package into one binary,
// so a same-named helper in a sibling file causes a hard build
// failure (`useTestBackend redeclared in this block`). The provider
// tests in stripe_test.go use `useTestBackendProvider` for exactly
// this reason; see that file's helper comment for the parallel
// pattern.

// useTestBackend reroutes stripe-go's API calls to the test server
// for the duration of the test. v82 builds a `*stripe.Backends` via
// NewBackendsWithConfig(URL=...) and installs it through
// SetBackend(stripe.APIBackend, backends.API). Cleanup restores the
// production backend so subsequent tests aren't accidentally routed
// to the dead test server URL.
//
// Calling stripe.SetBackend with a brand-new Backend (not a URL
// mutation) is also the only safe pattern when tests run in parallel
// — each test gets its own server and its own backend, no shared
// global state to race on.
func useTestBackend(t *testing.T, srvURL string) {
	t.Helper()
	backends := stripe.NewBackendsWithConfig(&stripe.BackendConfig{
		URL: stripe.String(srvURL),
	})
	stripe.SetBackend(stripe.APIBackend, backends.API)
	t.Cleanup(func() {
		// Reset to the production backend by constructing a fresh
		// one with no URL override (defaults to the live API).
		prodBackends := stripe.NewBackendsWithConfig(&stripe.BackendConfig{})
		stripe.SetBackend(stripe.APIBackend, prodBackends.API)
	})
}
