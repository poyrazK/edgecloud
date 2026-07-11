package noop

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/billing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

func TestNameReturnsProviderNoop(t *testing.T) {
	p := New()
	if got := p.Name(); got != domain.ProviderNoop {
		t.Fatalf("Name() = %q, want %q", got, domain.ProviderNoop)
	}
}

func TestCreateCheckoutSessionReturnsDeterministicSession(t *testing.T) {
	p := New()
	before := time.Now()
	sess, err := p.CreateCheckoutSession(context.Background(), billing.CheckoutInput{TenantID: "t_noop", Plan: "pro"})
	if err != nil {
		t.Fatalf("CreateCheckoutSession returned error: %v", err)
	}
	if !strings.HasPrefix(sess.ID, "noop_") {
		t.Fatalf("session ID = %q, want noop_ prefix", sess.ID)
	}
	wantURL := "/api/v1/billing/subscription?dev=noop&session=" + sess.ID
	if sess.URL != wantURL {
		t.Fatalf("session URL = %q, want %q", sess.URL, wantURL)
	}
	if sess.ExpiresAt.Before(before.Add(29*time.Minute)) || sess.ExpiresAt.After(before.Add(31*time.Minute)) {
		t.Fatalf("expires_at = %s, want about 30 minutes from %s", sess.ExpiresAt, before)
	}

	again, err := p.CreateCheckoutSession(context.Background(), billing.CheckoutInput{TenantID: "t_noop", Plan: "pro"})
	if err != nil {
		t.Fatalf("second CreateCheckoutSession returned error: %v", err)
	}
	if again.ID != sess.ID {
		t.Fatalf("second session ID = %q, want deterministic %q", again.ID, sess.ID)
	}
}

func TestCreateCheckoutSessionRequiresTenantAndPlan(t *testing.T) {
	p := New()
	if _, err := p.CreateCheckoutSession(context.Background(), billing.CheckoutInput{Plan: "pro"}); err == nil || !strings.Contains(err.Error(), "tenantID required") {
		t.Fatalf("missing tenant error = %v, want tenantID required", err)
	}
	if _, err := p.CreateCheckoutSession(context.Background(), billing.CheckoutInput{TenantID: "t_noop"}); err == nil || !strings.Contains(err.Error(), "plan required") {
		t.Fatalf("missing plan error = %v, want plan required", err)
	}
}

func TestCreatePortalSessionReturnsErrNoSubscription(t *testing.T) {
	p := New()
	_, err := p.CreatePortalSession(context.Background(), "t_noop", "https://app.example/account")
	if !errors.Is(err, ErrNoSubscription) {
		t.Fatalf("CreatePortalSession error = %v, want ErrNoSubscription", err)
	}
}

func TestGetSubscriptionReturnsErrNoSubscription(t *testing.T) {
	p := New()
	_, err := p.GetSubscription(context.Background(), "t_noop")
	if !errors.Is(err, ErrNoSubscription) {
		t.Fatalf("GetSubscription error = %v, want ErrNoSubscription", err)
	}
}

func TestVerifyWebhookReturnsErrInvalidSignature(t *testing.T) {
	p := New()
	_, err := p.VerifyWebhook(http.Header{}, []byte(`{"id":"evt_noop"}`))
	if !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("VerifyWebhook error = %v, want ErrInvalidSignature", err)
	}
}
