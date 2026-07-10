package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/billing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/cache"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// --- mocks ----------------------------------------------------------------

// usageMockQuotaRepo satisfies QuotaRepositoryForUsage. Set quota=nil to
// simulate the "tenant has no quota row" case (the handler maps that
// to 404); set err to simulate a DB error.
//
// Named usageMockQuotaRepo (not mockQuotaRepo) to avoid colliding
// with the same-named mock in worker_test.go.
type usageMockQuotaRepo struct {
	quota *domain.Quota
	err   error
}

func (m *usageMockQuotaRepo) GetByTenantID(_ context.Context, _ string) (*domain.Quota, error) {
	return m.quota, m.err
}

// usageMockBillingRepo satisfies BillingRepositoryForUsage. The three
// fields map 1:1 to the three reads the usage service does.
type usageMockBillingRepo struct {
	sub       *domain.BillingSubscription
	subErr    error
	events    []domain.BillingEvent
	eventsErr error
}

func (m *usageMockBillingRepo) GetByTenant(_ context.Context, _ string) (*domain.BillingSubscription, error) {
	return m.sub, m.subErr
}

func (m *usageMockBillingRepo) ListEventsByTenant(_ context.Context, _ string, _, _ time.Time, _ int) ([]domain.BillingEvent, error) {
	return m.events, m.eventsErr
}

// usageMockPortalOpener satisfies ProviderPortalOpener. URL="", err=nil
// simulates "free-tier tenant with no Stripe customer"; err=ErrNoSubscription
// is the noop path; any other err is logged.
type usageMockPortalOpener struct {
	url string
	err error
}

func (m *usageMockPortalOpener) CreatePortalSession(_ context.Context, _ string, _ string) (portalSession, error) {
	return portalSession{URL: m.url}, m.err
}

// --- tests ----------------------------------------------------------------

// TestUsageService_FreeTenant_NoPortal verifies the happy path for a
// free-tier tenant: billing_status="active", upgrade_options
// populated, no portal URL (free tenants have no Stripe customer).
func TestUsageService_FreeTenant_NoPortal(t *testing.T) {
	quota, err := domain.QuotaForPlan("free")
	if err != nil {
		t.Fatalf("QuotaForPlan(free): %v", err)
	}
	quota.TenantID = "t_free"
	quota.UsedRequestCount = 25000 // 25% of 100_000 cap

	qRepo := &usageMockQuotaRepo{quota: &quota}
	bRepo := &usageMockBillingRepo{sub: nil, events: nil} // no sub row yet
	portal := &usageMockPortalOpener{err: billing.ErrNoSubscription}

	svc := NewUsageService(qRepo, bRepo, portal, nil, UsageServiceConfig{})

	now := time.Now().UTC()
	got, err := svc.GetUsage(context.Background(), "t_free", now.Add(-7*24*time.Hour), now, 50)
	if err != nil {
		t.Fatalf("GetUsage: %v", err)
	}
	if got == nil {
		t.Fatal("got nil, want payload")
	}
	if got.BillingStatus != domain.BillingActive {
		t.Errorf("BillingStatus = %q, want %q", got.BillingStatus, domain.BillingActive)
	}
	if got.CurrentPeriod.RequestsUsed != 25000 {
		t.Errorf("RequestsUsed = %d, want 25000", got.CurrentPeriod.RequestsUsed)
	}
	if got.CurrentPeriod.OutboundBytesCap != int64(1000)*1024*1024 {
		t.Errorf("OutboundBytesCap = %d, want %d (1000 MiB)", got.CurrentPeriod.OutboundBytesCap, 1000*1024*1024)
	}
	if got.CurrentPeriod.UsagePct == nil || *got.CurrentPeriod.UsagePct != 25.0 {
		t.Errorf("UsagePct = %v, want 25.0", got.CurrentPeriod.UsagePct)
	}
	if len(got.UpgradeOptions) == 0 {
		t.Error("UpgradeOptions empty, want paid tiers listed for free tenant")
	}
	if got.BillingPortalURL != nil {
		t.Errorf("BillingPortalURL = %v, want nil for free tenant without sub", *got.BillingPortalURL)
	}
}

// TestUsageService_PaidTenant_WithPortal verifies the paid-tenant path:
// no upgrade options, portal URL present.
func TestUsageService_PaidTenant_WithPortal(t *testing.T) {
	quota, err := domain.QuotaForPlan("pro")
	if err != nil {
		t.Fatalf("QuotaForPlan(pro): %v", err)
	}
	quota.TenantID = "t_pro"
	qRepo := &usageMockQuotaRepo{quota: &quota}

	periodEnd := time.Now().Add(30 * 24 * time.Hour)
	sub := &domain.BillingSubscription{
		TenantID:         "t_pro",
		Plan:             "pro",
		Status:           domain.SubscriptionActive,
		CurrentPeriodEnd: &periodEnd,
	}
	bRepo := &usageMockBillingRepo{sub: sub}
	portal := &usageMockPortalOpener{url: "https://billing.stripe.com/c/p/portal_123"}

	svc := NewUsageService(qRepo, bRepo, portal, nil, UsageServiceConfig{})

	now := time.Now().UTC()
	got, err := svc.GetUsage(context.Background(), "t_pro", now.Add(-7*24*time.Hour), now, 50)
	if err != nil {
		t.Fatalf("GetUsage: %v", err)
	}
	if got == nil {
		t.Fatal("got nil, want payload")
	}
	if got.BillingStatus != domain.BillingActive {
		t.Errorf("BillingStatus = %q, want active", got.BillingStatus)
	}
	if len(got.UpgradeOptions) != 0 {
		t.Errorf("UpgradeOptions = %v, want empty for paid tenant", got.UpgradeOptions)
	}
	if got.BillingPortalURL == nil || *got.BillingPortalURL != "https://billing.stripe.com/c/p/portal_123" {
		t.Errorf("BillingPortalURL = %v, want portal URL", got.BillingPortalURL)
	}
}

// TestUsageService_PastDue_ActionRequired verifies the billing_status
// mapping: a paid tenant whose latest subscription status is past_due
// sees action_required at the top of the response.
func TestUsageService_PastDue_ActionRequired(t *testing.T) {
	quota, _ := domain.QuotaForPlan("pro")
	quota.TenantID = "t_past_due"
	qRepo := &usageMockQuotaRepo{quota: &quota}

	sub := &domain.BillingSubscription{
		TenantID: "t_past_due",
		Plan:     "pro",
		Status:   domain.SubscriptionPastDue,
	}
	bRepo := &usageMockBillingRepo{sub: sub}
	portal := &usageMockPortalOpener{url: "https://billing.stripe.com/p/past_due"}

	svc := NewUsageService(qRepo, bRepo, portal, nil, UsageServiceConfig{})

	now := time.Now().UTC()
	got, err := svc.GetUsage(context.Background(), "t_past_due", now.Add(-7*24*time.Hour), now, 50)
	if err != nil {
		t.Fatalf("GetUsage: %v", err)
	}
	if got.BillingStatus != domain.BillingActionRequired {
		t.Errorf("BillingStatus = %q, want %q", got.BillingStatus, domain.BillingActionRequired)
	}
	// The portal URL is still surfaced so the tenant has a path to fix
	// the issue — the action_required status is the SIGNAL, not a
	// block on the link.
	if got.BillingPortalURL == nil {
		t.Error("BillingPortalURL nil, want portal URL even for past_due")
	}
}

// TestUsageService_Canceled_ActionRequired mirrors the past_due test
// for the canceled branch. Cancellation is a terminal state from the
// dashboard's point of view (the tenant needs to actively re-subscribe).
func TestUsageService_Canceled_ActionRequired(t *testing.T) {
	quota, _ := domain.QuotaForPlan("pro")
	quota.TenantID = "t_canceled"
	qRepo := &usageMockQuotaRepo{quota: &quota}

	sub := &domain.BillingSubscription{
		TenantID: "t_canceled",
		Plan:     "pro",
		Status:   domain.SubscriptionCanceled,
	}
	bRepo := &usageMockBillingRepo{sub: sub}
	portal := &usageMockPortalOpener{err: billing.ErrNoSubscription}

	svc := NewUsageService(qRepo, bRepo, portal, nil, UsageServiceConfig{})

	now := time.Now().UTC()
	got, _ := svc.GetUsage(context.Background(), "t_canceled", now.Add(-7*24*time.Hour), now, 50)
	if got.BillingStatus != domain.BillingActionRequired {
		t.Errorf("BillingStatus = %q, want action_required", got.BillingStatus)
	}
}

// TestUsageService_EventsProjected confirms the timeline events are
// projected into BillingEventTimelineEntry shape (payload_hash dropped)
// and the from/to are echoed back.
func TestUsageService_EventsProjected(t *testing.T) {
	quota, _ := domain.QuotaForPlan("free")
	quota.TenantID = "t_events"
	qRepo := &usageMockQuotaRepo{quota: &quota}

	now := time.Now().UTC()
	tenantID := "t_events"
	events := []domain.BillingEvent{
		{
			EventID:     "evt_1",
			Provider:    domain.ProviderStripe,
			EventType:   domain.EventCheckoutCompleted,
			TenantID:    &tenantID,
			ReceivedAt:  now.Add(-2 * time.Hour),
			ProcessedAt: &now,
			PayloadHash: "secret-hash-should-not-leak",
		},
	}
	bRepo := &usageMockBillingRepo{sub: nil, events: events}
	portal := &usageMockPortalOpener{err: billing.ErrNoSubscription}

	svc := NewUsageService(qRepo, bRepo, portal, nil, UsageServiceConfig{})

	from := now.Add(-7 * 24 * time.Hour)
	got, err := svc.GetUsage(context.Background(), "t_events", from, now, 50)
	if err != nil {
		t.Fatalf("GetUsage: %v", err)
	}
	if len(got.Events) != 1 {
		t.Fatalf("len(Events) = %d, want 1", len(got.Events))
	}
	if got.Events[0].EventID != "evt_1" {
		t.Errorf("EventID = %q, want evt_1", got.Events[0].EventID)
	}
	if got.Events[0].EventType != string(domain.EventCheckoutCompleted) {
		t.Errorf("EventType = %q, want checkout.completed", got.Events[0].EventType)
	}
	if got.Events[0].ProcessedAt == nil {
		t.Error("ProcessedAt nil, want stamped")
	}
	if !got.From.Equal(from) {
		t.Errorf("From = %v, want %v (echoed)", got.From, from)
	}
}

// TestUsageService_NoQuota_NilReturn confirms the (nil, nil) contract
// for a tenant without a quota row. The handler maps nil to 404;
// the service must NOT return an error in this case (it's a normal
// "tenant not provisioned yet" state).
func TestUsageService_NoQuota_NilReturn(t *testing.T) {
	qRepo := &usageMockQuotaRepo{quota: nil}
	bRepo := &usageMockBillingRepo{}
	portal := &usageMockPortalOpener{}

	svc := NewUsageService(qRepo, bRepo, portal, nil, UsageServiceConfig{})

	now := time.Now().UTC()
	got, err := svc.GetUsage(context.Background(), "t_missing", now.Add(-7*24*time.Hour), now, 50)
	if err != nil {
		t.Errorf("err = %v, want nil for missing quota", err)
	}
	if got != nil {
		t.Errorf("got = %+v, want nil for missing quota", got)
	}
}

// TestUsageService_QuotaRepoError propagates DB errors so the handler
// can return 500. No silent fallback to nil — the handler depends on
// the service's error to distinguish 404 (nil) from 500 (err).
func TestUsageService_QuotaRepoError(t *testing.T) {
	qRepo := &usageMockQuotaRepo{err: errors.New("connection refused")}
	bRepo := &usageMockBillingRepo{}
	portal := &usageMockPortalOpener{}

	svc := NewUsageService(qRepo, bRepo, portal, nil, UsageServiceConfig{})

	now := time.Now().UTC()
	_, err := svc.GetUsage(context.Background(), "t_db_dead", now.Add(-7*24*time.Hour), now, 50)
	if err == nil {
		t.Fatal("err = nil, want DB error to propagate")
	}
}

// TestUsageService_CacheHit_NoRefresh confirms a fresh cache hit
// returns the cached payload without touching any of the underlying
// repos. We verify this by leaving the mocks wired to return errors —
// a hit must short-circuit before reaching them.
func TestUsageService_CacheHit_NoRefresh(t *testing.T) {
	quota, _ := domain.QuotaForPlan("free")
	quota.TenantID = "t_cached"

	qRepo := &usageMockQuotaRepo{quota: &quota}
	bRepo := &usageMockBillingRepo{}
	portal := &usageMockPortalOpener{}

	c := cache.NewUsageCache(10*time.Second, 60*time.Second)
	svc := NewUsageService(qRepo, bRepo, portal, c, UsageServiceConfig{})

	now := time.Now().UTC()
	from := now.Add(-7 * 24 * time.Hour)

	// Warm the cache via a real read.
	if _, err := svc.GetUsage(context.Background(), "t_cached", from, now, 50); err != nil {
		t.Fatalf("warm: %v", err)
	}

	// Now arm the mocks to fail. A cache hit MUST NOT touch them.
	qRepo.err = errors.New("repo down after warm")
	bRepo.eventsErr = errors.New("events repo down after warm")
	bRepo.subErr = errors.New("sub repo down after warm")

	got, err := svc.GetUsage(context.Background(), "t_cached", from, now, 50)
	if err != nil {
		t.Fatalf("GetUsage on cache hit: %v", err)
	}
	if got == nil || got.TenantID != "t_cached" {
		t.Errorf("got = %+v, want cached t_cached", got)
	}
}

// TestUsageService_CacheStaleHit verifies that a stale-but-within-window
// hit returns the cached value AND fires a background refresh.
// We assert the refresh happens by checking that the underlying mocks
// are called at least once more after the stale read (the background
// goroutine completes within the test's wait window).
func TestUsageService_CacheStaleHit(t *testing.T) {
	quota, _ := domain.QuotaForPlan("free")
	quota.TenantID = "t_stale"

	var quotaCalls atomic.Int32
	qRepo := &callCountingQuotaRepo{quota: &quota, calls: &quotaCalls}
	bRepo := &usageMockBillingRepo{}
	portal := &usageMockPortalOpener{}

	c := cache.NewUsageCache(10*time.Second, 60*time.Second)
	svc := NewUsageService(qRepo, bRepo, portal, c, UsageServiceConfig{})

	now := time.Now().UTC()
	from := now.Add(-7 * 24 * time.Hour)

	// Warm the cache.
	if _, err := svc.GetUsage(context.Background(), "t_stale", from, now, 50); err != nil {
		t.Fatalf("warm: %v", err)
	}
	warmCalls := quotaCalls.Load()
	if warmCalls == 0 {
		t.Fatal("warm did not call quotaRepo")
	}

	// Force the entry past expiry but within max-stale.
	// We can't easily mutate now() inside the service; instead we
	// directly reach into the cache and re-Set with an already-expired
	// expiresAt via a TTL of 0 (which the cache rejects). Simplest:
	// drop the cache and re-Set with a 1ns TTL, then sleep past it.
	c.Invalidate("t_stale")
	c.Set("t_stale", &domain.TenantUsage{TenantID: "t_stale"}, 1*time.Nanosecond)
	time.Sleep(10 * time.Millisecond)

	got, err := svc.GetUsage(context.Background(), "t_stale", from, now, 50)
	if err != nil {
		t.Fatalf("stale GetUsage: %v", err)
	}
	if got == nil {
		t.Fatal("got nil on stale hit")
	}

	// Give the background refresh a moment to land.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if quotaCalls.Load() > warmCalls {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if quotaCalls.Load() <= warmCalls {
		t.Errorf("background refresh did not call quotaRepo (warm=%d now=%d)", warmCalls, quotaCalls.Load())
	}
}

// callCountingQuotaRepo is a thin wrapper over the mock that counts
// invocations so the stale-hit test can assert the background refresh
// actually fired. Uses atomic.Int32 because the background-refresh
// goroutine increments it concurrently with the test goroutine
// reading it under -race.
type callCountingQuotaRepo struct {
	quota *domain.Quota
	err   error
	calls *atomic.Int32
}

func (m *callCountingQuotaRepo) GetByTenantID(_ context.Context, _ string) (*domain.Quota, error) {
	m.calls.Add(1)
	return m.quota, m.err
}

// TestNextMonthBoundaryUTC exercises the period-end calculation used
// by buildCurrentPeriod. Boundary at end-of-month (e.g. Jan 31) must
// roll over to Feb 1, not Mar 3 — Go's time.Date normalizes overflow.
func TestNextMonthBoundaryUTC(t *testing.T) {
	cases := []struct {
		in   time.Time
		want time.Time
	}{
		{time.Date(2026, 7, 15, 12, 30, 0, 0, time.UTC), time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)},
		{time.Date(2026, 12, 31, 23, 59, 0, 0, time.UTC), time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)},
		{time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC), time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)},
	}
	for _, tc := range cases {
		got := nextMonthBoundaryUTC(tc.in)
		if !got.Equal(tc.want) {
			t.Errorf("nextMonthBoundaryUTC(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
