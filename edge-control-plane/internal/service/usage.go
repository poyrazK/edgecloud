package service

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/billing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/cache"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// QuotaRepositoryForUsage is the subset of repository.QuotaRepository
// used by UsageService. Mirrors the narrow-interface pattern at
// handler/quota.go:14-17 so the service can be tested without a
// full sqlmock-backed QuotaRepository.
type QuotaRepositoryForUsage interface {
	GetByTenantID(ctx context.Context, tenantID string) (*domain.Quota, error)
}

// BillingRepositoryForUsage is the subset of repository.BillingRepository
// used by UsageService. Exposes GetByTenant (full row, needed for
// current_period_end and cancel_at_period_end) and ListEventsByTenant
// (timeline query added in issue #421).
type BillingRepositoryForUsage interface {
	GetByTenant(ctx context.Context, tenantID string) (*domain.BillingSubscription, error)
	ListEventsByTenant(ctx context.Context, tenantID string, from, to time.Time, limit int) ([]domain.BillingEvent, error)
}

// ProviderPortalOpener is the subset of billing.Provider used by
// UsageService. Wraps CreatePortalSession so the service can be
// tested without instantiating the full billing.BillingProvider
// (which has Stripe-specific deps under internal/billing/stripe/).
//
// The full interface signature is preserved so the concrete
// billing.Provider satisfies this seam directly without adapter.
type ProviderPortalOpener interface {
	CreatePortalSession(ctx context.Context, tenantID string, returnURL string) (portalSession, error)
}

// portalSession is a local mirror of billing.PortalSession. The
// billing package returns its own type; we copy the single field we
// care about to avoid a one-way import from service → billing
// (which would risk a cycle when billing/service.go evolves).
type portalSession struct {
	URL string
}

// billingProviderAdapter converts a billing.BillingProvider (which
// returns billing.PortalSession) into our narrow ProviderPortalOpener
// seam (which returns portalSession). Defined here so app.go's
// constructor can pass the concrete provider through without a
// per-call allocation on the hot path.
type billingProviderAdapter struct{ inner billing.BillingProvider }

func (a billingProviderAdapter) CreatePortalSession(ctx context.Context, tenantID string, returnURL string) (portalSession, error) {
	sess, err := a.inner.CreatePortalSession(ctx, tenantID, returnURL)
	if err != nil {
		return portalSession{}, err
	}
	return portalSession{URL: sess.URL}, nil
}

// UsageService composes the per-tenant usage dashboard response
// (issue #421). It is read-only, idempotent, and safe to call from
// any goroutine — the cache handles singleflight and the underlying
// repos are stateless DB readers.
//
// Method: GetUsage(ctx, tenantID, from, to, limit) returns a fully
// populated *domain.TenantUsage. The handler translates nil to 404
// for the no-quota case; every other error is a 500.
type UsageService struct {
	quotaRepo      QuotaRepositoryForUsage
	billingRepo    BillingRepositoryForUsage
	portalOpener   ProviderPortalOpener
	cache          *cache.UsageCache
	defaultLimit   int
	defaultWindow  time.Duration
	log            *log.Logger
}

// UsageServiceConfig bundles the constructor inputs that aren't
// repos. The repos are passed positionally because they have no
// useful zero value; the cosmetic knobs go here so adding a new one
// doesn't break every test's NewUsageService call site.
type UsageServiceConfig struct {
	DefaultLimit  int           // events slice cap when the request omits limit (default 50)
	DefaultWindow time.Duration // default from→to span (default 30d)
	Logger        *log.Logger   // optional; nil falls back to log.Default
}

// NewUsageService builds a UsageService. portalOpener is the narrow
// seam: callers (the tests) can pass anything satisfying it; the app
// constructor wraps the concrete billing.BillingProvider via
// billingProviderAdapter so the service depends only on this
// package's types. cache may be nil for tests that don't care about
// caching (in which case every GetUsage is a synchronous refetch).
func NewUsageService(
	quotaRepo QuotaRepositoryForUsage,
	billingRepo BillingRepositoryForUsage,
	portalOpener ProviderPortalOpener,
	usageCache *cache.UsageCache,
	cfg UsageServiceConfig,
) *UsageService {
	if cfg.DefaultLimit <= 0 {
		cfg.DefaultLimit = 50
	}
	if cfg.DefaultWindow <= 0 {
		cfg.DefaultWindow = 30 * 24 * time.Hour
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	return &UsageService{
		quotaRepo:     quotaRepo,
		billingRepo:   billingRepo,
		portalOpener:  portalOpener,
		cache:         usageCache,
		defaultLimit:  cfg.DefaultLimit,
		defaultWindow: cfg.DefaultWindow,
		log:           cfg.Logger,
	}
}

// NewUsageServiceFromBillingProvider is a convenience constructor for
// app.go: it wraps the concrete billing.BillingProvider into the
// narrow ProviderPortalOpener seam so the call site doesn't have to.
// Tests should use NewUsageService directly with a fake opener.
func NewUsageServiceFromBillingProvider(
	quotaRepo QuotaRepositoryForUsage,
	billingRepo BillingRepositoryForUsage,
	billingProvider billing.BillingProvider,
	usageCache *cache.UsageCache,
	cfg UsageServiceConfig,
) *UsageService {
	var opener ProviderPortalOpener
	if billingProvider != nil {
		opener = billingProviderAdapter{inner: billingProvider}
	}
	return NewUsageService(quotaRepo, billingRepo, opener, usageCache, cfg)
}

// GetUsage returns the dashboard payload for tenantID over the
// window [from, to], with at most limit events in the timeline slice.
//
// Cache behavior:
//   - cache hit (fresh): return immediately, no DB call.
//   - cache hit (stale, within max-stale window): return stale AND
//     singleflight-kick a background refresh. Other concurrent
//     callers serve the same stale value until the refresh publishes.
//   - cache miss or stale past max-stale: do a synchronous refetch.
//
// Errors:
//   - sqlx.ErrNoRows from quotaRepo.GetByTenantID is translated to
//     (nil, nil) so the handler can return 404 without a sentinel
//     import. Other errors propagate.
//   - sub == nil is fine (free-tier tenant); billing_status falls to
//     "active" via domain.BillingStatusForSubscription(nil).
func (s *UsageService) GetUsage(ctx context.Context, tenantID string, from, to time.Time, limit int) (*domain.TenantUsage, error) {
	if limit <= 0 {
		limit = s.defaultLimit
	}
	now := time.Now()

	// 1. Try cache. If fresh, return. If stale, return stale + kick
	//    a singleflight refresh.
	if s.cache != nil {
		if payload, ok, _ := s.cache.TryGet(tenantID, now); ok {
			if s.cache.TryStartRefresh(tenantID) {
				go s.backgroundRefresh(tenantID)
			}
			return payload, nil
		}
	}

	// 2. Synchronous refetch.
	return s.refresh(ctx, tenantID, from, to, limit, now)
}

// refresh is the synchronous compute path. Called on a cache miss
// and (in a goroutine) on a stale hit. It also writes the result
// back to the cache so the next caller gets a fresh hit.
func (s *UsageService) refresh(ctx context.Context, tenantID string, from, to time.Time, limit int, now time.Time) (*domain.TenantUsage, error) {
	quota, err := s.quotaRepo.GetByTenantID(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	if quota == nil {
		return nil, nil
	}

	sub, err := s.billingRepo.GetByTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	events, err := s.billingRepo.ListEventsByTenant(ctx, tenantID, from, to, limit)
	if err != nil {
		return nil, err
	}

	out := &domain.TenantUsage{
		TenantID:      tenantID,
		BillingStatus: domain.BillingStatusForSubscription(sub),
		CurrentPeriod: buildCurrentPeriod(quota),
		Events:        projectTimeline(events),
		From:          from,
		To:            to,
	}

	if sub != nil {
		out.UpgradeOptions = domain.UpgradeOptionsForPlan(sub.Plan)
		out.BillingPortalURL = s.tryOpenPortal(ctx, sub.TenantID)
	} else {
		// No billing_subscriptions row → free-tier tenant that has
		// never started checkout. They get the same upgrade list as
		// a tenant whose row says plan="free".
		out.UpgradeOptions = domain.UpgradeOptionsForPlan("free")
	}

	if s.cache != nil {
		s.cache.Set(tenantID, out, s.cache.TTL())
	}
	return out, nil
}

// backgroundRefresh runs the refresh in a goroutine, catches panics,
// and clears the singleflight flag on completion. Errors are logged
// (not returned) because there's no caller waiting on them.
func (s *UsageService) backgroundRefresh(tenantID string) {
	defer func() {
		if s.cache != nil {
			s.cache.FinishRefresh(tenantID)
		}
		if r := recover(); r != nil {
			s.log.Printf("usage backgroundRefresh(%s): panic: %v", tenantID, r)
		}
	}()
	now := time.Now()
	// Background refresh uses a window derived from the cached
	// payload's From/To if possible, so we re-fetch the same range
	// the user was looking at. Falling back to default window is
	// acceptable (the cached entry stays valid until the next read).
	from, to := s.defaultWindowRange(now)
	if cached, ok, _ := s.cache.TryGet(tenantID, now); ok && cached != nil {
		from, to = cached.From, cached.To
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := s.refresh(ctx, tenantID, from, to, s.defaultLimit, now); err != nil {
		s.log.Printf("usage backgroundRefresh(%s): %v", tenantID, err)
	}
}

// tryOpenPortal calls the provider's CreatePortalSession and returns
// a non-nil pointer to the URL on success. Any error (including the
// per-provider ErrNoSubscription) is swallowed and logged; the
// dashboard renders no portal link in that case, which is the right
// behavior for free-tier tenants without a Stripe customer row.
func (s *UsageService) tryOpenPortal(ctx context.Context, tenantID string) *string {
	if s.portalOpener == nil {
		return nil
	}
	portal, err := s.portalOpener.CreatePortalSession(ctx, tenantID, "")
	if err != nil {
		// ErrNoSubscription is expected for free-tier tenants. Anything
		// else is logged so operators can spot a Stripe-side outage
		// from logs without the dashboard surfacing a 500.
		if !errors.Is(err, billing.ErrNoSubscription) {
			s.log.Printf("usage tryOpenPortal(%s): %v", tenantID, err)
		}
		return nil
	}
	if portal.URL == "" {
		return nil
	}
	u := portal.URL
	return &u
}

// defaultWindowRange returns the [now - defaultWindow, now] interval.
// Used by backgroundRefresh when there is no cached payload to copy
// the range from.
func (s *UsageService) defaultWindowRange(now time.Time) (time.Time, time.Time) {
	return now.Add(-s.defaultWindow), now
}

// buildCurrentPeriod derives the response-side CurrentPeriodUsage
// from the quotas row. PeriodEnd is computed as the next month
// boundary UTC relative to quota.QuotaPeriodStart (matches the
// heartbeat pipeline's monthly reset semantics). Caps are
// normalized to int64 bytes / request counts so the dashboard can
// render them without knowing the MaxOutboundMB MiB convention.
func buildCurrentPeriod(q *domain.Quota) domain.CurrentPeriodUsage {
	return domain.CurrentPeriodUsage{
		PeriodStart:       q.QuotaPeriodStart,
		PeriodEnd:         nextMonthBoundaryUTC(q.QuotaPeriodStart),
		RequestsUsed:      q.UsedRequestCount,
		RequestsCap:       int64(q.MaxRequestsPerMonth),
		OutboundBytesUsed: q.UsedOutboundBytes,
		OutboundBytesCap:  int64(q.MaxOutboundMB) * 1024 * 1024,
		UsagePct:          q.UsagePct(),
	}
}

// nextMonthBoundaryUTC returns the first instant of the month after t
// in UTC. Used to set CurrentPeriodUsage.PeriodEnd — the heartbeat
// pipeline rolls over at the UTC month boundary (see
// repository/quota.go addColumn).
func nextMonthBoundaryUTC(t time.Time) time.Time {
	y, m, _ := t.UTC().Date()
	return time.Date(y, m+1, 1, 0, 0, 0, 0, time.UTC)
}

// projectTimeline converts []domain.BillingEvent into the response
// shape []domain.BillingEventTimelineEntry. Drops payload_hash
// (internal dedup key) but preserves processed_at so the dashboard
// can show "pending" for events the control plane hasn't dispatched
// yet.
func projectTimeline(events []domain.BillingEvent) []domain.BillingEventTimelineEntry {
	if len(events) == 0 {
		return nil
	}
	out := make([]domain.BillingEventTimelineEntry, len(events))
	for i, e := range events {
		out[i] = domain.BillingEventTimelineEntry{
			EventID:     e.EventID,
			EventType:   string(e.EventType),
			ReceivedAt:  e.ReceivedAt,
			ProcessedAt: e.ProcessedAt,
		}
	}
	return out
}