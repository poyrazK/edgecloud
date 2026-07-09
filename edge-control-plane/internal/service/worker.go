package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/loophealth"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/jmoiron/sqlx"
	natsio "github.com/nats-io/nats.go"
)

// Sentinel errors for WorkerService Register.
var (
	ErrInvalidWorkerID = errors.New("invalid worker_id format: must be w_<region>_<uuid>")
	ErrRegionMismatch  = errors.New("region mismatch between worker_id and request region")
	ErrQuotaExceeded   = errors.New("max workers reached for tenant")
)

// heartbeatRecover is a defer helper used by the inner goroutines
// spawned inside SubscribeHeartbeats (the channel-depth monitor and
// the drain that calls handleHeartbeat). The outer loophealth recover
// in RunBackground cannot catch panics inside those goroutines —
// SubscribeHeartbeats returns nil once it has launched them.
//
// It is a method on *WorkerService so it can also bump the heartbeat
// loop's panic counter (review finding #4): the outer wrapper's
// recover runs only inside the SubscribeHeartbeats call itself, so a
// panic in the inner drain would otherwise leave
// loops.heartbeat.panics at 0 even though /health still showed the
// loop as "running".
func (s *WorkerService) heartbeatRecover() {
	if r := recover(); r != nil {
		log.Printf("heartbeat: panic recovered in inner goroutine: %v\n%s", r, debug.Stack())
		if s.tracker != nil {
			s.tracker.Get("heartbeat").RecordPanic()
		}
	}
}

// tenantRepoInterface defines the repository methods used by WorkerService
// for tenant-level operations (issue #155).
type tenantRepoInterface interface {
	GetByID(ctx context.Context, id string) (*domain.Tenant, error)
	GetForUpdate(ctx context.Context, id string) (*domain.Tenant, error)
	SetDisabledAt(ctx context.Context, tenantID string, at time.Time) error
	ClearDisabledAt(ctx context.Context, tenantID string) error
	WithTx(tx *sqlx.Tx) *repository.TenantRepository
}

// workerRepoInterface defines the repository methods used by WorkerService.
type workerRepoInterface interface {
	Upsert(ctx context.Context, tenantID string, req *domain.RegisterWorkerRequest) (bool, error)
	CountByTenant(ctx context.Context, tenantID string) (int, error)
	Delete(ctx context.Context, id string) error
	DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error)
	ListByTenant(ctx context.Context, tenantID string) ([]domain.Worker, error)
	GetByID(ctx context.Context, id string) (*domain.Worker, error)
	UpdateLastSeen(ctx context.Context, id string) error
	UpdateAddr(ctx context.Context, id, addr string) error
	UpsertStatus(ctx context.Context, ws *domain.WorkerStatus) error
	ListRunningAppTarget(ctx context.Context, tenantID, appName string) ([]domain.AppTarget, error)
	GetAppStatus(ctx context.Context, tenantID, appName string) (*domain.AppWorkerStatus, error)
}

// quotaRepoInterface defines the repository methods used by WorkerService.
type quotaRepoInterface interface {
	GetByTenantID(ctx context.Context, tenantID string) (*domain.Quota, error)
	AddOutboundBytes(ctx context.Context, tenantID string, delta uint64) (*domain.Quota, error)
	AddRequestCount(ctx context.Context, tenantID string, delta uint64) (*domain.Quota, error)
}

// activeRepoInterface defines the active_deployments methods used by
// the stability-window evaluator. Kept narrow so the evaluator can be
// unit-tested with a sqlmock without standing up the full
// ActiveDeploymentRepository (DB, query builder, etc.).
type activeRepoInterface interface {
	Get(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error)
	SetStableSince(ctx context.Context, tenantID, appName, deploymentID string, ts time.Time) error
	ClearStableSince(ctx context.Context, tenantID, appName string) error
	PromoteToLastGood(ctx context.Context, tenantID, appName, deploymentID string) error
	ListByTenant(ctx context.Context, tenantID string) ([]domain.ActiveDeployment, error)
	WithTx(tx *sqlx.Tx) *repository.ActiveDeploymentRepository
}

// defaultStableWindowSeconds is the default for `STABLE_WINDOW_SECONDS`
// when the env var is unset or unparseable. Set to 30 to match the
// worker's heartbeat_interval_secs default — promotion can fire on the
// second heartbeat after activation. Tunable downward for tenants who
// want faster promotion; tunable upward for noisy apps where a single
// successful heartbeat isn't enough signal.
const defaultStableWindowSeconds = 30

// dedupeCacheTTL bounds the in-memory dedupe cache used to skip
// re-applying metering deltas on JetStream redelivery (issue #418).
// Set to four heartbeat intervals (4 × 30s = 120s) so the cache
// comfortably outlives any plausible redelivery window without
// growing unboundedly. Operators tuning `EDGE_HEARTBEAT_INTERVAL_SECS`
// below 30s should be aware the cache may overlap with a future
// legitimate heartbeat; the failure mode is conservative
// (under-count, not over-count) — see metering_dedupe.rs for the
// discussion.
const dedupeCacheTTL = 4 * 30 * time.Second

// dedupeEntry tracks when a heartbeat dedupe ID was first seen so the
// cache can evict stale entries.
type dedupeEntry struct {
	expiresAt time.Time
}

// WorkerService handles worker lifecycle business logic.
type WorkerService struct {
	db           *sqlx.DB
	workerRepo   workerRepoInterface
	quotaRepo    quotaRepoInterface
	activeRepo   activeRepoInterface
	tenantRepo   tenantRepoInterface
	nc           *natsio.Conn
	stableWindow time.Duration
	metricsAgg   *MetricsAggregator
	// jsForTest overrides the JetStream publisher used by
	// notifyDisableTenant. nil in production (the function falls back
	// to nc.JetStream()); non-nil in tests so they can assert that an
	// empty task_update is published per region without spinning up
	// nats-server (issue #440).
	jsForTest jetstreamPublisher
	// tracker optionally receives liveness updates from the heartbeat
	// drain. A nil tracker is permitted (existing tests build
	// WorkerService without one); the drain skips Beat/RecordPanic
	// calls when it's nil. The tracker is wired by app.New so the
	// /health endpoint can surface heartbeat panics + freshness
	// (review findings #3 and #4).
	tracker *loophealth.Tracker
	// dedupeCache maps `dedupe_id` → expiry time. Entries are evicted
	// lazily on read — when a present-but-expired key is touched in
	// `dedupeSeen`, it is deleted and re-recorded with a fresh TTL.
	// Bounded by the number of active (worker, deployment) pairs at
	// any moment. Note: both `checkOutboundQuota` and `checkRequestCount`
	// call `dedupeSeen` per app on every heartbeat (two goroutines per
	// delivery), so a redelivered heartbeat does 2× Load + Store per
	// app — negligible at 30s cadence but worth knowing.
	dedupeCache sync.Map
}

// NewWorkerService creates a new WorkerService.
//
// `stableWindow` is the minimum time a deployment must be observed
// running before it becomes eligible for promotion to
// last_good_deployment_id (the safety-net pointer). Pass 0 to use
// the default (30s, configurable via STABLE_WINDOW_SECONDS env). The
// CLI accepts any non-negative integer; sub-second precision is not
// supported.
//
// `tracker` is optional (pass nil to skip liveness reporting — used
// in tests that build a WorkerService without a control-plane tracker).
// When non-nil, the heartbeat drain bumps
// `tracker.Get("heartbeat").Beat()` on every message and the inner
// recover helper calls `RecordPanic()` on a recovered panic
// (review findings #3 and #4).
func NewWorkerService(
	db *sqlx.DB,
	workerRepo *repository.WorkerRepository,
	quotaRepo *repository.QuotaRepository,
	activeRepo *repository.ActiveDeploymentRepository,
	tenantRepo *repository.TenantRepository,
	nc *natsio.Conn,
	stableWindow time.Duration,
	metricsAgg *MetricsAggregator,
	tracker *loophealth.Tracker,
) *WorkerService {
	if stableWindow <= 0 {
		stableWindow = time.Duration(defaultStableWindowSeconds) * time.Second
	}
	return &WorkerService{
		db:           db,
		workerRepo:   workerRepo,
		quotaRepo:    quotaRepo,
		activeRepo:   activeRepo,
		tenantRepo:   tenantRepo,
		nc:           nc,
		stableWindow: stableWindow,
		metricsAgg:   metricsAgg,
		tracker:      tracker,
	}
}

// AppTargetLookup is the narrow contract the deployment handler needs to
// answer "is this tenant's app currently routable?". Kept separate from
// the full WorkerService so handler tests can mock just the one method
// without standing up a NATS connection, a worker repo, and a quota repo.
type AppTargetLookup interface {
	GetAppTarget(ctx context.Context, tenantID, appName string) (*domain.AppTarget, error)
}

// Register creates or updates a worker record for a tenant.
// It is idempotent — if the worker already exists, it just updates last_seen.
func (s *WorkerService) Register(ctx context.Context, tenantID string, req *domain.RegisterWorkerRequest) error {
	// 1. Validate worker_id format: w_<region>_<uuid>
	if !domain.IsValidWorkerID(req.WorkerID) {
		return ErrInvalidWorkerID
	}

	// 2. Validate region in worker_id matches request region
	parts := strings.SplitN(req.WorkerID[2:], "_", 2)
	if len(parts) != 2 || parts[0] != req.Region {
		return ErrRegionMismatch
	}

	if tenantID == "*" || tenantID == "" {
		tenantID = "t_system"
	}

	// 3. Atomic upsert — handles both new registrations and re-registrations.
	// Uses ON CONFLICT DO NOTHING so concurrent inserts for the same worker_id
	// are safely deduplicated; the RETURNING clause tells us if a row was inserted.
	wasCreated, err := s.workerRepo.Upsert(ctx, tenantID, req)
	if err != nil {
		return fmt.Errorf("upserting worker: %w", err)
	}

	// 4. If this was a re-registration (worker already existed), we're done.
	if !wasCreated {
		return nil
	}

	// 5. New worker: enforce MaxWorkers quota.
	quota, err := s.quotaRepo.GetByTenantID(ctx, tenantID)
	if err != nil {
		// Rollback the newly inserted worker row.
		_ = s.workerRepo.Delete(ctx, req.WorkerID)
		return fmt.Errorf("getting quota: %w", err)
	}
	count, err := s.workerRepo.CountByTenant(ctx, tenantID)
	if err != nil {
		_ = s.workerRepo.Delete(ctx, req.WorkerID)
		return fmt.Errorf("counting workers: %w", err)
	}
	if count > quota.MaxWorkers {
		_ = s.workerRepo.Delete(ctx, req.WorkerID)
		return ErrQuotaExceeded
	}
	return nil
}

// ListByTenant returns all workers for a tenant.
func (s *WorkerService) ListByTenant(ctx context.Context, tenantID string) ([]domain.Worker, error) {
	return s.workerRepo.ListByTenant(ctx, tenantID)
}

// Get returns the worker row for the given workerID. Returns
// (nil, nil) when no row exists so callers can distinguish "not
// found" from "db error" via the sentinel in service.ErrWorkerNotFound
// if/when the handler wants a 404. Used by the HTTP /sync fallback
// endpoint (issue #53) to map a worker_id in the URL to a
// (tenantID, region) pair before calling ReconcileService.BuildFullSync.
func (s *WorkerService) Get(ctx context.Context, workerID string) (*domain.Worker, error) {
	return s.workerRepo.GetByID(ctx, workerID)
}

// SubscribeHeartbeats starts a background NATS subscription to
// edgecloud.heartbeats.* and upserts worker status on each message.
//
// Returns nil immediately after launching a single supervisor
// goroutine (runHeartbeatDrain) that owns the NATS subscription, the
// channel-depth monitor, and the message drain. The supervisor
// re-subscribes on a recovered panic from the inner drain
// (review finding #5) so a malformed heartbeat no longer leaves the
// subscription dangling.
//
// When s.tracker is non-nil, the drain bumps
// tracker.Get("heartbeat").Beat() per message (review finding #3)
// and the inner recover helper bumps RecordPanic() on a recovered
// panic (review finding #4).
func (s *WorkerService) SubscribeHeartbeats(ctx context.Context) error {
	if s.nc == nil {
		// No NATS connection — skip subscription (e.g., in tests)
		return nil
	}
	go s.runHeartbeatDrain(ctx)
	return nil
}

// runHeartbeatDrain is the supervisor goroutine. It loops over
// (subscribe → drain) cycles: when the inner drain exits (either
// because ctx was cancelled or because the inner drain panicked and
// was recovered), we unsubscribe cleanly, close the channel, and if
// ctx is still alive, sleep briefly and re-subscribe. This is the
// fix for review finding #5: the pre-fix code recovered the panic
// but unwound the goroutine without ever reaching the
// sub.Unsubscribe / close(ch) cleanup, leaking the subscription
// until process exit.
func (s *WorkerService) runHeartbeatDrain(ctx context.Context) {
	for {
		if err := s.subscribeAndDrain(ctx); err != nil {
			log.Printf("heartbeat: subscribe error: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
		// Brief backoff so a misconfigured NATS connection at startup
		// doesn't tight-loop.
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

// subscribeAndDrain opens one NATS subscription, runs the
// channel-depth monitor + message drain, and tears them down on
// either ctx cancellation or a recovered panic in the drain. Returns
// the (non-fatal) error from the initial subscription attempt; ctx
// cancellation is silent.
func (s *WorkerService) subscribeAndDrain(ctx context.Context) error {
	// Buffer 5000 messages to handle bursts. At 30s/heartbeat per worker,
	// this accommodates ~150k concurrent workers without overflow.
	ch := make(chan *natsio.Msg, 5000)
	sub, err := s.nc.ChanSubscribe("edgecloud.heartbeats.>", ch)
	if err != nil {
		return fmt.Errorf("subscribing to heartbeats: %w", err)
	}
	log.Printf("heartbeat: subscribed to edgecloud.heartbeats.> (buffer=%d)", cap(ch))

	// Monitor channel depth every minute and log when it's getting full
	// so operators can tune the buffer before messages are dropped.
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		defer s.heartbeatRecover()
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if depth := len(ch); depth > 4000 {
					log.Printf("heartbeat: channel depth = %d (capacity %d) — near capacity, messages may be dropped", depth, cap(ch))
				} else if depth > 2000 {
					log.Printf("heartbeat: channel depth = %d (capacity %d)", depth, cap(ch))
				}
			}
		}
	}()

	// Drain messages. Two exit paths look identical to the supervisor:
	//   1. ctx cancellation — clean unsubscribe, close channel,
	//      wait for the monitor to exit, return.
	//   2. Recovered panic in handleHeartbeat — heartbeatRecover()
	//      bumps the tracker (when present) and logs the stack, then
	//      breaks out of the loop. The deferred unsubscribe/close
	//      below still runs, so the channel and subscription are
	//      released before the supervisor re-subscribes.
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		defer s.heartbeatRecover()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-ch:
				s.handleHeartbeat(ctx, msg)
				if s.tracker != nil {
					s.tracker.Get("heartbeat").Beat()
				}
			}
		}
	}()

	// Wait for the drain to exit, then tear down. The supervisor
	// re-subscribes unconditionally when ctx is still alive, so a
	// recovered panic in the drain triggers a fresh NATS subscription
	// on the next loop iteration.
	<-drainDone
	if err := sub.Unsubscribe(); err != nil {
		log.Printf("heartbeat: failed to unsubscribe: %v", err)
	}
	close(ch)
	<-monitorDone
	return nil
}

func (s *WorkerService) handleHeartbeat(ctx context.Context, msg *natsio.Msg) {
	var hb struct {
		Type       string          `json:"type"`
		Timestamp  time.Time       `json:"timestamp"`
		WorkerID   string          `json:"worker_id"`
		Region     string          `json:"region"`
		WorkerAddr string          `json:"worker_addr"`
		Apps       json.RawMessage `json:"apps"`
		// TenantID is sent by the worker so the control plane can
		// scope stability evaluation to the right row. Heartbeats
		// from the same worker may carry different tenants (the
		// worker hosts multiple tenants in multi-tenant mode).
		TenantID string `json:"tenant_id"`
		// ClusterHeadroom carries capacity info from issue #85.
		// Preserved as json.RawMessage so the autoscaler can decode
		// it on its own (autoscaler is wired in PR #3); we just
		// persist the apps blob today. A pre-#85 worker simply
		// doesn't have the field, so this stays nil — backward-compat
		// is automatic because Go's json.Unmarshal ignores unknown
		// fields by default.
		ClusterHeadroom json.RawMessage `json:"cluster_headroom"`
	}
	if err := json.Unmarshal(msg.Data, &hb); err != nil {
		// Log the raw message preview so operators can diagnose wire format
		// mismatches (the root cause of issue #297). A best-effort partial
		// unmarshal extracts the worker_id for identification even when the
		// full struct fails.
		rawPreview := string(msg.Data)
		if len(rawPreview) > 1024 {
			rawPreview = rawPreview[:1024] + "..."
		}
		workerID := extractHeartbeatWorkerID(msg.Data)
		log.Printf("heartbeat: failed to parse from worker %s: %v (raw: %s)", workerID, err, rawPreview)
		return
	}

	// Extract tenant_id from the top-level field. When absent (pre-#297
	// workers), fall back to extracting it from the first app's status
	// in the apps map. This makes the auto-register fallback (below)
	// actually work instead of being dead code.
	tenantID := hb.TenantID
	if tenantID == "" && len(hb.Apps) > 0 {
		tenantID = extractTenantIDFromApps(hb.Apps)
	}

	if err := s.workerRepo.UpdateLastSeen(ctx, hb.WorkerID); err != nil {
		log.Printf("heartbeat: failed to update last_seen for %s: %v", hb.WorkerID, err)
	}
	// Only update the worker's public IP when the heartbeat actually carries
	// one — a heartbeat from a legacy worker (no EDGE_WORKER_ADDR) must not
	// clobber a previously-known good value.
	if hb.WorkerAddr != "" {
		if err := s.workerRepo.UpdateAddr(ctx, hb.WorkerID, hb.WorkerAddr); err != nil {
			log.Printf("heartbeat: failed to update ip for %s: %v", hb.WorkerID, err)
		}
	}

	ws := &domain.WorkerStatus{
		WorkerID:   hb.WorkerID,
		Apps:       hb.Apps,
		LastReport: hb.Timestamp,
	}
	if err := s.workerRepo.UpsertStatus(ctx, ws); err != nil {
		// The FK constraint `worker_status_worker_id_fkey` fires when
		// the worker hasn't been registered yet. Auto-register with a
		// skeleton row so the status can be persisted (issue #283).
		if strings.Contains(err.Error(), "worker_status_worker_id_fkey") && tenantID != "" {
			log.Printf("heartbeat: auto-registering unregistered worker %s (region=%s, tenant=%s)", hb.WorkerID, hb.Region, tenantID)
			if _, regErr := s.workerRepo.Upsert(ctx, tenantID, &domain.RegisterWorkerRequest{
				WorkerID: hb.WorkerID,
				Region:   hb.Region,
			}); regErr != nil {
				log.Printf("heartbeat: failed to auto-register worker %s: %v", hb.WorkerID, regErr)
			} else if retryErr := s.workerRepo.UpsertStatus(ctx, ws); retryErr != nil {
				log.Printf("heartbeat: failed to upsert status for %s after auto-register: %v", hb.WorkerID, retryErr)
			}
		} else {
			log.Printf("heartbeat: failed to upsert status for %s: %v", hb.WorkerID, err)
		}
	}

	// Log success so operators can monitor heartbeat health via structured
	// logs (grep: "heartbeat: processed from"). Note: len(hb.Apps) is the
	// raw JSON byte length, not the app count; we log it as a rough size
	// indicator. The actual app count would require decoding the JSON.
	log.Printf("heartbeat: processed from %s (region=%s, tenant=%s, apps_bytes=%d)",
		hb.WorkerID, hb.Region, tenantID, len(hb.Apps))

	// Stability-window evaluate. Only fires when the heartbeat
	// carries a tenant_id and apps, and when the activeRepo is wired
	// (nil in tests or bootstrap mode). The AppSpec on the wire is the
	// source of truth for "which apps is this worker serving for
	// this tenant right now?". Heartbeats without apps are treated
	// as the worker's "I'm idle" signal — nothing to evaluate.
	if s.activeRepo != nil && tenantID != "" && len(hb.Apps) > 0 {
		s.evaluateStability(ctx, tenantID, hb.Apps)
	}

	// Decode app statuses to sum outbound bytes and enforce the per-tenant
	// max_outbound_mb quota. Old workers omit outbound_bytes (defaults to 0),
	// which we treat as "no data" — we log but do not act on a 0-byte total
	// so a single old worker cannot cause a false quota violation.
	// Dispatched in a separate goroutine so DB round-trips in quota enforcement
	// do not block the NATS heartbeat drain goroutine (which has a fixed 5000-msg
	// buffer and will drop overflow messages if the drain stalls).
	//
	// context.WithoutCancel strips the subscriber's cancellation signal so that
	// a graceful shutdown (which cancels ctx) does not abort in-flight quota
	// writes — byte deltas must reach the DB even when the subscriber is
	// shutting down. Context values (trace IDs, etc.) are preserved.
	go s.checkOutboundQuota(context.WithoutCancel(ctx), hb.Apps)

	// Same goroutine pattern as checkOutboundQuota above. This is a second
	// DB round-trip per heartbeat — Phase 2 ticket could batch both writes
	// into one UPDATE.
	go s.checkRequestCount(context.WithoutCancel(ctx), hb.Apps)

	// Ingest observer metrics into the in-memory aggregator so they are
	// immediately available at the Prometheus scrape endpoints. Pure
	// in-memory — no DB round-trip, so we do it inline (cheap).
	if s.metricsAgg != nil {
		s.ingestMetrics(hb.Apps)
	}
}

// extractHeartbeatWorkerID does a best-effort partial unmarshal of raw
// heartbeat JSON to extract the worker_id for error logging when the
// full struct can't be parsed. Returns "unknown" on failure.
func extractHeartbeatWorkerID(data []byte) string {
	var partial struct {
		WorkerID string `json:"worker_id"`
	}
	if err := json.Unmarshal(data, &partial); err != nil {
		return "unknown"
	}
	return partial.WorkerID
}

// extractTenantIDFromApps extracts the tenant_id from the first app's
// status in a heartbeat's apps map. Used as a fallback when the top-level
// tenant_id is absent (pre-#297 workers). Returns "" if no tenant_id is
// found in any app.
func extractTenantIDFromApps(appsRaw json.RawMessage) string {
	var appsMap map[string]struct {
		TenantID string `json:"tenant_id"`
	}
	if err := json.Unmarshal(appsRaw, &appsMap); err != nil {
		return ""
	}
	for _, app := range appsMap {
		if app.TenantID != "" {
			return app.TenantID
		}
	}
	return ""
}

// ingestMetrics decodes the apps JSON from a heartbeat and feeds each app's
// observer_metrics (plus the built-in request_count / outbound_bytes) into
// the MetricsAggregator so they are immediately available at the Prometheus
// scrape endpoints.
func (s *WorkerService) ingestMetrics(appsRaw json.RawMessage) {
	if len(appsRaw) == 0 {
		return
	}
	var apps map[string]domain.AppStatus
	if err := json.Unmarshal(appsRaw, &apps); err != nil {
		return
	}
	for appName, app := range apps {
		if app.TenantID == "" {
			continue
		}
		s.metricsAgg.Ingest(app.TenantID, appName, app.RequestCount, app.OutboundBytes, app.ObserverMetrics)
	}
}

// applyTenantDelta sums per-tenant usage deltas from a heartbeat payload,
// writes the cumulative total to the DB, and logs a quota breach when the
// monthly cap is exceeded. Used by checkOutboundQuota and checkRequestCount
// — both pass different selector functions for the field/cap/used labels
// but share this body.
//
// Idempotency (issue #418): each `AppStatus` may carry a `DedupeID` —
// a stable token that the worker stamps at heartbeat build time. When
// present and seen recently in `s.dedupeCache`, the app's contribution
// to the per-tenant delta is dropped (JetStream redelivery or reconcile
// replay). Apps without a `DedupeID` (pre-#418 workers) always
// contribute — the legacy behaviour is preserved.
//
// Sentinel: cap <= 0 means "unlimited" (the enterprise tier stores -1 for
// all max_* columns) or "unset / admin-cleared" — either way we skip the
// breach check rather than false-trip a tenant whose cap hasn't been
// initialized.
func (s *WorkerService) applyTenantDelta(
	ctx context.Context,
	appsRaw json.RawMessage,
	field func(*domain.AppStatus) uint64,
	capField func(*domain.Quota) int64,
	usedField func(*domain.Quota) int64,
	capLabel string,
	add func(context.Context, string, uint64) (*domain.Quota, error),
) {
	if len(appsRaw) == 0 {
		return
	}
	var apps map[string]domain.AppStatus
	if err := json.Unmarshal(appsRaw, &apps); err != nil {
		log.Printf("heartbeat: could not decode apps for %s quota check: %v", capLabel, err)
		return
	}

	// Sum this heartbeat's delta per tenant. Must aggregate all apps before
	// checking — an early check on a partial per-tenant total would false-trip
	// the quota when the tenant's heaviest app appears first in map iteration.
	//
	// Per the issue #418 dedupe contract: apps whose DedupeID is already in
	// the cache are skipped (their delta is considered already applied).
	// Apps with no DedupeID (legacy workers) always contribute — backward
	// compatibility with pre-#418 workers.
	byTenant := make(map[string]uint64)
	skippedDedupe := 0
	for _, app := range apps {
		if app.TenantID == "" {
			continue
		}
		if app.DedupeID != "" && s.dedupeSeen(app.DedupeID) {
			skippedDedupe++
			continue
		}
		byTenant[app.TenantID] += field(&app)
	}
	if skippedDedupe > 0 {
		log.Printf("heartbeat: skipped %d %s app(s) on dedupe (JetStream redelivery / reconcile replay)", skippedDedupe, capLabel)
	}

	for tenantID, delta := range byTenant {
		if delta == 0 {
			// Old worker or genuinely idle — skip; don't write a no-op UPDATE.
			continue
		}
		quota, err := add(ctx, tenantID, delta)
		if err != nil {
			log.Printf("heartbeat: failed to record %s for tenant %s: %v", capLabel, tenantID, err)
			continue
		}
		if quota == nil {
			// No quota row — tenant is unlimited; nothing to enforce.
			continue
		}
		cap := capField(quota)
		if cap <= 0 {
			// Unlimited or unconfigured — nothing to enforce.
			continue
		}
		used := usedField(quota)
		if used > cap {
			log.Printf(
				"quota: tenant %s used %d %s, exceeds monthly limit %d — disabling tenant",
				tenantID, used, capLabel, cap,
			)
			// Disable the tenant atomically (issue #440) so the
			// disable→empty-task_update publish serializes against
			// any in-flight ActivateDeployment on the same tenant.
			if err := s.disableTenantAtomically(ctx, tenantID); err != nil {
				log.Printf("quota: failed to disable tenant %s: %v", tenantID, err)
			}
		}
	}
}

// dedupeSeen reports whether the given dedupe ID has been observed within
// the cache TTL. On a fresh hit, the ID is recorded so subsequent
// deliveries (JetStream redelivery, reconcile replay) skip the delta.
// Lazy eviction: an expired entry is deleted and reported as "not seen".
func (s *WorkerService) dedupeSeen(id string) bool {
	now := time.Now()
	if v, ok := s.dedupeCache.Load(id); ok {
		entry := v.(dedupeEntry)
		if now.Before(entry.expiresAt) {
			return true
		}
		// Expired — evict and treat as fresh.
		s.dedupeCache.Delete(id)
	}
	s.dedupeCache.Store(id, dedupeEntry{expiresAt: now.Add(dedupeCacheTTL)})
	return false
}

// disableTenantAtomically stamps `tenants.disabled_at` and then publishes
// an empty task_update so workers stop the tenant's apps immediately, with
// row-level serialization against any in-flight ActivateDeployment on the
// same tenant (issue #440).
//
// Sequence under the tenant-row FOR UPDATE lock:
//
//  1. BEGIN tx.
//  2. SELECT … FROM tenants WHERE id = $1 FOR UPDATE  — blocks any
//     concurrent ActivateDeployment that took the lock just before us.
//  3. UPDATE tenants SET disabled_at = NOW().
//  4. SELECT … FROM active_deployments WHERE tenant_id = $1  — snapshot
//     the set of currently-active (deployment_id, app_name) pairs while
//     the lock is held. This is the "as-of-disable" baseline.
//  5. COMMIT — the lock releases.
//  6. SELECT … FROM active_deployments WHERE tenant_id = $1  — fresh,
//     non-tx read. Compute the set diff `adsNow − baseline`. If diff is
//     non-empty, an ActivateDeployment committed in step 2→5 with its own
//     non-empty task_update on the wire; publishing empty would kill the
//     just-activated app — the very bug this commit closes — so we skip
//     the publish and log the skipped pairs.
//  7. Otherwise publish empty per region via notifyDisableTenant.
//
// Callers (the quota-exceeded branch in applyTenantDelta) should treat
// the returned error as "disable failed, leave tenant in current state"
// and not retry — the next heartbeat will re-evaluate the quota.
func (s *WorkerService) disableTenantAtomically(ctx context.Context, tenantID string) error {
	if s.db == nil {
		// Tests build a WorkerService without a DB; preserve the
		// historical two-step behaviour for those call sites by falling
		// back to the non-tx path. Production always wires `db`.
		if err := s.tenantRepo.SetDisabledAt(ctx, tenantID, time.Now()); err != nil {
			return err
		}
		s.notifyDisableTenant(ctx, tenantID)
		return nil
	}

	var baseline []domain.ActiveDeployment
	err := repository.Transaction(ctx, s.db, func(tx *sqlx.Tx) error {
		txTenant := s.tenantRepo.WithTx(tx)
		if _, err := txTenant.GetForUpdate(ctx, tenantID); err != nil {
			return fmt.Errorf("locking tenant for disable: %w", err)
		}
		if err := txTenant.SetDisabledAt(ctx, tenantID, time.Now()); err != nil {
			return fmt.Errorf("stamping disabled_at: %w", err)
		}
		// Snapshot the active-deployment set under the tenant-row lock so
		// the post-commit diff can detect a racing activate. Reading
		// inside the tx is fine; the lock prevents any other writer
		// from changing `tenants.disabled_at` until commit, and an
		// active_deployments INSERT from a racing activate would have
		// already blocked on the tenant-row lock (commit 2 acquired the
		// same lock in its tx) — so this read sees the pre-activate
		// state.
		ads, err := s.activeRepo.WithTx(tx).ListByTenant(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("snapshotting active deployments: %w", err)
		}
		baseline = ads
		return nil
	})
	if err != nil {
		return err
	}

	// Fresh read outside the lock — if a racing activate committed
	// between our GetForUpdate and this read, its row is in adsNow but
	// not in baseline. Skip the empty publish in that case: the racing
	// activate's task_update is on the wire and will be the last state
	// workers observe.
	adsNow, err := s.activeRepo.ListByTenant(ctx, tenantID)
	if err != nil {
		log.Printf("quota: post-commit active-deployments read for tenant %s: %v", tenantID, err)
		// Conservative: if we can't determine the diff, fall through to
		// publishing empty. A racing activate's non-empty task_update
		// arriving after our empty is the bug we're fixing — but we'd
		// rather publish empty (and the activate's 409-conflict guard
		// from commit 2 will keep the activate side safe in the next
		// window) than skip the publish entirely and leave workers
		// running an over-quota tenant for the 5-min reconcile cycle.
		s.notifyDisableTenant(ctx, tenantID)
		return nil
	}

	added := diffActiveDeployments(baseline, adsNow)
	if len(added) > 0 {
		log.Printf(
			"quota: tenant %s disabled but %d active row(s) committed mid-tx; skipping empty task_update (issue #440 racing-activate detected): %s",
			tenantID, len(added), formatActiveDeploymentPairs(added),
		)
		return nil
	}

	s.notifyDisableTenant(ctx, tenantID)
	return nil
}

// diffActiveDeployments returns the set of rows in `now` that were not
// present in `baseline`, keyed by (tenant_id, app_name). A row counts as
// "the same" only if both keys match; deployment_id changes on a known
// (tenant, app) are treated as a new row because the racing activate
// flipped it and we want to know.
func diffActiveDeployments(baseline, now []domain.ActiveDeployment) []domain.ActiveDeployment {
	if len(baseline) == 0 {
		return now
	}
	index := make(map[string]struct{}, len(baseline))
	for _, ad := range baseline {
		index[ad.TenantID+"\x00"+ad.AppName] = struct{}{}
	}
	var added []domain.ActiveDeployment
	for _, ad := range now {
		if _, ok := index[ad.TenantID+"\x00"+ad.AppName]; !ok {
			added = append(added, ad)
		}
	}
	return added
}

// formatActiveDeploymentPairs renders the (app_name, deployment_id) pairs
// for the racing-activate skip log line. Bounded — at most a few dozen
// apps per tenant in practice.
func formatActiveDeploymentPairs(ads []domain.ActiveDeployment) string {
	parts := make([]string, 0, len(ads))
	for _, ad := range ads {
		parts = append(parts, fmt.Sprintf("%s=%s", ad.AppName, ad.DeploymentID))
	}
	return strings.Join(parts, ",")
}

// jetstreamPublisher is the minimal NATS JetStream surface
// notifyDisableTenant needs. *natsio.JetStreamContext satisfies it via
// its embedded JetStream.Publish method (variadic PubOpt for caller
// headers / msg-headers; the disable path passes none). Defined here so
// unit tests can substitute a recording fake without spinning up a
// real JetStream context (issue #440 — the disable-side tests assert
// that an empty task_update is published per region without requiring
// nats-server).
type jetstreamPublisher interface {
	Publish(subject string, data []byte, opts ...natsio.PubOpt) (*natsio.PubAck, error)
}

// jetStreamForConn returns the *natsio.Conn's JetStream context wrapped
// as a jetstreamPublisher. Returns an error when the conn is nil so the
// caller can log and return.
func jetStreamForConn(nc *natsio.Conn) (jetstreamPublisher, error) {
	if nc == nil {
		return nil, fmt.Errorf("nats conn is nil")
	}
	return nc.JetStream()
}

// notifyDisableTenant publishes a task_update with an empty apps map to
// every region where the tenant has active deployments. This tells workers
// to stop the tenant's apps immediately (issue #155).
func (s *WorkerService) notifyDisableTenant(ctx context.Context, tenantID string) {
	if s.nc == nil && s.jsForTest == nil {
		return
	}
	ads, err := s.activeRepo.ListByTenant(ctx, tenantID)
	if err != nil {
		log.Printf("quota: failed to list active deployments for tenant %s: %v", tenantID, err)
		return
	}
	if len(ads) == 0 {
		return
	}
	// Collect unique regions from all active deployments using the
	// RegionsPublished field (the set of regions that have been
	// notified for this tenant's deployments).
	regionSet := make(map[string]struct{})
	for _, ad := range ads {
		for _, r := range ad.RegionsPublished {
			regionSet[r] = struct{}{}
		}
	}
	if len(regionSet) == 0 {
		regionSet["global"] = struct{}{}
	}
	var js jetstreamPublisher
	if s.jsForTest != nil {
		js = s.jsForTest
	} else {
		var err error
		js, err = jetStreamForConn(s.nc)
		if err != nil {
			log.Printf("quota: JetStream context for tenant %s disable notification: %v", tenantID, err)
			return
		}
	}
	for region := range regionSet {
		msg := struct {
			Type      string                 `json:"type"`
			Timestamp time.Time              `json:"timestamp"`
			TenantID  string                 `json:"tenant_id"`
			Apps      map[string]interface{} `json:"apps"`
		}{
			Type:      "task_update",
			Timestamp: time.Now(),
			TenantID:  tenantID,
			Apps:      map[string]interface{}{},
		}
		data, err := json.Marshal(msg)
		if err != nil {
			log.Printf("quota: marshal disable notification for tenant %s: %v", tenantID, err)
			continue
		}
		subject := fmt.Sprintf("edgecloud.tasks.%s", region)
		if _, err := js.Publish(subject, data); err != nil {
			log.Printf("quota: publish disable notification for tenant %s region %s: %v", tenantID, region, err)
			continue
		}
		log.Printf("quota: published empty task_update for disabled tenant %s region %s", tenantID, region)
	}
}

// workerRepoForGC is the subset of *repository.WorkerRepository needed
// by WorkerGCService. Defined locally so tests can mock it without a DB.
type workerRepoForGC interface {
	DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error)
}

// WorkerGCService periodically prunes stale worker records. Matches the
// LogGCService pattern: fires immediately on start, then ticks at interval.
type WorkerGCService struct {
	repo workerRepoForGC
}

func NewWorkerGCService(repo workerRepoForGC) *WorkerGCService {
	return &WorkerGCService{repo: repo}
}

// Run blocks until ctx is cancelled. The first sweep fires immediately.
// Workers whose last_seen is older than `maxAge` are deleted.
// If interval or maxAge is non-positive the service refuses to run.
func (s *WorkerGCService) Run(ctx context.Context, interval, maxAge time.Duration) {
	if interval <= 0 || maxAge <= 0 {
		log.Printf("worker_gc: invalid interval=%s maxAge=%s; refusing to run", interval, maxAge)
		return
	}

	runOnce := func() {
		if ctx.Err() != nil {
			return
		}
		deleted, err := s.repo.DeleteOlderThan(ctx, maxAge)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("worker_gc: delete failed (maxAge=%s): %v", maxAge, err)
			return
		}
		if deleted > 0 {
			log.Printf("worker_gc: deleted %d stale worker records older than %s", deleted, maxAge)
		}
	}

	runOnce()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runOnce()
		}
	}
}

// checkOutboundQuota accumulates outbound_bytes from this heartbeat into the
// tenant's running total in the DB (cross-worker, cross-interval). When the
// cumulative total exceeds the per-month max_outbound_mb cap, the tenant is
// disabled (issue #155) and an empty task_update is published to every
// region where the tenant has active deployments so workers stop the
// tenant's apps immediately rather than waiting for the 5-minute reconcile
// cycle. The lazy month rollover in QuotaRepository.addColumn resets the
// counter when quotas.quota_period_start is in a past UTC calendar month,
// so this function does not need to manage period boundaries directly.
func (s *WorkerService) checkOutboundQuota(ctx context.Context, appsRaw json.RawMessage) {
	s.applyTenantDelta(ctx, appsRaw,
		func(a *domain.AppStatus) uint64 { return a.OutboundBytes },
		func(q *domain.Quota) int64 { return int64(q.MaxOutboundMB) * 1024 * 1024 },
		func(q *domain.Quota) int64 { return q.UsedOutboundBytes },
		"outbound bytes",
		s.quotaRepo.AddOutboundBytes,
	)
}

// checkRequestCount accumulates request_count from this heartbeat into the
// tenant's running total in the DB (cross-worker, cross-interval). When
// the cumulative total exceeds the per-month max_requests_per_month cap,
// the tenant is disabled (issue #155) and an empty task_update is published
// to every region where the tenant has active deployments. Mirrors
// checkOutboundQuota but reads app.RequestCount instead of app.OutboundBytes.
// The lazy month rollover in QuotaRepository.addColumn handles the period
// boundary; this function does not need to track it.
func (s *WorkerService) checkRequestCount(ctx context.Context, appsRaw json.RawMessage) {
	s.applyTenantDelta(ctx, appsRaw,
		func(a *domain.AppStatus) uint64 { return a.RequestCount },
		func(q *domain.Quota) int64 { return int64(q.MaxRequestsPerMonth) },
		func(q *domain.Quota) int64 { return q.UsedRequestCount },
		"requests",
		s.quotaRepo.AddRequestCount,
	)
}

// GetAppTarget returns the running target for a single
// `(tenant_id, app_name)` pair, or `nil` when no running target is
// found. The CLI's `edge status` uses this to validate that a
// `live_url` is actually live. The query is scoped to the calling
// tenant at the SQL level so cross-tenant information is not loaded
// into Go memory.
func (s *WorkerService) GetAppTarget(ctx context.Context, tenantID, appName string) (*domain.AppTarget, error) {
	targets, err := s.workerRepo.ListRunningAppTarget(ctx, tenantID, appName)
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, nil
	}
	return &targets[0], nil
}

// GetAppStatus returns the worker-reported status for one of the
// tenant's apps. The handler calls this for GET /api/v1/apps/{appName}/status.
//
// Two cases the handler relies on:
//   - No row from the repo (no worker has reported on this app, OR a
//     cross-tenant request for an app that exists but is not the
//     caller's): return a zero-value AppWorkerStatus{AppName: appName,
//     Status: "unknown"}. The handler encodes this as 200, not 404 —
//     404 would leak the existence of the app to a probing tenant.
//   - Repo returns a row: pass it through unchanged, with AppName
//     populated from the input (the repo echoes the JSONB key but
//     we set it here so callers see exactly what they asked for).
//
// The repo handles the cross-tenant guard at the SQL level (the JSONB
// `tenant_id` filter), so a t_evil request for t_victim's app
// produces the same "unknown" path as a never-deployed app.
func (s *WorkerService) GetAppStatus(ctx context.Context, tenantID, appName string) (*domain.AppWorkerStatus, error) {
	row, err := s.workerRepo.GetAppStatus(ctx, tenantID, appName)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return &domain.AppWorkerStatus{
			AppName: appName,
			Status:  "unknown",
		}, nil
	}
	row.AppName = appName
	return row, nil
}

// evaluateStability drives the "promote running deployment to
// last_good" rule. Per heartbeat, for every app the worker reports:
//
//   - if status == "running" and stable_since IS NULL on the active
//     row, arm the clock (SetStableSince NOW).
//   - if status != "running", clear the clock (ClearStableSince) so
//     a fresh arming is needed after the next recovery. This makes
//     flapping apps never get promoted — a single unhealthy blip
//     resets the window.
//   - if status == "running" and stable_since is older than
//     s.stableWindow AND auto_rollback_enabled = true, promote the
//     currently-active deployment to last_good so a future crash
//     can roll back to it. This is the "auto-promote to last-good"
//     rule from issue #74 step 3.
//
// Promotion is metadata-only — no TaskMessage is published because
// the worker is already serving the same deployment_id. The only
// effect is that `last_good_deployment_id` flips to the new active
// id, ready to be swapped in by the next rollback or auto-rollback.
//
// Resolution is heartbeat-bound: with the default 30s window and
// 30s heartbeat interval, the worst-case latency from "first
// running observation" to "promoted" is ~60s (next heartbeat +
// window). Tenants needing faster promotion can lower both.
//
// On any per-app error, log and continue — the stability check is
// best-effort. A DB hiccup on one app must not block the heartbeat
// path for other apps on the same worker.
func (s *WorkerService) evaluateStability(ctx context.Context, tenantID string, appsJSON json.RawMessage) {
	var apps map[string]struct {
		Status       string `json:"status"`
		DeploymentID string `json:"deployment_id"`
	}
	if err := json.Unmarshal(appsJSON, &apps); err != nil {
		log.Printf("stability: failed to parse apps JSON: %v", err)
		return
	}

	now := time.Now()
	for rawKey, app := range apps {
		// Heartbeat key is "app_name:deployment_id" for canary/blue-green
		// deployments (see edge-worker/src/supervisor.rs build_heartbeat).
		// `active_deployments.app_name` is keyed on the bare app_name, so we
		// strip any ":deployment_id" suffix before the lookup — otherwise
		// activeRepo.Get never matches and the auto-rollback stability
		// window never arms or fires (silent regression of #74).
		appName := rawKey
		if i := strings.IndexByte(rawKey, ':'); i >= 0 {
			appName = rawKey[:i]
		}
		ad, err := s.activeRepo.Get(ctx, tenantID, appName)
		if err != nil {
			log.Printf("stability: Get(%s, %s) failed: %v", tenantID, appName, err)
			continue
		}
		if ad == nil {
			// Heartbeat mentions an app that has no active row.
			// Could be a fresh deploy that hasn't been activated
			// yet; the activate path handles arming the clock,
			// nothing to do here.
			continue
		}
		// The deployment_id reported by the heartbeat is the
		// ground truth — if it doesn't match the active row's
		// deployment_id, an Activate landed between heartbeats
		// and the active row is ahead of the worker. The next
		// heartbeat (after the worker reconciles) will match;
		// in the meantime, skip stability work for this app.
		if ad.DeploymentID != app.DeploymentID {
			continue
		}

		switch app.Status {
		case "running":
			if ad.StableSince == nil {
				// First observation of "running" — arm the clock.
				if err := s.activeRepo.SetStableSince(ctx, tenantID, appName, app.DeploymentID, now); err != nil {
					log.Printf("stability: SetStableSince(%s, %s) failed: %v", tenantID, appName, err)
				}
				continue
			}
			// Already armed. Has it been long enough?
			if now.Sub(*ad.StableSince) < s.stableWindow {
				continue
			}
			// Stable long enough. Promote, but only if the
			// tenant opted in. (Without the auto-rollback flag
			// there's no automatic consumer of last_good — the
			// tenant would have to roll back manually, and
			// flipping last_good without auto-rollback could
			// surprise a manual rollback by overwriting the
			// pointer with a deployment the user hasn't
			// validated.)
			if !ad.AutoRollbackEnabled {
				continue
			}
			// Promote the currently-active deployment to
			// last_good. PromoteToLastGood's WHERE clause
			// (last_good_deployment_id IS NULL) ensures a
			// concurrent manual rollback isn't clobbered —
			// the call is a no-op when last_good already
			// points somewhere (e.g. a freshly-activated v2
			// trying to "promote itself" while v1 is still
			// the previous-good).
			if err := s.activeRepo.PromoteToLastGood(ctx, tenantID, appName, app.DeploymentID); err != nil {
				log.Printf("stability: PromoteToLastGood(%s, %s) failed: %v", tenantID, appName, err)
			}
		default:
			// Any non-running status (starting, stopping,
			// crashed, hung) resets the clock so the next
			// "running" observation has to start the window
			// from scratch. We don't bail out — there's no
			// auto-rollback promotion on this branch — but we
			// do ClearStableSince so a flapping app doesn't
			// accumulate partial-window credit across flaps.
			if ad.StableSince != nil {
				if err := s.activeRepo.ClearStableSince(ctx, tenantID, appName); err != nil {
					log.Printf("stability: ClearStableSince(%s, %s) failed: %v", tenantID, appName, err)
				}
			}
		}
	}
}
