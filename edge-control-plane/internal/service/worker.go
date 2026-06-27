package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	nats "github.com/nats-io/nats.go"
)

// Sentinel errors for WorkerService Register.
var (
	ErrInvalidWorkerID = errors.New("invalid worker_id format: must be w_<region>_<uuid>")
	ErrRegionMismatch  = errors.New("region mismatch between worker_id and request region")
	ErrQuotaExceeded   = errors.New("max workers reached for tenant")
)

// workerRepoInterface defines the repository methods used by WorkerService.
type workerRepoInterface interface {
	Upsert(ctx context.Context, tenantID string, req *domain.RegisterWorkerRequest) (bool, error)
	CountByTenant(ctx context.Context, tenantID string) (int, error)
	Delete(ctx context.Context, id string) error
	ListByTenant(ctx context.Context, tenantID string) ([]domain.Worker, error)
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
}

// defaultStableWindowSeconds is the default for `STABLE_WINDOW_SECONDS`
// when the env var is unset or unparseable. Set to 30 to match the
// worker's heartbeat_interval_secs default — promotion can fire on the
// second heartbeat after activation. Tunable downward for tenants who
// want faster promotion; tunable upward for noisy apps where a single
// successful heartbeat isn't enough signal.
const defaultStableWindowSeconds = 30

// WorkerService handles worker lifecycle business logic.
type WorkerService struct {
	workerRepo   workerRepoInterface
	quotaRepo    quotaRepoInterface
	activeRepo   activeRepoInterface
	nc           *nats.Conn
	stableWindow time.Duration
	metricsAgg   *MetricsAggregator
}

// NewWorkerService creates a new WorkerService.
//
// `stableWindow` is the minimum time a deployment must be observed
// running before it becomes eligible for promotion to
// last_good_deployment_id (the safety-net pointer). Pass 0 to use
// the default (30s, configurable via STABLE_WINDOW_SECONDS env). The
// CLI accepts any non-negative integer; sub-second precision is not
// supported.
func NewWorkerService(workerRepo *repository.WorkerRepository, quotaRepo *repository.QuotaRepository, activeRepo *repository.ActiveDeploymentRepository, nc *nats.Conn, stableWindow time.Duration, metricsAgg *MetricsAggregator) *WorkerService {
	if stableWindow <= 0 {
		stableWindow = time.Duration(defaultStableWindowSeconds) * time.Second
	}
	return &WorkerService{
		workerRepo:   workerRepo,
		quotaRepo:    quotaRepo,
		activeRepo:   activeRepo,
		nc:           nc,
		stableWindow: stableWindow,
		metricsAgg:   metricsAgg,
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

// SubscribeHeartbeats starts a background NATS subscription to edgecloud.heartbeats.*
// and upserts worker status on each message.
func (s *WorkerService) SubscribeHeartbeats(ctx context.Context) error {
	if s.nc == nil {
		// No NATS connection — skip subscription (e.g., in tests)
		return nil
	}
	ch := make(chan *nats.Msg, 100)
	sub, err := s.nc.ChanSubscribe("edgecloud.heartbeats.>", ch)
	if err != nil {
		return fmt.Errorf("subscribing to heartbeats: %w", err)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				if err := sub.Unsubscribe(); err != nil {
					log.Printf("SubscribeHeartbeats: failed to unsubscribe: %v", err)
				}
				close(ch)
				return
			case msg := <-ch:
				s.handleHeartbeat(ctx, msg)
			}
		}
	}()
	return nil
}

func (s *WorkerService) handleHeartbeat(ctx context.Context, msg *nats.Msg) {
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
	}
	if err := json.Unmarshal(msg.Data, &hb); err != nil {
		return
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
		log.Printf("heartbeat: failed to upsert status for %s: %v", hb.WorkerID, err)
	}

	// Stability-window evaluate. Only fires when the heartbeat
	// carries a tenant_id and apps — the AppSpec on the wire is the
	// source of truth for "which apps is this worker serving for
	// this tenant right now?". Heartbeats without apps are treated
	// as the worker's "I'm idle" signal — nothing to evaluate.
	if hb.TenantID != "" && len(hb.Apps) > 0 {
		s.evaluateStability(ctx, hb.TenantID, hb.Apps)
	}

	// Decode app statuses to sum outbound bytes and enforce the per-tenant
	// max_outbound_mb quota. Old workers omit outbound_bytes (defaults to 0),
	// which we treat as "no data" — we log but do not act on a 0-byte total
	// so a single old worker cannot cause a false quota violation.
	// Dispatched in a separate goroutine so DB round-trips in quota enforcement
	// do not block the NATS heartbeat drain goroutine (which has a fixed 100-msg
	// buffer and will drop overflow messages if the drain stalls).
	//
	// context.WithoutCancel strips the subscriber's cancellation signal so that
	// a graceful shutdown (which cancels ctx) does not abort in-flight quota
	// writes — byte deltas must reach the DB even when the subscriber is
	// shutting down. Context values (trace IDs, etc.) are preserved.
	go s.checkOutboundQuota(context.WithoutCancel(ctx), hb.Apps)

	// Ingest observer metrics into the in-memory aggregator so they are
	// immediately available at the Prometheus scrape endpoints. Pure
	// in-memory — no DB round-trip, so we do it inline (cheap).
	if s.metricsAgg != nil {
		s.ingestMetrics(hb.Apps)
	}
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

// checkOutboundQuota accumulates outbound_bytes from this heartbeat into the
// tenant's running total in the DB (cross-worker, cross-interval), then logs a
// violation when the cumulative total exceeds the per-month max_outbound_mb cap.
// Phase 1: log-only. Phase 2 (tracked in issue #120 follow-up): evict apps.
func (s *WorkerService) checkOutboundQuota(ctx context.Context, appsRaw json.RawMessage) {
	if len(appsRaw) == 0 {
		return
	}
	var apps map[string]domain.AppStatus
	if err := json.Unmarshal(appsRaw, &apps); err != nil {
		log.Printf("heartbeat: could not decode apps for quota check: %v", err)
		return
	}

	// Sum this heartbeat's delta per tenant. Must aggregate all apps before
	// checking — an early check on a partial per-tenant total would false-trip
	// the quota when the tenant's heaviest app appears first in map iteration.
	byTenant := make(map[string]uint64)
	for _, app := range apps {
		if app.TenantID != "" {
			byTenant[app.TenantID] += app.OutboundBytes
		}
	}

	for tenantID, deltaBytes := range byTenant {
		if deltaBytes == 0 {
			// Old worker or genuinely idle — skip; don't write a no-op UPDATE.
			continue
		}
		// Persist the delta and get back the updated cumulative total in one
		// round-trip. This aggregates across all workers and all intervals.
		quota, err := s.quotaRepo.AddOutboundBytes(ctx, tenantID, deltaBytes)
		if err != nil {
			log.Printf("heartbeat: failed to record outbound bytes for tenant %s: %v", tenantID, err)
			continue
		}
		if quota == nil {
			// No quota row — tenant is unlimited; nothing to enforce.
			continue
		}
		if quota.MaxOutboundMB <= 0 {
			// Unlimited or unconfigured — nothing to enforce.
			continue
		}
		limitBytes := int64(quota.MaxOutboundMB) * 1024 * 1024
		if quota.UsedOutboundBytes > limitBytes {
			log.Printf(
				"quota: tenant %s used %d outbound bytes, exceeds monthly limit %d (%d MB) — enforcement pending",
				tenantID, quota.UsedOutboundBytes, limitBytes, quota.MaxOutboundMB,
			)
		}
	}
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
