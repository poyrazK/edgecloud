package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// ErrTenantNotFoundInReconcile is returned by BuildFullSync when the caller
// (e.g. the /sync HTTP fallback handler) asks for a tenantID that has
// no row in the tenants table. A worker that references a deleted
// tenant is the only realistic production trigger — the handler maps
// it to 500 so operators see the inconsistent state in their error
// dashboards instead of silently receiving an empty payload.
//
// This sentinel was previously named ErrTenantNotFound; it was renamed
// in billing v0 so that the canonical "tenant row missing" sentinel
// could live in tenant.go as a 404-mapped error without ambiguity.
var ErrTenantNotFoundInReconcile = errors.New("tenant not found")

// reconcileTenants is the subset of *repository.TenantRepository the
// ReconcileService needs. List fans out across all tenants for the
// periodic sweep; GetByID fetches one row by ID for the on-demand
// RequestSync and the HTTP-fallback BuildFullSync paths. Without
// GetByID, both callers would have to walk the full List and match
// in memory — and the previous "len(tenant) == 0" guard didn't
// actually detect "not found" because tenant is a slice that can be
// empty for unrelated reasons (e.g. a fresh DB, or pagination later).
type reconcileTenants interface {
	ListActive(ctx context.Context) ([]domain.Tenant, error)
	GetByID(ctx context.Context, id string) (*domain.Tenant, error)
}

// reconcileActiveDeployments is the subset of
// *repository.ActiveDeploymentRepository the ReconcileService needs.
// ListByTenantWithDeployment joins active_deployments with deployments
// (hash + regions) in one round trip — see JoinedActiveDeployment.
// The un-joined ListByTenant is kept for symmetry with the production
// repo but is no longer called by ReconcileService (PR #166
// follow-up #1 eliminated the N+1 in reconcileTenant / BuildFullSync).
type reconcileActiveDeployments interface {
	ListByTenant(ctx context.Context, tenantID string) ([]domain.ActiveDeployment, error)
	ListByTenantWithDeployment(ctx context.Context, tenantID string) ([]repository.JoinedActiveDeployment, error)
}

// reconcileAppEnvs is the subset of *repository.AppEnvRepository the
// ReconcileService needs. ListByApps fetches env vars for multiple
// apps in one query, replacing the previous per-app List loop.
type reconcileAppEnvs interface {
	ListByApps(ctx context.Context, tenantID string, appNames []string) ([]domain.AppEnv, error)
}

// reconcileQuotas is the subset of *repository.QuotaRepository the
// ReconcileService needs to populate AppConfig.MaxMemoryMB.
type reconcileQuotas interface {
	GetByTenantID(ctx context.Context, tenantID string) (*domain.Quota, error)
}

// reconcileWorkers is the subset of *repository.WorkerRepository the
// ReconcileService needs for under-replication monitoring (issue #316).
type reconcileWorkers interface {
	CountRunningWorkers(ctx context.Context, tenantID string, appNames []string) (map[string]int, error)
}

// ReconcileService periodically publishes a TaskMessage::FullSync per
// (tenant, region) so workers can recover from lost or stale NATS
// task_update messages. Idempotent: the worker's diff logic treats
// identical AppConfig payloads as no-ops.
//
// The periodic cadence is the safety-net window for "message lost in
// NATS stream / consumer crashed mid-diff / max_age / max_deliver
// exceeded". The default interval (5 min) is the issue #53
// recommendation; operators tune via RECONCILE_INTERVAL.
//
// Three callers:
//
//   - Run: periodic timer (background goroutine started from
//     cmd/api/main.go).
//   - RequestSync: on-demand entry from the RegisterWorker hook
//     (commit 3) — publishes one full_sync for one (tenant, region)
//     immediately, does not block on the timer.
//   - BuildFullSync: pure compute (no publish) consumed by the HTTP
//     fallback endpoint (commit 4) so the GET /sync handler can return
//     the same payload the periodic loop would publish.
type ReconcileService struct {
	tenantRepo    reconcileTenants
	activeRepo    reconcileActiveDeployments
	appEnvRepo    reconcileAppEnvs
	quotaRepo     reconcileQuotas
	workerRepo    reconcileWorkers
	publisher     nats.Publisher
	defaultRegion string
	envDecrypter  TrafficEnvDecrypter // nil = plaintext pass-through
}

func NewReconcileService(
	tenantRepo reconcileTenants,
	activeRepo reconcileActiveDeployments,
	appEnvRepo reconcileAppEnvs,
	quotaRepo reconcileQuotas,
	workerRepo reconcileWorkers,
	publisher nats.Publisher,
	defaultRegion string,
) *ReconcileService {
	if defaultRegion == "" {
		defaultRegion = "global"
	}
	return &ReconcileService{
		tenantRepo:    tenantRepo,
		activeRepo:    activeRepo,
		appEnvRepo:    appEnvRepo,
		quotaRepo:     quotaRepo,
		workerRepo:    workerRepo,
		publisher:     publisher,
		defaultRegion: defaultRegion,
	}
}

// SetEnvDecrypter injects the decrypter for env values at publish.
func (s *ReconcileService) SetEnvDecrypter(dec TrafficEnvDecrypter) {
	s.envDecrypter = dec
}

// Run blocks until ctx is cancelled. The first sweep fires immediately
// (operationally useful — when the process restarts we don't want to
// wait `interval` before catching up workers that missed messages);
// subsequent sweeps tick at `interval`.
//
// Mirrors LogGCService.Run: invalid (non-positive) intervals are
// refused rather than allowed to busy-loop.
func (s *ReconcileService) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		log.Printf("reconcile: invalid interval=%s; refusing to run", interval)
		return
	}

	runOnce := func() {
		if ctx.Err() != nil {
			return
		}
		if err := s.RunOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return // shutting down — expected
			}
			log.Printf("reconcile: sweep failed: %v", err)
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

// RunOnce publishes one full_sync per (tenant, region). Returns an
// error so Run can log it; per-tenant/per-region failures inside are
// logged-and-continued so a single bad tenant doesn't take the whole
// sweep down.
func (s *ReconcileService) RunOnce(ctx context.Context) error {
	tenants, err := s.tenantRepo.ListActive(ctx)
	if err != nil {
		return err
	}
	for _, t := range tenants {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.reconcileTenant(ctx, t.ID, t.AllowlistedDestinations, "")
	}
	return nil
}

// RequestSync is the on-demand entry point used by the RegisterWorker
// hook (commit 3). Publishes one full_sync for one (tenant, region)
// immediately — does not block on the periodic timer.
//
// Empty `region` means "fan out to every region this tenant has
// active deployments in" (i.e. behave like the periodic sweep but
// for one tenant). Non-empty `region` means "publish only to that
// one region" — the on-register path uses this to scope the publish
// to the freshly-registered worker's region.
func (s *ReconcileService) RequestSync(ctx context.Context, tenantID, region string) {
	tenant, err := s.tenantRepo.GetByID(ctx, tenantID)
	if err != nil {
		log.Printf("reconcile: RequestSync(%s,%s): get tenant: %v", tenantID, region, err)
		return
	}
	if tenant == nil {
		// The previously-implemented `len(tenant) == 0` check never
		// fired — `tenant` was the whole List result, so an empty DB
		// (or a deployment row written before tenant creation) would
		// silently fall through with `allowlist = nil`. That stripped
		// egress rules for a tenant whose allowlist we couldn't load
		// — the wrong direction for a security boundary. Now we
		// explicitly fail-closed.
		log.Printf("reconcile: RequestSync(%s,%s): tenant not found", tenantID, region)
		return
	}
	s.reconcileTenant(ctx, tenantID, tenant.AllowlistedDestinations, region)
}

// reconcileTenant is the shared core: read active_deployments for one
// tenant, enrich each app's AppConfig, group by region, and publish
// one full_sync per region.
//
// When `regionFilter` is non-empty, only that region is published
// (RegisterWorker hook); when empty, every region is published
// (periodic sweep + on-register with no scoped region).
//
// Query budget per tenant per sweep: 3 round trips (joined active +
// deployment, bulk envs, quota). The previous implementation was
// 2M+2 — one active list + one quota + M deployment lookups + M env
// lists. See PR #166 follow-up #1.
func (s *ReconcileService) reconcileTenant(ctx context.Context, tenantID string, allowlist []string, regionFilter string) {
	joined, err := s.activeRepo.ListByTenantWithDeployment(ctx, tenantID)
	if err != nil {
		log.Printf("reconcile: tenant=%s: list active+deployment: %v", tenantID, err)
		return
	}
	if len(joined) == 0 {
		return
	}

	// Surface broken (active, missing-deployment) pairs to the
	// operator. ListByTenantWithDeployment uses LEFT JOIN semantics
	// so orphan rows survive the join with Hash="" / Regions=nil;
	// we skip them here and log the count so the broken state is
	// visible (the pre-N+1 reconcile loop did the same on a per-row
	// path). Subsequent sweeps will keep reporting until the
	// operator fixes the underlying row.
	publishable := joined[:0]
	orphanCount := 0
	for _, j := range joined {
		if !j.Hash.Valid {
			orphanCount++
			log.Printf("reconcile: tenant=%s app=%s has active row referencing missing deployment %q; skipping publish",
				tenantID, j.AppName, j.DeploymentID)
			continue
		}
		publishable = append(publishable, j)
	}
	if orphanCount > 0 {
		log.Printf("reconcile: tenant=%s: %d active row(s) reference a missing deployment; operator action required to re-activate or delete",
			tenantID, orphanCount)
	}
	if len(publishable) == 0 {
		return
	}

	// Issue #316: under-replication check. Query how many distinct workers
	// report each app as running; warn if below desired_replicas.
	appNames := make([]string, len(publishable))
	for i, j := range publishable {
		appNames[i] = j.AppName
	}
	runningCounts, err := s.workerRepo.CountRunningWorkers(ctx, tenantID, appNames)
	if err != nil {
		log.Printf("reconcile: tenant=%s: CountRunningWorkers: %v; skipping under-replication check", tenantID, err)
	} else {
		for _, j := range publishable {
			if j.DesiredReplicas > 0 {
				count := runningCounts[j.AppName]
				if count < j.DesiredReplicas {
					log.Printf("reconcile: tenant=%s app=%s region=%s: under-replicated: have=%d want=%d",
						tenantID, j.AppName, s.defaultRegion, count, j.DesiredReplicas)
				}
			}
		}
	}

	// Bulk-fetch env vars for every publishable active app in one round trip.
	allEnvs, err := s.appEnvRepo.ListByApps(ctx, tenantID, appNames)
	if err != nil {
		log.Printf("reconcile: tenant=%s: list envs (bulk): %v; publishing without env", tenantID, err)
		allEnvs = nil
	}
	envByApp := make(map[string]map[string]string, len(publishable))
	for _, e := range allEnvs {
		if envByApp[e.AppName] == nil {
			envByApp[e.AppName] = make(map[string]string)
		}
		v := e.EnvValue
		if s.envDecrypter != nil {
			// Issue #441: fail closed. A plaintext or tampered env row
			// aborts this tenant's reconcile sweep (log-and-return)
			// — operators see the error in CP logs and re-encrypt the
			// offending row before the next tick. We don't try to
			// "continue past the bad row" because that would publish
			// the rest of the app's env plaintext, which is exactly
			// the bug being fixed.
			d, err := s.envDecrypter.Decrypt(e.EnvValue)
			if err != nil {
				log.Printf("reconcile: tenant=%s %s/%s: decrypt failed: %v (aborting tenant reconcile; re-encrypt via POST /api/v1/admin/secrets/re-encrypt)", tenantID, e.AppName, e.EnvKey, err)
				return
			}
			v = d
		}
		envByApp[e.AppName][e.EnvKey] = v
	}

	maxMemoryMB := 256
	if q, err := s.quotaRepo.GetByTenantID(ctx, tenantID); err == nil && q != nil && q.MaxMemoryMB > 0 {
		maxMemoryMB = q.MaxMemoryMB
	}

	// region -> { app_name: AppConfig }
	byRegion := map[string]map[string]nats.AppConfig{}
	for _, j := range publishable {
		envMap := envByApp[j.AppName] // nil if the bulk fetch errored
		if envMap == nil {
			envMap = map[string]string{}
		}

		cfg := nats.BuildAppConfig(
			j.DeploymentID,
			// j.Hash was confirmed Valid by the orphan filter above;
			// Hash.String is the non-NULL hash from the joined deployments row.
			j.Hash.String,
			j.Signature.String,    // issue #307: signature is empty for legacy rows; worker honors EDGE_REQUIRE_SIGNATURE
			j.SigningKeyID.String, // issue #307 PR1: per-key kid
			"",                    // issue #308: preview_id is empty on the reconcile path (full_sync never carries preview metadata)
			0,                     // issue #308: preview_pr_number — same reasoning as preview_id
			envMap,
			allowlist,
			maxMemoryMB,
		)

		regions := []string(j.Regions)
		if len(regions) == 0 {
			regions = []string{s.defaultRegion}
		}
		for _, region := range regions {
			if regionFilter != "" && region != regionFilter {
				continue
			}
			if byRegion[region] == nil {
				byRegion[region] = map[string]nats.AppConfig{}
			}
			byRegion[region][j.AppName] = cfg
		}
	}

	for region, apps := range byRegion {
		if err := s.publisher.PublishFullSync(region, &nats.TaskMessage{
			Type:      "full_sync",
			Timestamp: time.Now().UTC(),
			TenantID:  tenantID,
			Apps:      apps,
		}); err != nil {
			log.Printf("reconcile: tenant=%s region=%s: publish full_sync: %v", tenantID, region, err)
		}
	}
}

// BuildFullSync computes the same per-region AppConfig map that
// reconcileTenant publishes, but returns it to the caller instead of
// publishing. Used by the HTTP fallback endpoint (commit 4) so GET
// /api/internal/workers/{workerID}/sync can return the exact payload
// the worker would receive via NATS.
//
// `region` is the worker's region — only that region's app set is
// returned. An empty region falls back to the control plane's default
// region (matches reconcileTenant's behavior for legacy deployments
// with no explicit regions).
//
// Query budget: 3 round trips (tenant + joined active+deployment +
// bulk envs); the quota lookup is folded into the envs fetch path
// when MaxMemoryMB is non-zero. See PR #166 follow-up #1.
func (s *ReconcileService) BuildFullSync(ctx context.Context, tenantID, region string) (map[string]nats.AppConfig, error) {
	if region == "" {
		region = s.defaultRegion
	}

	// Resolve tenant allowlist up front. A missing tenant row is an
	// inconsistent-state error (worker registered but tenant deleted):
	// return ErrTenantNotFoundInReconcile so the HTTP handler can surface a 500
	// with a useful log line, instead of silently returning a payload
	// stripped of egress rules.
	tenant, err := s.tenantRepo.GetByID(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	if tenant == nil {
		return nil, ErrTenantNotFoundInReconcile
	}
	allowlist := tenant.AllowlistedDestinations

	joined, err := s.activeRepo.ListByTenantWithDeployment(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	if len(joined) == 0 {
		return map[string]nats.AppConfig{}, nil
	}

	// Drop broken (active, missing-deployment) rows. LEFT JOIN brings
	// them through with Hash="" — they have no deployment to publish,
	// and the operator must fix the underlying state (re-activate or
	// delete the active row). The reconcileTenant periodic loop logs
	// these for visibility; here we just skip them, since /sync is a
	// worker-facing read endpoint and shouldn't pollute the worker's
	// view with 500s for an operator-actionable state.
	publishable := joined[:0]
	orphanCount := 0
	for _, j := range joined {
		if !j.Hash.Valid {
			orphanCount++
			continue
		}
		publishable = append(publishable, j)
	}
	if orphanCount > 0 {
		log.Printf("reconcile: BuildFullSync tenant=%s region=%s: skipped %d active row(s) referencing missing deployment",
			tenantID, region, orphanCount)
	}
	if len(publishable) == 0 {
		return map[string]nats.AppConfig{}, nil
	}

	// Bulk-fetch env vars for every active app in one round trip.
	appNames := make([]string, 0, len(publishable))
	for _, j := range publishable {
		regions := []string(j.Regions)
		if len(regions) == 0 {
			regions = []string{s.defaultRegion}
		}
		matched := false
		for _, r := range regions {
			if r == region {
				matched = true
				break
			}
		}
		if matched {
			appNames = append(appNames, j.AppName)
		}
	}
	if len(appNames) == 0 {
		return map[string]nats.AppConfig{}, nil
	}
	allEnvs, err := s.appEnvRepo.ListByApps(ctx, tenantID, appNames)
	if err != nil {
		// Mirror reconcileTenant: log-and-continue with empty env
		// rather than failing the whole request. A transient env
		// fetch error shouldn't 500 the /sync fallback that
		// exists specifically to recover from outages.
		log.Printf("reconcile: BuildFullSync tenant=%s: list envs (bulk): %v", tenantID, err)
		allEnvs = nil
	}
	envByApp := make(map[string]map[string]string, len(appNames))
	for _, e := range allEnvs {
		if envByApp[e.AppName] == nil {
			envByApp[e.AppName] = make(map[string]string)
		}
		v := e.EnvValue
		if s.envDecrypter != nil {
			// Issue #441: fail closed. Same shape as reconcileTenant.
			d, err := s.envDecrypter.Decrypt(e.EnvValue)
			if err != nil {
				return nil, fmt.Errorf("decrypting env %s/%s: %w", e.AppName, e.EnvKey, err)
			}
			v = d
		}
		envByApp[e.AppName][e.EnvKey] = v
	}

	maxMemoryMB := 256
	if q, err := s.quotaRepo.GetByTenantID(ctx, tenantID); err == nil && q != nil && q.MaxMemoryMB > 0 {
		maxMemoryMB = q.MaxMemoryMB
	}

	out := map[string]nats.AppConfig{}
	for _, j := range publishable {
		regions := []string(j.Regions)
		if len(regions) == 0 {
			regions = []string{s.defaultRegion}
		}
		matched := false
		for _, r := range regions {
			if r == region {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}

		envMap := envByApp[j.AppName]
		if envMap == nil {
			envMap = map[string]string{}
		}

		out[j.AppName] = nats.BuildAppConfig(
			j.DeploymentID,
			// Same Valid-guaranteed pattern as reconcileTenant: the
			// orphan filter at the top of BuildFullSync excludes rows
			// where Hash is SQL NULL, so by construction Hash.String
			// is the populated hash.
			j.Hash.String,
			j.Signature.String,    // issue #307: empty for legacy rows
			j.SigningKeyID.String, // issue #307 PR1: per-key kid
			"",                    // issue #308: preview_id is empty on the reconcile path (full_sync never carries preview metadata)
			0,                     // issue #308: preview_pr_number — same reasoning as preview_id
			envMap,
			allowlist,
			maxMemoryMB,
		)
	}
	return out, nil
}
