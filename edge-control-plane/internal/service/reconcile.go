package service

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
)

// ErrTenantNotFound is returned by BuildFullSync when the caller
// (e.g. the /sync HTTP fallback handler) asks for a tenantID that has
// no row in the tenants table. A worker that references a deleted
// tenant is the only realistic production trigger — the handler maps
// it to 500 so operators see the inconsistent state in their error
// dashboards instead of silently receiving an empty payload.
var ErrTenantNotFound = errors.New("tenant not found")

// reconcileTenants is the subset of *repository.TenantRepository the
// ReconcileService needs. List fans out across all tenants for the
// periodic sweep; GetByID fetches one row by ID for the on-demand
// RequestSync and the HTTP-fallback BuildFullSync paths. Without
// GetByID, both callers would have to walk the full List and match
// in memory — and the previous "len(tenant) == 0" guard didn't
// actually detect "not found" because tenant is a slice that can be
// empty for unrelated reasons (e.g. a fresh DB, or pagination later).
type reconcileTenants interface {
	List(ctx context.Context) ([]domain.Tenant, error)
	GetByID(ctx context.Context, id string) (*domain.Tenant, error)
}

// reconcileActiveDeployments is the subset of
// *repository.ActiveDeploymentRepository the ReconcileService needs.
type reconcileActiveDeployments interface {
	ListByTenant(ctx context.Context, tenantID string) ([]domain.ActiveDeployment, error)
}

// reconcileDeployments is the subset of *repository.DeploymentRepository
// the ReconcileService needs to enrich an active row with its deployment
// hash and target regions.
type reconcileDeployments interface {
	GetByID(ctx context.Context, id string) (*domain.Deployment, error)
}

// reconcileAppEnvs is the subset of *repository.AppEnvRepository the
// ReconcileService needs to attach per-app env vars to the published
// TaskMessage.
type reconcileAppEnvs interface {
	List(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error)
}

// reconcileQuotas is the subset of *repository.QuotaRepository the
// ReconcileService needs to populate AppConfig.MaxMemoryMB.
type reconcileQuotas interface {
	GetByTenantID(ctx context.Context, tenantID string) (*domain.Quota, error)
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
	tenantRepo     reconcileTenants
	activeRepo     reconcileActiveDeployments
	deploymentRepo reconcileDeployments
	appEnvRepo     reconcileAppEnvs
	quotaRepo      reconcileQuotas
	publisher      nats.Publisher
	defaultRegion  string
}

func NewReconcileService(
	tenantRepo reconcileTenants,
	activeRepo reconcileActiveDeployments,
	deploymentRepo reconcileDeployments,
	appEnvRepo reconcileAppEnvs,
	quotaRepo reconcileQuotas,
	publisher nats.Publisher,
	defaultRegion string,
) *ReconcileService {
	if defaultRegion == "" {
		defaultRegion = "global"
	}
	return &ReconcileService{
		tenantRepo:     tenantRepo,
		activeRepo:     activeRepo,
		deploymentRepo: deploymentRepo,
		appEnvRepo:     appEnvRepo,
		quotaRepo:      quotaRepo,
		publisher:      publisher,
		defaultRegion:  defaultRegion,
	}
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
	tenants, err := s.tenantRepo.List(ctx)
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
// Per-app enrichment duplicates the per-row work in
// DeploymentService.publishSwap / RepublishActiveDeployments. A future
// refactor should extract a shared `buildAppConfig` helper so all
// three callers (Activate, Republish, Reconcile) stay in sync —
// filed as a follow-up because extracting it touches the hot path.
func (s *ReconcileService) reconcileTenant(ctx context.Context, tenantID string, allowlist []string, regionFilter string) {
	activeList, err := s.activeRepo.ListByTenant(ctx, tenantID)
	if err != nil {
		log.Printf("reconcile: tenant=%s: list active: %v", tenantID, err)
		return
	}
	if len(activeList) == 0 {
		return
	}

	maxMemoryMB := 256
	if q, err := s.quotaRepo.GetByTenantID(ctx, tenantID); err == nil && q != nil && q.MaxMemoryMB > 0 {
		maxMemoryMB = q.MaxMemoryMB
	}

	// region -> { app_name: AppConfig }
	byRegion := map[string]map[string]nats.AppConfig{}
	for _, ad := range activeList {
		deployment, err := s.deploymentRepo.GetByID(ctx, ad.DeploymentID)
		if err != nil || deployment == nil {
			log.Printf("reconcile: tenant=%s app=%s: deployment %s not found; skipping", tenantID, ad.AppName, ad.DeploymentID)
			continue
		}

		envs, err := s.appEnvRepo.List(ctx, tenantID, ad.AppName)
		if err != nil {
			log.Printf("reconcile: tenant=%s app=%s: list env: %v; publishing without env", tenantID, ad.AppName, err)
		}
		envMap := make(map[string]string, len(envs))
		for _, e := range envs {
			envMap[e.EnvKey] = e.EnvValue
		}

		cfg := nats.AppConfig{
			DeploymentID:   ad.DeploymentID,
			DeploymentHash: deployment.Hash,
			Env:            envMap,
			Allowlist:      allowlist,
			MaxMemoryMB:    maxMemoryMB,
		}

		regions := deployment.Regions
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
			byRegion[region][ad.AppName] = cfg
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
func (s *ReconcileService) BuildFullSync(ctx context.Context, tenantID, region string) (map[string]nats.AppConfig, error) {
	if region == "" {
		region = s.defaultRegion
	}

	// Resolve tenant allowlist up front. A missing tenant row is an
	// inconsistent-state error (worker registered but tenant deleted):
	// return ErrTenantNotFound so the HTTP handler can surface a 500
	// with a useful log line, instead of silently returning a payload
	// stripped of egress rules.
	tenant, err := s.tenantRepo.GetByID(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	if tenant == nil {
		return nil, ErrTenantNotFound
	}
	allowlist := tenant.AllowlistedDestinations

	activeList, err := s.activeRepo.ListByTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	if len(activeList) == 0 {
		return map[string]nats.AppConfig{}, nil
	}

	maxMemoryMB := 256
	if q, err := s.quotaRepo.GetByTenantID(ctx, tenantID); err == nil && q != nil && q.MaxMemoryMB > 0 {
		maxMemoryMB = q.MaxMemoryMB
	}

	out := map[string]nats.AppConfig{}
	for _, ad := range activeList {
		deployment, err := s.deploymentRepo.GetByID(ctx, ad.DeploymentID)
		if err != nil || deployment == nil {
			continue
		}

		regions := deployment.Regions
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

		envs, err := s.appEnvRepo.List(ctx, tenantID, ad.AppName)
		if err != nil {
			// Mirror reconcileTenant: log-and-continue with empty env
			// rather than failing the whole request. A transient env
			// fetch error shouldn't 500 the /sync fallback that
			// exists specifically to recover from outages.
			log.Printf("reconcile: BuildFullSync tenant=%s app=%s: list env: %v", tenantID, ad.AppName, err)
		}
		envMap := make(map[string]string, len(envs))
		for _, e := range envs {
			envMap[e.EnvKey] = e.EnvValue
		}

		out[ad.AppName] = nats.AppConfig{
			DeploymentID:   ad.DeploymentID,
			DeploymentHash: deployment.Hash,
			Env:            envMap,
			Allowlist:      allowlist,
			MaxMemoryMB:    maxMemoryMB,
		}
	}
	return out, nil
}
