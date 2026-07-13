package service

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/jmoiron/sqlx"
)

// TrafficDeploymentRepoInterface is the deployment-repo subset needed by TrafficService.
type TrafficDeploymentRepoInterface interface {
	GetByID(ctx context.Context, id string) (*domain.Deployment, error)
}

// TrafficSplitRepoInterface is the split-repo subset needed by TrafficService.
type TrafficSplitRepoInterface interface {
	Get(ctx context.Context, tenantID, appName string) ([]*domain.TrafficSplit, error)
	DeleteAllForApp(ctx context.Context, tenantID, appName string) error
}

// TrafficActiveRepoInterface is the active-deployment-repo subset needed by TrafficService.
type TrafficActiveRepoInterface interface {
	Get(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error)
}

// TrafficEnvRepoInterface is the app-env-repo subset needed by TrafficService.
type TrafficEnvRepoInterface interface {
	List(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error)
}

// TrafficEnvDecrypter is the subset of SecretEncryptor used by TrafficService.
// Injected via SetEnvDecrypter. When nil, env values pass through as plaintext.
type TrafficEnvDecrypter interface {
	Decrypt(value string) (string, error)
}

// TrafficTenantRepoInterface is the tenant-repo subset needed by TrafficService.
type TrafficTenantRepoInterface interface {
	GetByID(ctx context.Context, id string) (*domain.Tenant, error)
}

// TrafficQuotaRepoInterface is the quota-repo subset needed by TrafficService.
type TrafficQuotaRepoInterface interface {
	GetByTenantID(ctx context.Context, tenantID string) (*domain.Quota, error)
}

// TrafficPublisherInterface is the NATS publisher subset needed by TrafficService.
type TrafficPublisherInterface interface {
	PublishTaskUpdate(region string, msg *nats.TaskMessage) error
}

// TrafficService handles traffic split business logic.
type TrafficService struct {
	db             *sqlx.DB
	splitRepo      TrafficSplitRepoInterface
	deploymentRepo TrafficDeploymentRepoInterface
	activeRepo     TrafficActiveRepoInterface
	appEnvRepo     TrafficEnvRepoInterface
	tenantRepo     TrafficTenantRepoInterface
	quotaRepo      TrafficQuotaRepoInterface
	publisher      TrafficPublisherInterface
	// defaultRegion is the fallback when none of the splits' deployments
	// declare any regions of their own. Mirrors DeploymentService.defaultRegion
	// (which gets it from config) so a control plane that runs without an
	// explicit region still publishes to a subject every worker subscribes to.
	defaultRegion string
	// envDecrypter decrypts env values before publishing to workers.
	// When nil, values pass through as plaintext (dev mode / backward compat).
	envDecrypter TrafficEnvDecrypter
}

// NewTrafficService creates a TrafficService.
func NewTrafficService(
	db *sqlx.DB,
	splitRepo TrafficSplitRepoInterface,
	deploymentRepo TrafficDeploymentRepoInterface,
	activeRepo TrafficActiveRepoInterface,
	appEnvRepo TrafficEnvRepoInterface,
	tenantRepo TrafficTenantRepoInterface,
	quotaRepo TrafficQuotaRepoInterface,
	publisher TrafficPublisherInterface,
	defaultRegion string,
) *TrafficService {
	return &TrafficService{
		db:             db,
		splitRepo:      splitRepo,
		deploymentRepo: deploymentRepo,
		activeRepo:     activeRepo,
		appEnvRepo:     appEnvRepo,
		tenantRepo:     tenantRepo,
		quotaRepo:      quotaRepo,
		publisher:      publisher,
		defaultRegion:  defaultRegion,
	}
}

// SetEnvDecrypter injects the decrypter used for decrypting env values at publish.
func (s *TrafficService) SetEnvDecrypter(dec TrafficEnvDecrypter) {
	s.envDecrypter = dec
}

// ValidateSum checks that the sum of weights equals 100.
func ValidateSum(splits []*domain.TrafficSplit) error {
	var total int
	for _, s := range splits {
		total += s.Weight
	}
	if total != 100 {
		return fmt.Errorf("weights must sum to 100, got %d", total)
	}
	return nil
}

// SetTraffic atomically sets the traffic splits for an app.
// Each deployment_id is validated to belong to the tenant and app.
// Sum of weights must equal 100.
//
// With len(entries) == 0, this is a "clear" — all splits are deleted and a
// legacy single-deployment TaskMessage is published so workers stop any
// canary deployment and revert to the active deployment only. Without that
// publish, workers keep the stale `Routes` from the last non-empty
// TaskMessage and continue splitting traffic on a deployment the control
// plane considers inactive.
func (s *TrafficService) SetTraffic(ctx context.Context, tenantID, appName string, entries []domain.TrafficSplitEntry) error {
	if len(entries) == 0 {
		if err := s.splitRepo.DeleteAllForApp(ctx, tenantID, appName); err != nil {
			return fmt.Errorf("clearing traffic splits: %w", err)
		}
		return s.publishClearTaskUpdate(ctx, tenantID, appName)
	}

	splits := make([]*domain.TrafficSplit, len(entries))
	deployments := make(map[string]*domain.Deployment, len(entries))
	for i, e := range entries {
		d, err := s.deploymentRepo.GetByID(ctx, e.DeploymentID)
		if err != nil || d == nil {
			return fmt.Errorf("deployment %q not found", e.DeploymentID)
		}
		if d.TenantID != tenantID || d.AppName != appName {
			return fmt.Errorf("deployment %q not found", e.DeploymentID)
		}
		deployments[e.DeploymentID] = d
		splits[i] = &domain.TrafficSplit{
			TenantID:     tenantID,
			AppName:      appName,
			DeploymentID: e.DeploymentID,
			Weight:       e.Weight,
		}
	}

	if err := ValidateSum(splits); err != nil {
		return err
	}

	if err := repository.SetTrafficSplits(ctx, s.db, splits); err != nil {
		return fmt.Errorf("setting traffic split: %w", err)
	}

	// Publish task update to activate all deployments in the split concurrently.
	return s.publishTaskUpdate(ctx, tenantID, appName, deployments)
}

// GetTraffic returns the current traffic splits for an app.
func (s *TrafficService) GetTraffic(ctx context.Context, tenantID, appName string) ([]*domain.TrafficSplit, error) {
	splits, err := s.splitRepo.Get(ctx, tenantID, appName)
	if err != nil {
		return nil, err
	}
	return splits, nil
}

// ClearTraffic removes all traffic splits for an app and republishes a
// TaskMessage so workers reconcile back to the active deployment (otherwise
// they'd keep the stale Routes from the previous canary TaskMessage).
func (s *TrafficService) ClearTraffic(ctx context.Context, tenantID, appName string) error {
	if err := s.splitRepo.DeleteAllForApp(ctx, tenantID, appName); err != nil {
		return fmt.Errorf("clearing traffic splits: %w", err)
	}
	return s.publishClearTaskUpdate(ctx, tenantID, appName)
}

// publishClearTaskUpdate publishes a single-deployment TaskMessage for the
// currently-active deployment (per active_deployments), so any worker
// running a canary route stops it and reverts to the active deployment.
// If no active deployment exists yet, nothing is published — workers
// haven't been told to run the app in the first place, so they have
// nothing to fall back from.
func (s *TrafficService) publishClearTaskUpdate(ctx context.Context, tenantID, appName string) error {
	active, err := s.activeRepo.Get(ctx, tenantID, appName)
	if err != nil {
		return fmt.Errorf("getting active deployment: %w", err)
	}
	if active == nil {
		return nil
	}
	dep, err := s.deploymentRepo.GetByID(ctx, active.DeploymentID)
	if err != nil || dep == nil {
		return fmt.Errorf("active deployment %q not found", active.DeploymentID)
	}

	envs, err := s.appEnvRepo.List(ctx, tenantID, appName)
	if err != nil {
		return fmt.Errorf("listing env vars: %w", err)
	}
	envMap, err := buildEnvMap(envs, s.envDecrypter)
	if err != nil {
		return err
	}

	tenant, err := s.tenantRepo.GetByID(ctx, tenantID)
	if err != nil || tenant == nil {
		return fmt.Errorf("tenant not found")
	}

	quota, err := s.quotaRepo.GetByTenantID(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("getting quota: %w", err)
	}
	maxMemoryMB := 256
	if quota != nil && quota.MaxMemoryMB > 0 {
		maxMemoryMB = quota.MaxMemoryMB
	}

	regions := domain.StringArrayTo(dep.Regions)
	if len(regions) == 0 {
		regions = []string{s.defaultRegion}
	}

	msg := &nats.TaskMessage{
		Type:      "task_update",
		Timestamp: time.Now(),
		TenantID:  tenantID,
		Apps: map[string]nats.AppConfig{
			appName: nats.BuildAppConfig(
				dep.ID,
				dep.Hash,
				dep.Signature,    // issue #307
				dep.SigningKeyID, // issue #307 PR1: per-key kid
				"",               // issue #308: preview_id — RepublishActiveDeployments only fires for production actives, never previews
				0,                // issue #308: preview_pr_number — same reasoning as preview_id
				envMap,
				tenant.AllowlistedDestinations,
				maxMemoryMB,
				// no routes — legacy single-deployment shape
			),
		},
	}

	var failedRegions []string
	for _, region := range regions {
		if err := s.publisher.PublishTaskUpdate(region, msg); err != nil {
			log.Printf("publishing clear task update failed for region %q (tenant %s, app %s): %v", region, tenantID, appName, err)
			failedRegions = append(failedRegions, region)
		}
	}
	if len(failedRegions) > 0 {
		return fmt.Errorf("publishing clear task update failed for region(s): %s", strings.Join(failedRegions, ","))
	}
	return nil
}

// publishTaskUpdate sends a TaskMessage that tells workers to run all
// deployments in the traffic split concurrently.
//
// `deployments` is the cache of split deployments fetched during SetTraffic's
// validation pass, keyed by deployment_id. Reusing it cuts the redundant
// `deploymentRepo.GetByID` roundtrips that the previous implementation made
// in the route-building and region-fanout loops (3N → N lookups per
// SetTraffic call). It also removes the split-brain window where a
// deployment's Hash could differ between the route entry and the regions
// fanout (each loop saw a different snapshot).
func (s *TrafficService) publishTaskUpdate(ctx context.Context, tenantID, appName string, deployments map[string]*domain.Deployment) error {
	splits, err := s.splitRepo.Get(ctx, tenantID, appName)
	if err != nil {
		return fmt.Errorf("fetching splits: %w", err)
	}
	if len(splits) == 0 {
		return nil // nothing to publish
	}

	envs, err := s.appEnvRepo.List(ctx, tenantID, appName)
	if err != nil {
		return fmt.Errorf("listing env vars: %w", err)
	}
	envMap, err := buildEnvMap(envs, s.envDecrypter)
	if err != nil {
		return err
	}

	tenant, err := s.tenantRepo.GetByID(ctx, tenantID)
	if err != nil || tenant == nil {
		return fmt.Errorf("tenant not found")
	}

	quota, err := s.quotaRepo.GetByTenantID(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("getting quota: %w", err)
	}
	maxMemoryMB := 256
	if quota != nil && quota.MaxMemoryMB > 0 {
		maxMemoryMB = quota.MaxMemoryMB
	}

	// Build the routes list for the nats.AppConfig.
	// Each route carries its OWN deployment_hash — the worker needs the
	// per-route hash to download and verify the right artifact (the
	// top-level AppConfig.DeploymentHash only covers the primary). All
	// routes share the same env/allowlist/max_memory from the app config.
	var primaryHash string
	var primarySigningKeyID string // issue #307 PR1
	routes := make([]nats.DeploymentRoute, len(splits))
	for i, sp := range splits {
		d, ok := deployments[sp.DeploymentID]
		if !ok || d == nil {
			return fmt.Errorf("deployment %q not found", sp.DeploymentID)
		}
		routes[i] = nats.DeploymentRoute{
			DeploymentID:        sp.DeploymentID,
			DeploymentHash:      d.Hash,
			DeploymentSignature: d.Signature,    // issue #307: per-route signature
			SigningKeyID:        d.SigningKeyID, // issue #307 PR1: per-key kid
			Weight:              sp.Weight,
		}
		if i == 0 {
			primaryHash = d.Hash
			// Save the primary's kid so the top-level AppConfig
			// SigningKeyID reflects the same key used for the
			// primary route (issue #307 PR1).
			primarySigningKeyID = d.SigningKeyID
		}
	}

	// Fan out the TaskMessage to the union of regions declared by every
	// split's deployment. A worker subscribed to one of those regions will
	// pick up the message via its `filter_subject` and reconcile.
	regionSet := make(map[string]struct{}, len(deployments))
	for _, d := range deployments {
		for _, r := range domain.StringArrayTo(d.Regions) {
			regionSet[r] = struct{}{}
		}
	}
	regions := make([]string, 0, len(regionSet))
	for r := range regionSet {
		regions = append(regions, r)
	}
	if len(regions) == 0 {
		regions = []string{s.defaultRegion}
	}

	msg := &nats.TaskMessage{
		Type:      "task_update",
		Timestamp: time.Now(),
		TenantID:  tenantID,
		Apps: map[string]nats.AppConfig{
			appName: nats.BuildAppConfig(
				splits[0].DeploymentID, // primary; Routes drives worker behavior
				primaryHash,
				routes[0].DeploymentSignature, // primary signature (issue #307)
				primarySigningKeyID,           // issue #307 PR1: per-key kid
				"",                            // issue #308: preview_id — canary splits are production-only by design
				0,                             // issue #308: preview_pr_number — same reasoning as preview_id
				envMap,
				tenant.AllowlistedDestinations,
				maxMemoryMB,
				routes...,
			),
		},
	}

	var failedRegions []string
	for _, region := range regions {
		if err := s.publisher.PublishTaskUpdate(region, msg); err != nil {
			log.Printf("publishing task update for traffic split failed for region %q (tenant %s, app %s): %v", region, tenantID, appName, err)
			failedRegions = append(failedRegions, region)
		}
	}
	if len(failedRegions) > 0 {
		return fmt.Errorf("publishing traffic split failed for region(s): %s", strings.Join(failedRegions, ","))
	}
	return nil
}

// buildEnvMap converts a slice of AppEnv rows into a map, decrypting values
// when a decrypter is provided. Used by both publishClearTaskUpdate and
// publishTaskUpdate.
//
// Issue #441: a decrypt error (ErrPlaintextEnvNotAllowed or
// ErrCiphertextMismatch) is now propagated rather than swallowed —
// publishing plaintext env values to workers would defeat the entire
// secrets encryption model. Callers bubble the error up to the publish
// boundary, which fails the activate / rollback / reconcile.
func buildEnvMap(envs []domain.AppEnv, dec TrafficEnvDecrypter) (map[string]string, error) {
	m := make(map[string]string, len(envs))
	for _, e := range envs {
		v := e.EnvValue
		if dec != nil {
			d, err := dec.Decrypt(e.EnvValue)
			if err != nil {
				return nil, fmt.Errorf("decrypting env %s: %w", e.EnvKey, err)
			}
			v = d
		}
		m[e.EnvKey] = v
	}
	return m, nil
}
