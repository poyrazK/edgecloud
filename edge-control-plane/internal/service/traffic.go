package service

import (
	"context"
	"fmt"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// TrafficService handles traffic split business logic.
type TrafficService struct {
	splitRepo      *repository.TrafficSplitRepository
	deploymentRepo  *repository.DeploymentRepository
	activeRepo     *repository.ActiveDeploymentRepository
	appEnvRepo     *repository.AppEnvRepository
	tenantRepo     *repository.TenantRepository
	quotaRepo      *repository.QuotaRepository
	publisher      nats.Publisher
}

// NewTrafficService creates a TrafficService.
func NewTrafficService(
	splitRepo *repository.TrafficSplitRepository,
	deploymentRepo *repository.DeploymentRepository,
	activeRepo *repository.ActiveDeploymentRepository,
	appEnvRepo *repository.AppEnvRepository,
	tenantRepo *repository.TenantRepository,
	quotaRepo *repository.QuotaRepository,
	publisher nats.Publisher,
) *TrafficService {
	return &TrafficService{
		splitRepo:      splitRepo,
		deploymentRepo: deploymentRepo,
		activeRepo:     activeRepo,
		appEnvRepo:     appEnvRepo,
		tenantRepo:     tenantRepo,
		quotaRepo:      quotaRepo,
		publisher:      publisher,
	}
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
func (s *TrafficService) SetTraffic(ctx context.Context, tenantID, appName string, entries []domain.TrafficSplitEntry) error {
	if len(entries) == 0 {
		// Clearing all splits is a valid operation — equivalent to no canary.
		return s.splitRepo.DeleteAllForApp(ctx, tenantID, appName)
	}

	splits := make([]*domain.TrafficSplit, len(entries))
	for i, e := range entries {
		d, err := s.deploymentRepo.GetByID(ctx, e.DeploymentID)
		if err != nil || d == nil {
			return fmt.Errorf("deployment %q not found", e.DeploymentID)
		}
		if d.TenantID != tenantID || d.AppName != appName {
			return fmt.Errorf("deployment %q not found", e.DeploymentID)
		}
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

	if err := s.splitRepo.Set(ctx, splits); err != nil {
		return fmt.Errorf("setting traffic split: %w", err)
	}

	// Publish task update to activate all deployments in the split concurrently.
	return s.publishTaskUpdate(ctx, tenantID, appName)
}

// GetTraffic returns the current traffic splits for an app.
func (s *TrafficService) GetTraffic(ctx context.Context, tenantID, appName string) ([]*domain.TrafficSplit, error) {
	splits, err := s.splitRepo.Get(ctx, tenantID, appName)
	if err != nil {
		return nil, err
	}
	return splits, nil
}

// ClearTraffic removes all traffic splits for an app.
func (s *TrafficService) ClearTraffic(ctx context.Context, tenantID, appName string) error {
	return s.splitRepo.DeleteAllForApp(ctx, tenantID, appName)
}

// publishTaskUpdate sends a TaskMessage that tells workers to run all
// deployments in the traffic split concurrently.
func (s *TrafficService) publishTaskUpdate(ctx context.Context, tenantID, appName string) error {
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
	envMap := make(map[string]string)
	for _, e := range envs {
		envMap[e.EnvKey] = e.EnvValue
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
	// The primary deployment's hash is used as DeploymentHash; all instances
	// share the same env/allowlist/max_memory.
	var primaryHash string
	routes := make([]nats.DeploymentRoute, len(splits))
	for i, sp := range splits {
		d, _ := s.deploymentRepo.GetByID(ctx, sp.DeploymentID)
		routes[i] = nats.DeploymentRoute{DeploymentID: sp.DeploymentID, Weight: sp.Weight}
		if i == 0 || primaryHash == "" {
			primaryHash = d.Hash
		}
	}

	// Publish to default region "global". A future phase will fan out per
	// deployment's region list (matching the ActivateDeployment pattern).
	msg := &nats.TaskMessage{
		Type:      "task_update",
		Timestamp: time.Now(),
		TenantID:  tenantID,
		Apps: map[string]nats.AppConfig{
			appName: {
				DeploymentID:   splits[0].DeploymentID, // primary; Routes drives worker behavior
				DeploymentHash: primaryHash,
				Routes:        routes,
				Env:           envMap,
				Allowlist:     domain.StringArrayTo(tenant.AllowlistedDestinations),
				MaxMemoryMB:   maxMemoryMB,
			},
		},
	}

	return s.publisher.PublishTaskUpdate("global", msg)
}
