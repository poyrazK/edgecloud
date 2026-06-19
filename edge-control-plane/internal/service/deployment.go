package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
	"github.com/google/uuid"
)

// IsValidAppName returns true if the app name is safe for use in paths.
// Rejects empty strings and strings containing path traversal characters.
func IsValidAppName(name string) bool {
	if name == "" {
		return false
	}
	return !strings.ContainsAny(name, "/\\..")
}

// MaxArtifactSize is the maximum allowed artifact size in bytes (100 MiB).
const MaxArtifactSize = 100 * 1024 * 1024

// Sentinel errors.
var ErrMaxDeploymentsQuotaExceeded = fmt.Errorf("max deployments reached for tenant")

// DeploymentService handles deployment business logic.
type DeploymentService struct {
	deploymentRepo *repository.DeploymentRepository
	activeRepo     *repository.ActiveDeploymentRepository
	appEnvRepo     *repository.AppEnvRepository
	quotaRepo      *repository.QuotaRepository
	tenantRepo     *repository.TenantRepository
	artifactStore  *storage.ArtifactStore
	publisher      nats.Publisher
	appSvc         *AppService
}

func NewDeploymentService(
	deploymentRepo *repository.DeploymentRepository,
	activeRepo *repository.ActiveDeploymentRepository,
	appEnvRepo *repository.AppEnvRepository,
	quotaRepo *repository.QuotaRepository,
	tenantRepo *repository.TenantRepository,
	artifactStore *storage.ArtifactStore,
	publisher nats.Publisher,
) *DeploymentService {
	return &DeploymentService{
		deploymentRepo: deploymentRepo,
		activeRepo:     activeRepo,
		appEnvRepo:     appEnvRepo,
		quotaRepo:      quotaRepo,
		tenantRepo:     tenantRepo,
		artifactStore:  artifactStore,
		publisher:      publisher,
	}
}

// SetAppService sets the AppService dependency for auto-creating apps on deploy.
func (s *DeploymentService) SetAppService(appSvc *AppService) {
	s.appSvc = appSvc
}

// Deploy creates a new deployment and stores the artifact.
func (s *DeploymentService) Deploy(ctx context.Context, tenantID, appName string, r io.Reader) (*domain.Deployment, error) {
	// Validate appName to prevent path traversal (defense-in-depth)
	if !IsValidAppName(appName) {
		return nil, fmt.Errorf("invalid app name")
	}

	// Auto-create the app record if it doesn't already exist (backward compatible).
	if s.appSvc != nil {
		if err := s.appSvc.CreateIfNotExists(ctx, tenantID, appName); err != nil {
			return nil, fmt.Errorf("creating app: %w", err)
		}
	}

	// Check quota
	quota, err := s.quotaRepo.GetByTenantID(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("getting quota: %w", err)
	}

	count, err := s.deploymentRepo.CountByApp(ctx, tenantID, appName)
	if err != nil {
		return nil, fmt.Errorf("counting deployments: %w", err)
	}
	if count >= quota.MaxDeployments {
		return nil, ErrMaxDeploymentsQuotaExceeded
	}

	// Read artifact and compute hash (bounded to prevent memory exhaustion)
	data, err := io.ReadAll(io.LimitReader(r, MaxArtifactSize+1))
	if err != nil {
		return nil, fmt.Errorf("reading artifact: %w", err)
	}
	if int64(len(data)) > MaxArtifactSize {
		return nil, fmt.Errorf("artifact exceeds maximum size of %d bytes", MaxArtifactSize)
	}

	// Reject non-wasm artifacts before persisting them. Without this guard a
	// non-wasm file would be stored, hashed, and shipped to workers, where
	// it would fail only at execution time. Magic bytes are the cheapest
	// first-line check — full module validation is wasmtime's job.
	if !validateWasm(data) {
		return nil, fmt.Errorf("invalid wasm artifact: missing magic bytes (\\0asm)")
	}

	hash := sha256.Sum256(data)

	deployment := &domain.Deployment{
		ID:        "d_" + uuid.New().String(),
		TenantID:  tenantID,
		AppName:   appName,
		Status:    domain.StatusDeployed,
		Hash:      hex.EncodeToString(hash[:]),
		CreatedAt: time.Now(),
	}

	if err := s.deploymentRepo.Create(ctx, deployment); err != nil {
		return nil, fmt.Errorf("creating deployment: %w", err)
	}

	// Save artifact
	if err := s.artifactStore.Save(tenantID, appName, deployment.ID, bytes.NewReader(data)); err != nil {
		return nil, fmt.Errorf("saving artifact: %w", err)
	}

	return deployment, nil
}

func (s *DeploymentService) GetDeployment(ctx context.Context, tenantID, id string) (*domain.Deployment, error) {
	deployment, err := s.deploymentRepo.GetByID(ctx, id)
	if err != nil || deployment == nil {
		return nil, err
	}
	if deployment.TenantID != tenantID {
		return nil, nil // not found for this tenant
	}
	return deployment, nil
}

func (s *DeploymentService) ListDeployments(ctx context.Context, tenantID, appName string) ([]domain.Deployment, error) {
	return s.deploymentRepo.ListByApp(ctx, tenantID, appName)
}

func (s *DeploymentService) ListDeploymentsPaginated(ctx context.Context, tenantID, appName string, limit, offset int) ([]domain.Deployment, error) {
	// Negative inputs are silently corrected: limit ≤ 0 becomes 20, offset < 0 becomes 0.
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	return s.deploymentRepo.ListByAppPaginated(ctx, tenantID, appName, limit, offset)
}

func (s *DeploymentService) ListDeploymentsPaginatedWithTotal(ctx context.Context, tenantID, appName string, limit, offset int) ([]domain.Deployment, int, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	total, err := s.deploymentRepo.CountByApp(ctx, tenantID, appName)
	if err != nil {
		return nil, 0, fmt.Errorf("counting deployments: %w", err)
	}
	deployments, err := s.deploymentRepo.ListByAppPaginated(ctx, tenantID, appName, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	return deployments, total, nil
}

func (s *DeploymentService) ActivateDeployment(ctx context.Context, tenantID, appName, deploymentID string) error {
	deployment, err := s.deploymentRepo.GetByID(ctx, deploymentID)
	if err != nil || deployment == nil {
		return fmt.Errorf("deployment not found")
	}
	if deployment.TenantID != tenantID || deployment.AppName != appName {
		return fmt.Errorf("deployment not found")
	}

	if err := s.activeRepo.Set(ctx, &domain.ActiveDeployment{
		TenantID:     tenantID,
		AppName:      appName,
		DeploymentID: deploymentID,
	}); err != nil {
		return fmt.Errorf("setting active deployment: %w", err)
	}

	// Publish task update
	envs, err := s.appEnvRepo.List(ctx, tenantID, appName)
	if err != nil {
		return fmt.Errorf("listing env vars: %w", err)
	}
	envMap := make(map[string]string)
	for _, e := range envs {
		envMap[e.EnvKey] = e.EnvValue
	}

	tenant, err := s.tenantRepo.GetByID(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("getting tenant: %w", err)
	}
	if tenant == nil {
		return fmt.Errorf("tenant not found")
	}

	quota, err := s.quotaRepo.GetByTenantID(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("getting quota: %w", err)
	}
	maxMemoryMB := 256
	if quota != nil {
		maxMemoryMB = quota.MaxMemoryMB
	}

	msg := &nats.TaskMessage{
		Type:      "task_update",
		Timestamp: time.Now(),
		TenantID:  tenantID,
		Apps: map[string]nats.AppConfig{
			appName: {
				DeploymentID:   deploymentID,
				DeploymentHash: deployment.Hash,
				Env:            envMap,
				Allowlist:      tenant.AllowlistedDestinations,
				MaxMemoryMB:    maxMemoryMB,
			},
		},
	}
	if err := s.publisher.PublishTaskUpdate("global", msg); err != nil {
		return fmt.Errorf("publishing task update: %w", err)
	}

	return nil
}

func (s *DeploymentService) GetActiveDeployment(ctx context.Context, tenantID, appName string) (*domain.Deployment, error) {
	ad, err := s.activeRepo.Get(ctx, tenantID, appName)
	if err != nil || ad == nil {
		return nil, err
	}
	return s.deploymentRepo.GetByID(ctx, ad.DeploymentID)
}

func (s *DeploymentService) GetArtifact(ctx context.Context, tenantID, appName, deploymentID string) (io.ReadCloser, error) {
	// Verify deployment belongs to this tenant
	deployment, err := s.deploymentRepo.GetByID(ctx, deploymentID)
	if err != nil || deployment == nil {
		return nil, fmt.Errorf("deployment not found")
	}
	if deployment.TenantID != tenantID || deployment.AppName != appName {
		return nil, fmt.Errorf("deployment not found")
	}
	return s.artifactStore.Open(tenantID, appName, deploymentID)
}
