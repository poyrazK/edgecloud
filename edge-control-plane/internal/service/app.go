package service

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// appRepoInterface defines the app repository methods used by AppService.
type appRepoInterface interface {
	Create(ctx context.Context, app *domain.App) error
	Get(ctx context.Context, tenantID, appName string) (*domain.App, error)
	List(ctx context.Context, tenantID string, limit, offset int) ([]domain.App, error)
	AtomicDelete(ctx context.Context, tenantID, appName string) (bool, error)
	InsertIfNotExists(ctx context.Context, app *domain.App) (bool, error)
}

// AppService handles app business logic.
type AppService struct {
	db            *sqlx.DB
	appRepo       appRepoInterface
	appEnvRepo    *repository.AppEnvRepository
	activeRepo    *repository.ActiveDeploymentRepository
	deployRepo    *repository.DeploymentRepository
	artifactStore *storage.ArtifactStore
}

func NewAppService(
	db *sqlx.DB,
	appRepo *repository.AppRepository,
	deploymentRepo *repository.DeploymentRepository,
	activeRepo *repository.ActiveDeploymentRepository,
	appEnvRepo *repository.AppEnvRepository,
	artifactStore *storage.ArtifactStore,
) *AppService {
	return &AppService{
		db:            db,
		appRepo:       appRepo,
		activeRepo:    activeRepo,
		appEnvRepo:    appEnvRepo,
		deployRepo:    deploymentRepo,
		artifactStore: artifactStore,
	}
}

// Create creates a new app. Returns ErrAppAlreadyExists if it already exists.
var ErrAppAlreadyExists = fmt.Errorf("app already exists")

func (s *AppService) Create(ctx context.Context, tenantID, appName string, req *domain.CreateAppRequest) (*domain.App, error) {
	if !IsValidAppName(appName) {
		return nil, fmt.Errorf("invalid app name: %s", appName)
	}

	var desc *string
	if req.Description != "" {
		desc = &req.Description
	}

	// Use InsertIfNotExists for an atomic check-and-insert — no TOCTOU race.
	app := &domain.App{
		ID:          "a_" + uuid.New().String(),
		TenantID:    tenantID,
		Name:        appName,
		Description: desc,
		CreatedAt:   time.Now(),
	}
	inserted, err := s.appRepo.InsertIfNotExists(ctx, app)
	if err != nil {
		return nil, fmt.Errorf("creating app: %w", err)
	}
	if !inserted {
		return nil, ErrAppAlreadyExists
	}
	return app, nil
}

// Get returns an app by name, or nil if not found.
func (s *AppService) Get(ctx context.Context, tenantID, appName string) (*domain.App, error) {
	return s.appRepo.Get(ctx, tenantID, appName)
}

// List returns apps for a tenant with pagination.
func (s *AppService) List(ctx context.Context, tenantID string, limit, offset int) ([]domain.App, error) {
	return s.appRepo.List(ctx, tenantID, limit, offset)
}

// Delete deletes an app and all its associated data atomically.
// Returns ErrAppNotFound if the app does not exist.
var ErrAppNotFound = fmt.Errorf("app not found")

func (s *AppService) Delete(ctx context.Context, tenantID, appName string) error {
	deleted, err := s.appRepo.AtomicDelete(ctx, tenantID, appName)
	if err != nil {
		return fmt.Errorf("deleting app: %w", err)
	}
	if !deleted {
		return ErrAppNotFound
	}

	// Delete artifact files before cascade — os.Remove is idempotent (no error if absent).
	// Collect errors so caller knows if cleanup failed.
	var delErr error
	if s.artifactStore != nil {
		deployments, err := s.deployRepo.ListByApp(ctx, tenantID, appName)
		if err != nil {
			log.Printf("warning: failed to list deployments for artifact cleanup: %v", err)
		} else {
			for _, d := range deployments {
				if err := s.artifactStore.Delete(tenantID, appName, d.ID); err != nil {
					delErr = fmt.Errorf("artifact cleanup failed for %s: %w", d.ID, err)
				}
			}
		}
	}

	// Cascade deletes run in a transaction so they either all succeed or all fail.
	err = repository.Transaction(ctx, s.db, func(tx *sqlx.Tx) error {
		appEnvRepo := s.appEnvRepo.WithTx(tx)
		activeRepo := s.activeRepo.WithTx(tx)
		deployRepo := s.deployRepo.WithTx(tx)

		if err := appEnvRepo.DeleteByApp(ctx, tenantID, appName); err != nil {
			return fmt.Errorf("deleting app env: %w", err)
		}
		if err := activeRepo.Delete(ctx, tenantID, appName); err != nil {
			return fmt.Errorf("deleting active deployment: %w", err)
		}
		if err := deployRepo.DeleteByApp(ctx, tenantID, appName); err != nil {
			return fmt.Errorf("deleting deployments: %w", err)
		}
		return nil
	})
	if err != nil {
		// Log but don't fail — app row already deleted above.
		// In production, consider a background reconciler to clean orphaned rows.
		log.Printf("warning: cascade delete partially failed after app deletion: %v", err)
	}
	// Surface artifact deletion errors to the caller. DB deletion has already committed.
	if delErr != nil {
		return delErr
	}
	return nil
}

// CreateIfNotExists creates an app if it doesn't already exist.
// Idempotent — safe to call multiple times.
func (s *AppService) CreateIfNotExists(ctx context.Context, tenantID, appName string) error {
	if !IsValidAppName(appName) {
		return fmt.Errorf("invalid app name: %s", appName)
	}

	app := &domain.App{
		ID:          "a_" + uuid.New().String(),
		TenantID:    tenantID,
		Name:        appName,
		Description: nil,
		CreatedAt:   time.Now(),
	}
	// InsertIfNotExists uses INSERT ... ON CONFLICT DO NOTHING — inherently idempotent.
	_, err := s.appRepo.InsertIfNotExists(ctx, app)
	if err != nil {
		return fmt.Errorf("creating app: %w", err)
	}
	return nil
}
