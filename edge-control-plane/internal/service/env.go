package service

import (
	"context"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// EnvService handles environment variable business logic.
type EnvService struct {
	appEnvRepo *repository.AppEnvRepository
}

func NewEnvService(appEnvRepo *repository.AppEnvRepository) *EnvService {
	return &EnvService{appEnvRepo: appEnvRepo}
}

func (s *EnvService) SetEnv(ctx context.Context, tenantID, appName, key, value string) error {
	return s.appEnvRepo.Set(ctx, &domain.AppEnv{
		TenantID: tenantID,
		AppName:  appName,
		EnvKey:   key,
		EnvValue: value,
	})
}

func (s *EnvService) ListEnv(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error) {
	return s.appEnvRepo.List(ctx, tenantID, appName)
}

func (s *EnvService) DeleteEnv(ctx context.Context, tenantID, appName, key string) error {
	return s.appEnvRepo.Delete(ctx, tenantID, appName, key)
}
