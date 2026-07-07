package service

import (
	"context"
	"fmt"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// EnvRepoInterface is the subset of *repository.AppEnvRepository used by EnvService.
type EnvRepoInterface interface {
	Set(ctx context.Context, env *domain.AppEnv) error
	List(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error)
	ListByApps(ctx context.Context, tenantID string, appNames []string) ([]domain.AppEnv, error)
	Delete(ctx context.Context, tenantID, appName, key string) error
	// ListAllApps returns all distinct (tenant_id, app_name) pairs.
	ListAllApps(ctx context.Context) ([]string, []string, error)
}

// EnvService handles environment variable business logic.
type EnvService struct {
	appEnvRepo EnvRepoInterface
	encryptor  *SecretEncryptor // nil = encryption disabled (dev mode)
}

func NewEnvService(appEnvRepo EnvRepoInterface) *EnvService {
	return &EnvService{appEnvRepo: appEnvRepo}
}

// SetSecretEncryptor sets the encryptor after construction.
// Returns the receiver so it can be chained.
func (s *EnvService) SetSecretEncryptor(sec *SecretEncryptor) *EnvService {
	s.encryptor = sec
	return s
}

func (s *EnvService) SetEnv(ctx context.Context, tenantID, appName, key, value string) error {
	encrypted, err := s.encryptor.Encrypt(value)
	if err != nil {
		return fmt.Errorf("encrypting env value: %w", err)
	}
	return s.appEnvRepo.Set(ctx, &domain.AppEnv{
		TenantID: tenantID,
		AppName:  appName,
		EnvKey:   key,
		EnvValue: encrypted,
	})
}

func (s *EnvService) ListEnv(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error) {
	rows, err := s.appEnvRepo.List(ctx, tenantID, appName)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		decrypted, err := s.encryptor.Decrypt(rows[i].EnvValue)
		if err != nil {
			return nil, fmt.Errorf("decrypting env %s: %w", rows[i].EnvKey, err)
		}
		rows[i].EnvValue = decrypted
	}
	return rows, nil
}

func (s *EnvService) DeleteEnv(ctx context.Context, tenantID, appName, key string) error {
	return s.appEnvRepo.Delete(ctx, tenantID, appName, key)
}

// Decrypt is a pass-through to the encryptor. Used by publish call sites
// that read env vars from the repo directly and need to decrypt inline.
func (s *EnvService) Decrypt(value string) (string, error) {
	return s.encryptor.Decrypt(value)
}

// DecryptEnvMap fetches env vars for an app and returns a decrypted map.
// Used at publish boundaries — the map is ready to embed in NATS AppConfig.
func (s *EnvService) DecryptEnvMap(ctx context.Context, tenantID, appName string) (map[string]string, error) {
	rows, err := s.appEnvRepo.List(ctx, tenantID, appName)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(rows))
	for _, r := range rows {
		v, err := s.encryptor.Decrypt(r.EnvValue)
		if err != nil {
			return nil, fmt.Errorf("decrypting env %s: %w", r.EnvKey, err)
		}
		out[r.EnvKey] = v
	}
	return out, nil
}

// DecryptEnvMapBulk fetches env vars for multiple apps in one query and
// returns a map of app_name → { key → value }. Used by the reconcile loop.
func (s *EnvService) DecryptEnvMapBulk(ctx context.Context, tenantID string, appNames []string) (map[string]map[string]string, error) {
	rows, err := s.appEnvRepo.ListByApps(ctx, tenantID, appNames)
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[string]string)
	for _, r := range rows {
		v, err := s.encryptor.Decrypt(r.EnvValue)
		if err != nil {
			return nil, fmt.Errorf("decrypting env %s/%s: %w", r.AppName, r.EnvKey, err)
		}
		if out[r.AppName] == nil {
			out[r.AppName] = make(map[string]string)
		}
		out[r.AppName][r.EnvKey] = v
	}
	return out, nil
}

// ReEncryptAll decrypts every env value across all tenants and re-encrypts
// with the current active key. Used after key rotation to migrate old-format
// or old-key values to the new key. Returns the total number of values
// re-encrypted. Safe to run concurrently with active deploys — each env
// value is read-decrypt-write under the row's upsert semantics.
func (s *EnvService) ReEncryptAll(ctx context.Context) (int, error) {
	if s.encryptor == nil {
		return 0, fmt.Errorf("encryption is disabled (no key configured)")
	}

	tenants, apps, err := s.appEnvRepo.ListAllApps(ctx)
	if err != nil {
		return 0, fmt.Errorf("listing apps: %w", err)
	}

	var total int
	for i := range tenants {
		rows, err := s.appEnvRepo.List(ctx, tenants[i], apps[i])
		if err != nil {
			return total, fmt.Errorf("listing env for %s/%s: %w", tenants[i], apps[i], err)
		}
		for _, row := range rows {
			decrypted, err := s.encryptor.Decrypt(row.EnvValue)
			if err != nil {
				return total, fmt.Errorf("decrypting %s/%s/%s: %w", tenants[i], apps[i], row.EnvKey, err)
			}
			reEncrypted, err := s.encryptor.Encrypt(decrypted)
			if err != nil {
				return total, fmt.Errorf("re-encrypting %s/%s/%s: %w", tenants[i], apps[i], row.EnvKey, err)
			}
			row.EnvValue = reEncrypted
			if err := s.appEnvRepo.Set(ctx, &row); err != nil {
				return total, fmt.Errorf("writing %s/%s/%s: %w", tenants[i], apps[i], row.EnvKey, err)
			}
			total++
		}
	}
	return total, nil
}
