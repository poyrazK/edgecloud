package service

import (
	"context"
	"errors"
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
	// StreamAll iterates every row in the table (issue #441).
	StreamAll(ctx context.Context, fn func(domain.AppEnv) error) error
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
// or old-key values to the new key. Safe to run concurrently with active
// deploys — each env value is read-decrypt-write under the row's upsert
// semantics.
//
// Issue #441: plaintext rows (legacy or seeded via SQL migration) are
// already plaintext, so re-encrypting them is a no-op. We count them
// (plaintextSkipped) and move on. Hard decrypt errors (cipher mismatch)
// still abort the sweep — those rows need investigation, not silent
// rewrite. The (reEncrypted, plaintextSkipped, err) return shape lets
// the admin handler surface both counts.
func (s *EnvService) ReEncryptAll(ctx context.Context) (reEncrypted, plaintextSkipped int, err error) {
	if s.encryptor == nil {
		return 0, 0, fmt.Errorf("encryption is disabled (no key configured)")
	}

	tenants, apps, err := s.appEnvRepo.ListAllApps(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("listing apps: %w", err)
	}

	for i := range tenants {
		rows, err := s.appEnvRepo.List(ctx, tenants[i], apps[i])
		if err != nil {
			return reEncrypted, plaintextSkipped, fmt.Errorf("listing env for %s/%s: %w", tenants[i], apps[i], err)
		}
		for _, row := range rows {
			decrypted, err := s.encryptor.Decrypt(row.EnvValue)
			if errors.Is(err, ErrPlaintextEnvNotAllowed) {
				plaintextSkipped++
				continue
			}
			if err != nil {
				return reEncrypted, plaintextSkipped, fmt.Errorf("decrypting %s/%s/%s: %w", tenants[i], apps[i], row.EnvKey, err)
			}
			reEncryptedVal, err := s.encryptor.Encrypt(decrypted)
			if err != nil {
				return reEncrypted, plaintextSkipped, fmt.Errorf("re-encrypting %s/%s/%s: %w", tenants[i], apps[i], row.EnvKey, err)
			}
			row.EnvValue = reEncryptedVal
			if err := s.appEnvRepo.Set(ctx, &row); err != nil {
				return reEncrypted, plaintextSkipped, fmt.Errorf("writing %s/%s/%s: %w", tenants[i], apps[i], row.EnvKey, err)
			}
			reEncrypted++
		}
	}
	return reEncrypted, plaintextSkipped, nil
}

// CountPlaintextRows streams every app_env row and counts how many do
// NOT match the encrypted shape for some key in the keyring. Used at
// startup (issue #441: refuse to boot when n>0 unless
// EDGE_ALLOW_LEGACY_PLAINTEXT_ENV=true) and on GET /admin/secrets/keys
// (plaintext_row_count field). Returns 0 immediately when the encryptor
// is nil (dev mode — there's nothing to be plaintext "against").
func (s *EnvService) CountPlaintextRows(ctx context.Context) (int, error) {
	if s.encryptor == nil {
		return 0, nil
	}
	n := 0
	err := s.appEnvRepo.StreamAll(ctx, func(env domain.AppEnv) error {
		if !s.encryptor.LooksLikeCipher(env.EnvValue) {
			n++
		}
		return nil
	})
	return n, err
}
