package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
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

	// Issue #560: deps needed to publish-if-active on env writes.
	// All are injected post-construction via SetPublishDeps so the
	// existing NewEnvService(appEnvRepo) signature stays stable
	// for test harnesses that don't exercise the publish path.
	//
	// appEnvRepoTx is the concrete *repository.AppEnvRepository
	// used only by the publish path. It's separate from the
	// EnvRepoInterface field above because the WithTx(tx) method
	// lives on the concrete type and the mocks that implement
	// EnvRepoInterface don't have it. Set to the same underlying
	// instance as appEnvRepo at wiring time — they hit the same
	// rows.
	db             *sqlx.DB
	tenantRepo     *repository.TenantRepository
	activeRepo     *repository.ActiveDeploymentRepository
	deploymentRepo *repository.DeploymentRepository
	quotaRepo      *repository.QuotaRepository
	outboxRepo     *repository.OutboxRepository
	appEnvRepoTx   *repository.AppEnvRepository
	publishBuilder *publishBuilder
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

// GetEncryptor exposes the underlying encryptor. Returns nil when
// encryption is disabled (dev mode). Used by DeploymentService's
// env-load+decrypt block (issue #560) so it can route through the
// shared loadDecryptedEnvMap helper below without needing a
// passthrough Decrypt call.
func (s *EnvService) GetEncryptor() *SecretEncryptor {
	return s.encryptor
}

// SetPublishDeps wires the publish-if-active path (issue #560). All
// seven deps must be non-nil for env writes to notify workers; if
// any is missing, SetEnv/DeleteEnv silently skip the publish (best-
// effort like activate's cache-push — the env write itself still
// succeeds). Mirrors the DeploymentService setter pattern so test
// harnesses can build a minimal EnvService without these.
func (s *EnvService) SetPublishDeps(
	db *sqlx.DB,
	tenantRepo *repository.TenantRepository,
	activeRepo *repository.ActiveDeploymentRepository,
	deploymentRepo *repository.DeploymentRepository,
	quotaRepo *repository.QuotaRepository,
	outboxRepo *repository.OutboxRepository,
	appEnvRepoTx *repository.AppEnvRepository,
	publishBuilder *publishBuilder,
) {
	s.db = db
	s.tenantRepo = tenantRepo
	s.activeRepo = activeRepo
	s.deploymentRepo = deploymentRepo
	s.quotaRepo = quotaRepo
	s.outboxRepo = outboxRepo
	s.appEnvRepoTx = appEnvRepoTx
	s.publishBuilder = publishBuilder
}

// SetEnv writes the env row and, if the app has an active deployment,
// publishes a task_update so the running worker picks up the new env
// (issue #560). Apps with no active deployment silently skip the
// publish — there's no worker to notify yet.
//
// Returns ErrTenantDisabled (mapped to 409 by the handler) if the
// tenant row is currently disabled; mirrors DeploymentService's
// pre-check at deployment.go and closes the issue #440 disable-vs-
// mutate race window for the env-write path.
//
// Tx lifecycle: the env write, the active-row read, the deployment /
// tenant / quota reads, and the outbox enqueue all run inside ONE
// sqlx tx. A failure at any step rolls back the env write too — a
// 5xx from `edge env set` therefore leaves the env row unchanged,
// matching the pre-#560 contract for activate's outbox path.
func (s *EnvService) SetEnv(ctx context.Context, tenantID, appName, key, value string) error {
	encrypted, err := s.encryptor.Encrypt(value)
	if err != nil {
		return fmt.Errorf("encrypting env value: %w", err)
	}
	if s.publishDepsReady() {
		return s.withPublishTx(ctx, func(tx *sqlx.Tx) error {
			if err := s.appEnvRepoTx.WithTx(tx).Set(ctx, &domain.AppEnv{
				TenantID: tenantID,
				AppName:  appName,
				EnvKey:   key,
				EnvValue: encrypted,
			}); err != nil {
				return fmt.Errorf("setting env: %w", err)
			}
			return s.publishIfActiveTx(ctx, tx, tenantID, appName)
		})
	}
	// Pre-#560 fallback: publish deps not wired (test harness / dev
	// mode). The env write goes through the non-tx repo; no publish.
	return s.appEnvRepo.Set(ctx, &domain.AppEnv{
		TenantID: tenantID,
		AppName:  appName,
		EnvKey:   key,
		EnvValue: encrypted,
	})
}

// publishDepsReady returns true iff every publish-path dep is
// non-nil. Used to gate the tx path in SetEnv / DeleteEnv so legacy
// test harnesses that don't wire the full publish graph keep
// working without modification.
func (s *EnvService) publishDepsReady() bool {
	return s.db != nil && s.tenantRepo != nil && s.activeRepo != nil &&
		s.deploymentRepo != nil && s.quotaRepo != nil && s.outboxRepo != nil &&
		s.appEnvRepoTx != nil && s.publishBuilder != nil
}

// withPublishTx runs fn inside a single sqlx tx, committing on nil
// error and rolling back otherwise. Centralizes the BEGIN / COMMIT
// envelope for SetEnv / DeleteEnv so both call sites share the same
// rollback semantics.
func (s *EnvService) withPublishTx(ctx context.Context, fn func(tx *sqlx.Tx) error) error {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		// Rollback is a no-op after a successful Commit (sqlx
		// signals the tx as closed). Defensive: any panic inside fn
		// also rolls back here.
		_ = tx.Rollback()
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// publishIfActiveTx is the core issue #560 helper. Reads
// active_deployments under the caller's tx; if no active row exists,
// returns nil (silent skip — there's no worker to notify yet). If an
// active row exists, looks up the deployment / tenant / quota / env
// under the same tx, builds the TaskMessage payload via the shared
// publishBuilder, and enqueues an outbox row (issue #42) — the
// drainer publishes it after commit.
//
// Returns ErrTenantDisabled (mapped to 409) when the tenant is
// currently disabled; the env write that preceded this call rolls
// back with the rest of the tx.
func (s *EnvService) publishIfActiveTx(ctx context.Context, tx *sqlx.Tx, tenantID, appName string) error {
	active, err := s.activeRepo.WithTx(tx).Get(ctx, tenantID, appName)
	if err != nil {
		return fmt.Errorf("reading active deployment: %w", err)
	}
	if active == nil {
		// No active deployment → no worker to notify. Silent skip
		// per the issue #560 design decision (no 409, no error).
		return nil
	}

	deployment, err := s.deploymentRepo.WithTx(tx).GetByID(ctx, active.DeploymentID)
	if err != nil {
		return fmt.Errorf("loading deployment: %w", err)
	}
	if deployment == nil {
		// Active row points at a deleted deployment — fail loud so
		// the env write rolls back. The reconcile loop will GC the
		// orphaned active row on its next tick.
		return fmt.Errorf("active row references missing deployment %s", active.DeploymentID)
	}

	tenant, err := s.tenantRepo.WithTx(tx).GetByID(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("loading tenant: %w", err)
	}
	if tenant == nil {
		return ErrTenantNotFound
	}
	if tenant.IsDisabled() {
		return fmt.Errorf("%w: tenant=%s", ErrTenantDisabled, tenantID)
	}

	quota, err := s.quotaRepo.WithTx(tx).GetByTenantID(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("loading quota: %w", err)
	}

	envMap, err := loadDecryptedEnvMap(ctx, s.appEnvRepoTx.WithTx(tx), s.encryptor, tenantID, appName)
	if err != nil {
		return err
	}

	regions := deployment.Regions
	// Empty regions are fine — the publisher fans out via stream config,
	// and pq.StringArray below handles both nil and []string{}. Mirrors
	// the activate path's deployment.Regions propagation.

	payload, err := s.publishBuilder.buildPublishPayload(ctx, tenantID, appName,
		active.DeploymentID, deployment, tenant, regions, quota, envMap, "")
	if err != nil {
		return fmt.Errorf("building publish payload: %w", err)
	}

	attemptID := uuid.NewString()
	if err := s.outboxRepo.WithTx(tx).Enqueue(ctx, &repository.OutboxRow{
		TenantID:  tenantID,
		AppName:   appName,
		Kind:      "task_update",
		Payload:   payload,
		Regions:   pq.StringArray(regions),
		DedupeKey: tenantID + ":" + appName + ":" + attemptID,
	}); err != nil {
		return fmt.Errorf("enqueueing outbox row: %w", err)
	}
	return nil
}

// Decrypt is a pass-through to the encryptor. Used by publish call sites
// that read env vars from the repo directly and need to decrypt inline.
func (s *EnvService) Decrypt(value string) (string, error) {
	return s.encryptor.Decrypt(value)
}

// loadDecryptedEnvMap reads the full env map for (tenantID, appName)
// via appEnvRepo and decrypts each value through encryptor (a nil
// encryptor signals plaintext passthrough — dev mode). Replaces the
// otherwise-duplicated 12-line list+decrypt block that previously
// lived in three places:
//
//   - (*EnvService).publishIfActiveTx (issue #560)
//   - (*DeploymentService).activateDeployment (issue #440)
//   - (*DeploymentService).RollbackDeployment (issue #440)
//
// The call site is responsible for tx-scoping appEnvRepo (typically
// `appEnvRepo.WithTx(tx)`); this helper does no transaction management.
func loadDecryptedEnvMap(ctx context.Context, appEnvRepo *repository.AppEnvRepository, encryptor *SecretEncryptor, tenantID, appName string) (map[string]string, error) {
	rows, err := appEnvRepo.List(ctx, tenantID, appName)
	if err != nil {
		return nil, fmt.Errorf("listing env vars: %w", err)
	}
	envMap := make(map[string]string, len(rows))
	for _, e := range rows {
		if encryptor != nil {
			plain, derr := encryptor.Decrypt(e.EnvValue)
			if derr != nil {
				return nil, fmt.Errorf("decrypting env %s: %w", e.EnvKey, derr)
			}
			envMap[e.EnvKey] = plain
			continue
		}
		envMap[e.EnvKey] = e.EnvValue
	}
	return envMap, nil
}

// ListEnv fetches the env rows for an app and decrypts them for the
// HTTP response. Reads happen OUTSIDE a tx (single-shot List), so a
// concurrent env write may show up partially — that's fine for a
// GET-style handler.
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

// DeleteEnv deletes the env row and, if the app has an active
// deployment, publishes a task_update so the running worker picks
// up the absence of the env var (issue #560). Same shape as SetEnv:
// silent skip when there's no active deployment; ErrTenantDisabled
// (409) when the tenant is disabled; the env delete and the publish
// run inside ONE tx so a 5xx leaves the row in place.
func (s *EnvService) DeleteEnv(ctx context.Context, tenantID, appName, key string) error {
	if s.publishDepsReady() {
		return s.withPublishTx(ctx, func(tx *sqlx.Tx) error {
			if err := s.appEnvRepoTx.WithTx(tx).Delete(ctx, tenantID, appName, key); err != nil {
				return fmt.Errorf("deleting env: %w", err)
			}
			return s.publishIfActiveTx(ctx, tx, tenantID, appName)
		})
	}
	// Pre-#560 fallback.
	return s.appEnvRepo.Delete(ctx, tenantID, appName, key)
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
