package service

import (
	"bytes"
	"context"
	"database/sql"
	"log"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	natsio "github.com/nats-io/nats.go"
)

// recordingJetStream satisfies jetstreamPublisher for unit tests. Each
// Publish call is appended to publishes so the test can assert on the
// subject + payload (issue #440 disable-side serialization).
type recordingJetStream struct {
	mu        sync.Mutex
	publishes []recordedPublishJS
}

type recordedPublishJS struct {
	subject string
	data    []byte
}

func (r *recordingJetStream) Publish(subject string, data []byte, _ ...natsio.PubOpt) (*natsio.PubAck, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.publishes = append(r.publishes, recordedPublishJS{subject: subject, data: append([]byte(nil), data...)})
	return &natsio.PubAck{Stream: "edgecloud-tasks"}, nil
}

// workerSvcForDisableTest wires a WorkerService backed by sqlmock. The
// returned cleanup closes the mock DB and verifies all expectations
// were met (sqlmock does this on Close).
func workerSvcForDisableTest(t *testing.T, listByTenantFn func(ctx context.Context, tenantID string) ([]domain.ActiveDeployment, error)) (*WorkerService, sqlmock.Sqlmock, *recordingJetStream, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	repo := repository.NewActiveDeploymentRepository(sqlxDB)
	js := &recordingJetStream{}
	// Default listByTenantFn: empty result (tests can override).
	if listByTenantFn == nil {
		listByTenantFn = func(_ context.Context, _ string) ([]domain.ActiveDeployment, error) {
			return nil, nil
		}
	}
	svc := &WorkerService{
		db:         sqlxDB,
		activeRepo: &listByTenantWrapper{concrete: repo, fn: listByTenantFn},
		tenantRepo: &tenantRepoForDisable{TenantRepository: repository.NewTenantRepository(sqlxDB)},
		jsForTest:  js,
		nc:         nil,
	}
	return svc, mock, js, func() { _ = mockDB.Close() }
}

// tenantRepoForDisable is a thin shim around *TenantRepository that
// satisfies tenantRepoInterface by exposing WithTx. The concrete
// *TenantRepository already has SetDisabledAt, ClearDisabledAt, GetByID,
// GetForUpdate — only WithTx is missing on the interface so the call
// `s.tenantRepo.WithTx(tx).GetForUpdate(...)` typechecks.
type tenantRepoForDisable struct {
	*repository.TenantRepository
}

func (t *tenantRepoForDisable) WithTx(tx *sqlx.Tx) *repository.TenantRepository {
	return t.TenantRepository.WithTx(tx)
}

// listByTenantWrapper swaps the post-commit ListByTenant call for a
// test-supplied function while delegating everything else to the
// concrete repo. Used by the racing-activate test to simulate a fresh
// row that arrived between the in-tx snapshot and the post-commit
// read.
type listByTenantWrapper struct {
	concrete *repository.ActiveDeploymentRepository
	fn       func(ctx context.Context, tenantID string) ([]domain.ActiveDeployment, error)
}

func (w *listByTenantWrapper) Get(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error) {
	return w.concrete.Get(ctx, tenantID, appName)
}
func (w *listByTenantWrapper) SetStableSince(ctx context.Context, tenantID, appName, deploymentID string, ts time.Time) error {
	return w.concrete.SetStableSince(ctx, tenantID, appName, deploymentID, ts)
}
func (w *listByTenantWrapper) ClearStableSince(ctx context.Context, tenantID, appName string) error {
	return w.concrete.ClearStableSince(ctx, tenantID, appName)
}
func (w *listByTenantWrapper) PromoteToLastGood(ctx context.Context, tenantID, appName, deploymentID string) error {
	return w.concrete.PromoteToLastGood(ctx, tenantID, appName, deploymentID)
}
func (w *listByTenantWrapper) ListByTenant(ctx context.Context, tenantID string) ([]domain.ActiveDeployment, error) {
	if w.fn != nil {
		return w.fn(ctx, tenantID)
	}
	return w.concrete.ListByTenant(ctx, tenantID)
}
func (w *listByTenantWrapper) WithTx(tx *sqlx.Tx) *repository.ActiveDeploymentRepository {
	return w.concrete.WithTx(tx)
}

// TestActivateDeployment_DisabledTenant_ReturnsErrTenantDisabled pins
// the activate-side guard added in commit 2 of issue #440: when
// tenant.GetForUpdate returns a row with non-nil disabled_at, the
// activate must abort with ErrTenantDisabled and skip the publish.
func TestActivateDeployment_DisabledTenant_ReturnsErrTenantDisabled(t *testing.T) {
	pub := newRecordingPublisher()
	svc, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const (
		deploymentID = "d_abc123"
		appName      = "myapp"
		tenantID     = "t_disabled"
	)

	// 1. deploymentRepo.GetByID.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
			AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, "hash", `{"us-east"}`, time.Now(), false, "", "", []byte{}, 0, nil, nil, nil))

	// 2. Activate tx: tenant.GetForUpdate returns a disabled row →
	//    abort. The active_deployments GetForUpdate must NOT run.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at FROM tenants WHERE id = \$1 FOR UPDATE`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at", "disabled_at"}).
			AddRow(tenantID, "Disabled Tenant", "free", pq.Array([]string{}), time.Now().Add(-1*time.Hour), time.Now()))
	mock.ExpectRollback()

	err := svc.ActivateDeployment(context.Background(), tenantID, appName, deploymentID)
	if err == nil {
		t.Fatalf("ActivateDeployment on disabled tenant returned nil; want ErrTenantDisabled")
	}
	if !isErrTenantDisabled(err) {
		t.Errorf("err = %v, want ErrTenantDisabled", err)
	}
	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("publish regions = %v, want none (tenant disabled before publish)", got)
	}
}

// isErrTenantDisabled unwraps errors.Is(err, service.ErrTenantDisabled)
// without forcing this file to import the `service` package (it lives
// in service itself). Helper used only by the subtests in this file.
func isErrTenantDisabled(err error) bool {
	return err != nil && strings.Contains(err.Error(), ErrTenantDisabled.Error())
}

// TestActivateDeployment_DisabledAtPostCommit_ReturnsErrTenantDisabled
// pins the defence-in-depth post-commit check at deployment.go:984:
// the tx-time guard passes (tenant.GetForUpdate returns enabled_at=nil)
// and the tx commits, but the post-commit GetByID observes a tenant
// that became disabled between the lock release and the read. The
// activate must still abort with ErrTenantDisabled (now wrapping the
// sentinel so the handler maps to 409) and must NOT publish.
//
// This covers a future non-tx activation path that skips the tx-time
// FOR UPDATE check, or a race where a disable commits in the narrow
// window between the tx-time check and the post-commit read. Without
// this test, dropping the post-commit check would not be caught by
// the existing test suite.
func TestActivateDeployment_DisabledAtPostCommit_ReturnsErrTenantDisabled(t *testing.T) {
	pub := newRecordingPublisher()
	svc, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const (
		deploymentID = "d_abc123"
		appName      = "myapp"
		tenantID     = "t_postcommit_disabled"
	)

	// 1. deploymentRepo.GetByID.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
			AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, "hash", `{"us-east"}`, time.Now(), false, "", "", []byte{}, 0, nil, nil, nil))

	// 2. Activate tx: tenant.GetForUpdate returns ENABLED (the tx-time
	//    guard passes). active_deployments get/insert/clear all succeed
	//    so the tx commits cleanly.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at FROM tenants WHERE id = \$1 FOR UPDATE`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at", "disabled_at"}).
			AddRow(tenantID, "Tenant A", "free", pq.Array([]string{}), time.Now(), nil))
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs(tenantID, appName).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	// 3. appEnvRepo.List — return no env vars (activate flow does
	//    DecryptEnvMap-via-List when envSvc is nil; we want to reach
	//    the post-commit tenant GetByID, so the env query must
	//    succeed first).
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))

	// 4. Post-commit tenant.GetByID returns a DISABLED row (a disable
	//    raced into the gap between the tx-time FOR UPDATE release
	//    and this read). The defence-in-depth check at deployment.go:984
	//    must fire here, wrap ErrTenantDisabled, and short-circuit
	//    before publishSwap runs.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at FROM tenants WHERE id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at", "disabled_at"}).
			AddRow(tenantID, "Tenant A", "free", pq.Array([]string{}), time.Now(), time.Now()))

	err := svc.ActivateDeployment(context.Background(), tenantID, appName, deploymentID)
	if err == nil {
		t.Fatalf("ActivateDeployment on post-commit-disabled tenant returned nil; want ErrTenantDisabled")
	}
	if !isErrTenantDisabled(err) {
		t.Errorf("err = %v, want ErrTenantDisabled (post-commit check must wrap the sentinel)", err)
	}
	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("publish regions = %v, want none (post-commit check must short-circuit before publishSwap)", got)
	}
}

// TestDisableTenantAtomically_NoConcurrentActivate_PublishesEmpty
// pins the happy path: no in-flight activate row has a NULL
// last_publish_at, so the disable path enters no wait and publishes
// empty per region.
func TestDisableTenantAtomically_NoConcurrentActivate_PublishesEmpty(t *testing.T) {
	buf, restore := captureLogger(t)
	defer restore()

	regions := []string{"us-east", "eu-west"}
	svc, mock, js, cleanup := workerSvcForDisableTest(t, func(_ context.Context, _ string) ([]domain.ActiveDeployment, error) {
		// Post-commit read: one row, last_publish_at non-NULL
		// (publishSwap completed well before disable arrived).
		priorPublish := time.Now().Add(-1 * time.Second)
		return []domain.ActiveDeployment{
			{
				TenantID:                   "t_a",
				AppName:                    "app1",
				DeploymentID:               "d_1",
				RegionsPublished:           regions,
				LastPublishAt:              &priorPublish,
				ActivationAttemptStartedAt: nil,
			},
		}, nil
	})
	defer cleanup()

	const tenantID = "t_a"

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at FROM tenants WHERE id = \$1 FOR UPDATE`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at", "disabled_at"}).
			AddRow(tenantID, "Tenant A", "free", pq.Array([]string{}), time.Now(), nil))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE tenants SET disabled_at = $2 WHERE id = $1`)).
		WithArgs(tenantID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := svc.disableTenantAtomically(context.Background(), tenantID); err != nil {
		t.Fatalf("disableTenantAtomically: %v", err)
	}

	if len(js.publishes) != len(regions) {
		t.Fatalf("publish count = %d, want %d (one per region)", len(js.publishes), len(regions))
	}
	got := make(map[string][]byte, len(js.publishes))
	for _, p := range js.publishes {
		got[p.subject] = p.data
	}
	for _, r := range regions {
		subj := "edgecloud.tasks." + r
		data, ok := got[subj]
		if !ok {
			t.Errorf("missing publish to %s; got subjects %v", subj, keysOf(got))
			continue
		}
		if !bytes.Contains(data, []byte(`"apps":{}`)) {
			t.Errorf("publish to %s missing empty apps payload; got %s", subj, string(data))
		}
		if !bytes.Contains(data, []byte(`"tenant_id":"`+tenantID+`"`)) {
			t.Errorf("publish to %s missing tenant_id; got %s", subj, string(data))
		}
	}
	out := buf.String()
	if !strings.Contains(out, "published empty task_update for disabled tenant "+tenantID) {
		t.Errorf("missing published-empty log line; got %q", out)
	}
}

// TestDisableTenantAtomically_RacingActivatePublishCompleted_PublishesEmpty
// pins the post-redesign behavior (issue #440 fix, commit 3):
// when the racing activate's publishSwap has already stamped
// last_publish_at by the time the disable path checks, the disable
// path enters no wait loop and publishes empty immediately. The
// activate's task_update is already on the wire; the disable's
// empty lands after — JetStream per-subject ordering is the
// de-facto guarantee on the worker side (H5 follow-up adds a
// generation token).
//
// The (rare) case where the racing activate's publish hasn't
// completed yet — driving the new wait loop — is covered by
// commit 9's TestDisableTenantAtomically_WaitsForInFlight
// ActivatePublishes / …WaitTimeoutExitsAndPublishes.
func TestDisableTenantAtomically_RacingActivatePublishCompleted_PublishesEmpty(t *testing.T) {
	_, restore := captureLogger(t)
	defer restore()

	now := time.Now()
	priorPublish := now.Add(-1 * time.Second)
	svc, mock, js, cleanup := workerSvcForDisableTest(t, func(_ context.Context, _ string) ([]domain.ActiveDeployment, error) {
		// Post-commit read: one racing-activate row whose
		// publishSwap already completed (LastPublishAt set).
		// Old commit-1's diff guard would have made the disable
		// path skip the empty publish here; post-redesign we
		// publish empty anyway — the activate's on-the-wire
		// message reaches workers before the disable's empty,
		// guaranteed by JetStream per-subject ordering.
		return []domain.ActiveDeployment{
			{
				TenantID:                   "t_a",
				AppName:                    "fresh",
				DeploymentID:               "d_fresh",
				ActivationAttemptStartedAt: &now,
				LastPublishAt:              &priorPublish,
			},
		}, nil
	})
	defer cleanup()

	const tenantID = "t_a"

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at FROM tenants WHERE id = \$1 FOR UPDATE`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at", "disabled_at"}).
			AddRow(tenantID, "Tenant A", "free", pq.Array([]string{}), time.Now(), nil))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE tenants SET disabled_at = $2 WHERE id = $1`)).
		WithArgs(tenantID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := svc.disableTenantAtomically(context.Background(), tenantID); err != nil {
		t.Fatalf("disableTenantAtomically: %v", err)
	}

	if len(js.publishes) == 0 {
		t.Errorf("publish count = 0, want ≥1 (post-redesign publish empty even when racing activate completed)")
	}
}

// TestDisableTenantAtomically_EmptyActiveList_NoPublish pins the
// "no current active rows" case: the disable commits but
// notifyDisableTenant short-circuits (no region to broadcast to), so
// zero publishes is the correct outcome. This guards against an
// accidental publish of empty to a global region with no apps to
// stop.
func TestDisableTenantAtomically_EmptyActiveList_NoPublish(t *testing.T) {
	_, restore := captureLogger(t)
	defer restore()

	// wrapper default fn returns nil — both in-tx and post-commit
	// reads see no rows.
	svc, mock, js, cleanup := workerSvcForDisableTest(t, nil)
	defer cleanup()

	const tenantID = "t_empty"

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at FROM tenants WHERE id = \$1 FOR UPDATE`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at", "disabled_at"}).
			AddRow(tenantID, "Empty Tenant", "free", pq.Array([]string{}), time.Now(), nil))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE tenants SET disabled_at = $2 WHERE id = $1`)).
		WithArgs(tenantID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := svc.disableTenantAtomically(context.Background(), tenantID); err != nil {
		t.Fatalf("disableTenantAtomically: %v", err)
	}

	if len(js.publishes) != 0 {
		t.Errorf("publish count = %d, want 0 (no active rows ⇒ no publish)", len(js.publishes))
	}
}

func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// silence unused-import warnings if a future refactor drops a usage.
var _ = log.Writer
