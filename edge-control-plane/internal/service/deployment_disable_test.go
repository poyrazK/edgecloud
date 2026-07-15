package service

import (
	"bytes"
	"context"
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
	getFn    func(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error)
}

func (w *listByTenantWrapper) Get(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error) {
	if w.getFn != nil {
		return w.getFn(ctx, tenantID, appName)
	}
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
	svc, _, mock, cleanup := activateSvcForTest(t, pub, "global")
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
	mock.ExpectQuery(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id = \$1 FOR UPDATE`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at", "disabled_at", "overage_allowed_until"}).
			AddRow(tenantID, "Disabled Tenant", "free", pq.Array([]string{}), time.Now().Add(-1*time.Hour), time.Now(), nil))
	mock.ExpectRollback()

	err := svc.ActivateDeployment(context.Background(), tenantID, appName, deploymentID, "")
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
// pins the tx-time tenant.GetForUpdate check at deployment.go:1194
// (issue #440). The activate tx takes a tenants FOR UPDATE row lock
// before reading active_deployments; when that read returns a row
// with non-nil disabled_at, the activate must abort with
// ErrTenantDisabled (now wrapping the sentinel so the handler maps
// it to 409) and must NOT publish.
//
// The tx-time check inside the activate tx IS the post-commit check
// of the previous design: under READ COMMITTED, the disable's
// SetDisabledAt can only commit before our FOR UPDATE if it took
// the lock first; the lock serializes them, so observing disabled_at
// non-nil here means disable won and we abandon the publish. (The
// no-row "tenant disabled between deploy and activate" path is
// covered by TestApplyTenantDelta.)
func TestActivateDeployment_DisabledAtPostCommit_ReturnsErrTenantDisabled(t *testing.T) {
	pub := newRecordingPublisher()
	svc, _, mock, cleanup := activateSvcForTest(t, pub, "global")
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

	// 2. Activate tx: tenant.GetForUpdate returns DISABLED (the tx-time
	//    guard fires). Activate must roll back and return
	//    ErrTenantDisabled without touching active_deployments or
	//    the outbox.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id = \$1 FOR UPDATE`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at", "disabled_at", "overage_allowed_until"}).
			AddRow(tenantID, "Tenant A", "free", pq.Array([]string{}), time.Now(), time.Now(), nil))
	mock.ExpectRollback()

	err := svc.ActivateDeployment(context.Background(), tenantID, appName, deploymentID, "")
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
	mock.ExpectQuery(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id = \$1 FOR UPDATE`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at", "disabled_at", "overage_allowed_until"}).
			AddRow(tenantID, "Tenant A", "free", pq.Array([]string{}), time.Now(), nil, nil))
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
	mock.ExpectQuery(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id = \$1 FOR UPDATE`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at", "disabled_at", "overage_allowed_until"}).
			AddRow(tenantID, "Tenant A", "free", pq.Array([]string{}), time.Now(), nil, nil))
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
	mock.ExpectQuery(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id = \$1 FOR UPDATE`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at", "disabled_at", "overage_allowed_until"}).
			AddRow(tenantID, "Empty Tenant", "free", pq.Array([]string{}), time.Now(), nil, nil))
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

// TestDisableTenantAtomically_WaitsForInFlightActivatePublishes
// pins the canonical race fix (issue #440 commit 3): the disable
// path detects an in-flight activate row (ActivationAttemptStartedAt
// recent, LastPublishAt NULL) and polls last_publish_at until the
// activate's publishSwap stamps it. Only then does disable publish
// the empty task_update — JetStream per-subject ordering means the
// activate's non-empty message reaches workers first.
func TestDisableTenantAtomically_WaitsForInFlightActivatePublishes(t *testing.T) {
	buf, restore := captureLogger(t)
	defer restore()

	regions := []string{"us-east"}
	now := time.Now()
	priorAttemptStart := now.Add(-2 * time.Second)
	var getCalls int
	publishDoneCh := make(chan struct{})

	svc, mock, js, cleanup := workerSvcForDisableTest(t, func(_ context.Context, _ string) ([]domain.ActiveDeployment, error) {
		// One in-flight row from a racing activate. disable path
		// must poll Get(...) until LastPublishAt becomes non-NULL.
		return []domain.ActiveDeployment{
			{
				TenantID:                   "t_a",
				AppName:                    "app1",
				DeploymentID:               "d_1",
				RegionsPublished:           regions,
				ActivationAttemptStartedAt: &priorAttemptStart,
				LastPublishAt:              nil,
			},
		}, nil
	})
	// Override Get on the wrapper so we can simulate the activate's
	// publishSwap stamping last_publish_at mid-wait.
	svc.activeRepo.(*listByTenantWrapper).getFn = func(_ context.Context, _, appName string) (*domain.ActiveDeployment, error) {
		getCalls++
		lp := &now
		if getCalls < 3 {
			// First two polls: still null (simulate in-flight publish).
			lp = nil
		}
		// From the 3rd call: stamped.
		if getCalls == 3 {
			close(publishDoneCh)
		}
		return &domain.ActiveDeployment{
			TenantID:                   "t_a",
			AppName:                    appName,
			DeploymentID:               "d_1",
			RegionsPublished:           regions,
			ActivationAttemptStartedAt: &priorAttemptStart,
			LastPublishAt:              lp,
		}, nil
	}
	// Use a fast controllable ticker so the test completes quickly.
	svc.SetDisablePublishWaitPoll(func(_ time.Duration) (<-chan time.Time, func()) {
		ch := make(chan time.Time, 1)
		// Fire one tick after a short delay so the loop runs a few
		// iterations; the Get override stamps LastPublishAt on the
		// third call.
		go func() {
			time.Sleep(20 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch, func() {}
	})
	// Generous budget so we don't time out before the 3rd poll.
	svc.SetDisablePublishWaitBudget(2 * time.Second)
	defer cleanup()

	const tenantID = "t_a"

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id = \$1 FOR UPDATE`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at", "disabled_at", "overage_allowed_until"}).
			AddRow(tenantID, "Tenant A", "free", pq.Array([]string{}), time.Now(), nil, nil))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE tenants SET disabled_at = $2 WHERE id = $1`)).
		WithArgs(tenantID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := svc.disableTenantAtomically(context.Background(), tenantID); err != nil {
		t.Fatalf("disableTenantAtomically: %v", err)
	}

	// Wait loop must have polled Get at least once before stamping.
	if getCalls < 3 {
		t.Errorf("Get call count = %d, want ≥ 3 (wait loop polled; closed over `publishDoneCh` after stamp)", getCalls)
	}
	select {
	case <-publishDoneCh:
	default:
		t.Errorf("publishDoneCh never closed — wait loop never saw last_publish_at stamped")
	}
	if len(js.publishes) != len(regions) {
		t.Fatalf("publish count = %d, want %d (empty per region after wait completed)", len(js.publishes), len(regions))
	}
	out := buf.String()
	if !strings.Contains(out, "waiting up to") {
		t.Errorf("missing 'waiting up to' log line; got %q", out)
	}
	if !strings.Contains(out, "all stamped last_publish_at") {
		t.Errorf("missing 'all stamped' log line; got %q", out)
	}
}

// TestDisableTenantAtomically_WaitTimeoutExitsAndPublishes pins the
// graceful-degrade path (issue #440 commit 3): when the in-flight
// activate's publishSwap never completes within the budget, the
// disable path logs a 'timed out' line and publishes empty anyway.
// The trade-off documented in the plan: better to publish empty
// than to leave workers running an over-quota tenant for the 5-min
// reconcile cycle.
func TestDisableTenantAtomically_WaitTimeoutExitsAndPublishes(t *testing.T) {
	buf, restore := captureLogger(t)
	defer restore()

	regions := []string{"us-east"}
	now := time.Now()
	priorAttemptStart := now.Add(-2 * time.Second)

	svc, mock, js, cleanup := workerSvcForDisableTest(t, func(_ context.Context, _ string) ([]domain.ActiveDeployment, error) {
		return []domain.ActiveDeployment{
			{
				TenantID:                   "t_a",
				AppName:                    "app1",
				DeploymentID:               "d_1",
				RegionsPublished:           regions,
				ActivationAttemptStartedAt: &priorAttemptStart,
				LastPublishAt:              nil, // never stamps
			},
		}, nil
	})
	svc.activeRepo.(*listByTenantWrapper).getFn = func(_ context.Context, _, appName string) (*domain.ActiveDeployment, error) {
		return &domain.ActiveDeployment{
			TenantID: "t_a", AppName: appName, DeploymentID: "d_1",
			RegionsPublished:           regions,
			ActivationAttemptStartedAt: &priorAttemptStart,
			LastPublishAt:              nil,
		}, nil
	}
	// Ticker that fires a few times but budget is short.
	svc.SetDisablePublishWaitPoll(func(_ time.Duration) (<-chan time.Time, func()) {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(10 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch, func() {}
	})
	svc.SetDisablePublishWaitBudget(50 * time.Millisecond)
	defer cleanup()

	const tenantID = "t_a"

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id = \$1 FOR UPDATE`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at", "disabled_at", "overage_allowed_until"}).
			AddRow(tenantID, "Tenant A", "free", pq.Array([]string{}), time.Now(), nil, nil))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE tenants SET disabled_at = $2 WHERE id = $1`)).
		WithArgs(tenantID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	start := time.Now()
	if err := svc.disableTenantAtomically(context.Background(), tenantID); err != nil {
		t.Fatalf("disableTenantAtomically: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("disable took %s, want < 2s (budget was 50ms)", elapsed)
	}
	if len(js.publishes) != len(regions) {
		t.Fatalf("publish count = %d, want %d (empty per region after timeout)", len(js.publishes), len(regions))
	}
	out := buf.String()
	if !strings.Contains(out, "publish wait timed out") {
		t.Errorf("missing 'timed out' log line; got %q", out)
	}
}

// TestDisableTenantAtomically_NoInFlightRows_PublishesImmediately
// pins the common-case fast path (issue #440 commit 3): no rows
// have a recent ActivationAttemptStartedAt with NULL
// LastPublishAt, so the disable path skips the wait loop and
// publishes empty immediately. The 'waiting' log line must NOT
// appear.
func TestDisableTenantAtomically_NoInFlightRows_PublishesImmediately(t *testing.T) {
	buf, restore := captureLogger(t)
	defer restore()

	regions := []string{"us-east"}
	priorPublish := time.Now().Add(-1 * time.Second)
	svc, mock, js, cleanup := workerSvcForDisableTest(t, func(_ context.Context, _ string) ([]domain.ActiveDeployment, error) {
		return []domain.ActiveDeployment{
			{
				TenantID:                   "t_a",
				AppName:                    "app1",
				DeploymentID:               "d_1",
				RegionsPublished:           regions,
				ActivationAttemptStartedAt: nil, // no in-flight marker
				LastPublishAt:              &priorPublish,
			},
		}, nil
	})
	defer cleanup()

	const tenantID = "t_a"

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id = \$1 FOR UPDATE`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at", "disabled_at", "overage_allowed_until"}).
			AddRow(tenantID, "Tenant A", "free", pq.Array([]string{}), time.Now(), nil, nil))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE tenants SET disabled_at = $2 WHERE id = $1`)).
		WithArgs(tenantID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := svc.disableTenantAtomically(context.Background(), tenantID); err != nil {
		t.Fatalf("disableTenantAtomically: %v", err)
	}

	if len(js.publishes) != len(regions) {
		t.Fatalf("publish count = %d, want %d", len(js.publishes), len(regions))
	}
	out := buf.String()
	if strings.Contains(out, "waiting up to") {
		t.Errorf("unexpected 'waiting up to' log line; got %q", out)
	}
}

// TestRollbackDeployment_DisabledTenant_ReturnsErrTenantDisabled
// pins commit 2's coverage for the rollback path (issue #440):
// when tenant.GetForUpdate returns a row with non-nil
// disabled_at inside the rollback tx, rollback must abort with
// ErrTenantDisabled and skip the publishSwap. The handler's
// errors.Is(err, ErrTenantDisabled) → 409 mapping at
// deployment.go:798 is therefore reachable for the rollback route
// — it was previously dead code.
func TestRollbackDeployment_DisabledTenant_ReturnsErrTenantDisabled(t *testing.T) {
	disabledAt := time.Now().Add(-1 * time.Hour)
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	pub := newRecordingPublisher()
	svc := &DeploymentService{
		db:             sqlxDB,
		deploymentRepo: repository.NewDeploymentRepository(sqlxDB),
		activeRepo:     repository.NewActiveDeploymentRepository(sqlxDB),
		appEnvRepo:     repository.NewAppEnvRepository(sqlxDB),
		tenantRepo:     repository.NewTenantRepository(sqlxDB),
		quotaRepo:      repository.NewQuotaRepository(sqlxDB),
		publisher:      pub,
		defaultRegion:  "us-east",

		memoryQuotaRepo: mockDeployMemoryQuotaFactory(),
	}

	// Rollback tx: first query is tenants FOR UPDATE (disabled row).
	// Commit 2's guard reads it; on non-nil DisabledAt, abort with
	// ErrTenantDisabled. No further SQL runs.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id = \$1 FOR UPDATE`).
		WithArgs("t_disabled").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at", "disabled_at", "overage_allowed_until"}).
			AddRow("t_disabled", "Disabled", "free", pq.Array([]string{}), time.Now().Add(-24*time.Hour), disabledAt, nil))
	mock.ExpectRollback()

	_, err = svc.RollbackDeployment(context.Background(), "t_disabled", "myapp", "", false)
	if err == nil {
		t.Fatalf("RollbackDeployment on disabled tenant returned nil; want ErrTenantDisabled")
	}
	if !isErrTenantDisabled(err) {
		t.Errorf("err = %v, want ErrTenantDisabled", err)
	}
	// No publish should happen — publishSwap is skipped by the
	// guard.
	if got := len(pub.calls); got != 0 {
		t.Errorf("publish count = %d, want 0 (rollback aborted before publishSwap)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}
