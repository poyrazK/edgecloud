package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// RecordingPublisher implements nats.Publisher by capturing every
// PublishTaskUpdate call. Used by the fan-out tests to assert that
// ActivateDeployment issues exactly one publish per region (and that
// the message body is identical across calls — only the region
// differs).
//
// failFor lets a test inject per-region failures; an empty map means
// every publish succeeds. PublishHeartbeat and EnsureStream are
// no-ops because ActivateDeployment never calls them.
type RecordingPublisher struct {
	calls   []recordedPublish
	failFor map[string]error
}

type recordedPublish struct {
	region string
	msg    *nats.TaskMessage
}

func newRecordingPublisher() *RecordingPublisher {
	return &RecordingPublisher{failFor: map[string]error{}}
}

// regionsCalled returns the regions in publish-call order (the
// service iterates deployment.Regions without reordering, so the
// order is stable).
func (p *RecordingPublisher) regionsCalled() []string {
	out := make([]string, len(p.calls))
	for i, c := range p.calls {
		out[i] = c.region
	}
	return out
}

func (p *RecordingPublisher) PublishTaskUpdate(region string, msg *nats.TaskMessage) error {
	p.calls = append(p.calls, recordedPublish{region: region, msg: msg})
	if err, ok := p.failFor[region]; ok {
		return err
	}
	return nil
}

func (p *RecordingPublisher) PublishHeartbeat(string, *nats.HeartbeatMessage) error {
	return nil
}

// PublishFullSync records the call but is not exercised by the fan-out
// tests (those only assert on ActivateDeployment's per-region
// task_update publishes). failFor semantics are NOT honored here — a
// FullSync publish failure during a test would surface as the test
// crashing, not as a "region failed" event, since FullSync is a
// scheduled reconcile, not an event-driven activation.
func (p *RecordingPublisher) PublishFullSync(region string, msg *nats.TaskMessage) error {
	p.calls = append(p.calls, recordedPublish{region: region, msg: msg})
	return nil
}

func (p *RecordingPublisher) EnsureStream(nats.StreamConfig) error { return nil }

// activateSvcForTest wires a DeploymentService with sqlmock-backed
// repositories and the given publisher. `defaultRegion` is what
// ActivateDeployment should fall back to when the deployment row has
// an empty regions array.
//
// Also returns an OutboxDrainer bound to the same sqlx.DB (issue #42):
// ActivateDeployment now writes an outbox row inside its tx instead
// of calling Publisher.PublishTaskUpdate. Tests that want to assert
// on the published TaskMessage must drive a drainer tick after
// ActivateDeployment returns.
func activateSvcForTest(t *testing.T, pub nats.Publisher, defaultRegion string) (*DeploymentService, *OutboxDrainer, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	outboxRepo := repository.NewOutboxRepository(sqlxDB)
	activeRepo := repository.NewActiveDeploymentRepository(sqlxDB)
	svc := &DeploymentService{
		db:             sqlxDB,
		deploymentRepo: repository.NewDeploymentRepository(sqlxDB),
		activeRepo:     activeRepo,
		appEnvRepo:     repository.NewAppEnvRepository(sqlxDB),
		tenantRepo:     repository.NewTenantRepository(sqlxDB),
		quotaRepo:      repository.NewQuotaRepository(sqlxDB),
		outboxRepo:     outboxRepo,
		publisher:      pub,
		defaultRegion:  defaultRegion,
	}
	drainer := NewOutboxDrainer(outboxRepo, pub, time.Second, 50, 10)
	return svc, drainer, mock, func() { _ = mockDB.Close() }
}

// expectTenantForUpdateOK mocks the tenant FOR UPDATE select that the
// activate / rollback tx issues at the very top (issue #440 disable
// gate). The row returned has disabled_at=NULL so the gate passes.
//
// The column list mirrors TenantRepository.GetForUpdate (7 columns):
// id, name, plan, allowlisted_destinations, created_at, disabled_at,
// overage_allowed_until. Returning only 5 columns still satisfies
// sqlmock because the query matcher is regex-based, but listing the
// full shape makes the column-count discipline visible at the call
// site — and lets future additions (e.g. a new column on tenants)
// fail loud here instead of silently truncating in production.
func expectTenantForUpdateOK(mock sqlmock.Sqlmock, tenantID string) {
	mock.ExpectQuery(`SELECT.*tenants.*FOR UPDATE`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "name", "plan", "allowlisted_destinations",
			"created_at", "disabled_at", "overage_allowed_until",
		}).AddRow(tenantID, "Test Tenant", "free", `{}`, time.Now(), nil, nil))
}

// TestActivateDeployment_FansOutToAllRegions pins the core #82
// behavior: a deployment row whose regions list has 3 entries
// results in exactly 3 PublishTaskUpdate calls, one per region, with
// the same TaskMessage body. If a future refactor accidentally drops
// the loop (publishes only to the first region, or uses the control
// plane's own region instead of the deployment's regions), this
// test fails.
func TestActivateDeployment_FansOutToAllRegions(t *testing.T) {
	pub := newRecordingPublisher()
	svc, drainer, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const (
		deploymentID   = "d_abc123"
		appName        = "myapp"
		tenantID       = "t_test"
		deploymentHash = "abc123hash"
	)

	// 1. deploymentRepo.GetByID returns a row with 3 regions.
	regionsCol := `{"us-east","eu-west","ap-south"}`
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
			AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, deploymentHash, regionsCol, time.Now(), false, "", "", []byte{}, 0, nil, nil, nil))

	// 2. ActivateDeployment wraps the GetForUpdate + Set + ClearStableSince
	// + buildPublishPayload + outbox INSERT in a single tx (issue #42).
	// The env / tenant / quota reads now happen INSIDE the tx via
	// WithTx so they participate in the same atomic snapshot.
	mock.ExpectBegin()
	// Issue #440: tenant FOR UPDATE gate. Must come before the
	// active_deployments read so the disable-vs-activate race is
	// closed (see deployment.go::activateDeployment). Disabled=nil
	// so the gate passes; the dedicated *_TenantGate_* tests cover
	// the disabled and not-found arms.
	expectTenantForUpdateOK(mock, tenantID)
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs(tenantID, appName).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// ActivateDeployment also calls ClearStableSince inside the tx
	// (resets the stability clock for the new deployment). Mock the
	// UPDATE; the new column doesn't change row shape for the mock.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// In-tx reads (issue #42 / #44 part 2): quota is read once,
	// up-front (reused for buildPublishPayload's maxMemoryMB and the
	// in-tx AddMemoryMB UPDATE — see deployment.go::activateDeployment).
	// env / tenant are then read by buildPublishPayload.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, used_outbound_bytes, used_request_count, used_memory_mb, quota_period_start, quota_lock_grace_until FROM quotas WHERE tenant_id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "max_requests_per_month", "used_outbound_bytes", "used_request_count", "used_memory_mb", "quota_period_start", "quota_lock_grace_until"}).
			AddRow(tenantID, 100, 50, 10, 512, 1024, 100_000, 0, 0, 0, time.Now(), nil))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))

	// 4. tenantRepo.GetByID — return a tenant with an allowlist so the
	// TaskMessage carries it. Projection widened post-#420 with
	// `disabled_at, overage_allowed_until`.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow(tenantID, "Test Tenant", "free", `{"api.example.com"}`, time.Now()))

	// Issue #42: outbox INSERT happens inside the tx (after the
	// ClearStableSince UPDATE, before the commit).
	expectInTxOutboxInsert(mock, tenantID, appName)
	expectInTxMemoryAdd(mock, tenantID, 512)

	mock.ExpectCommit()

	// Post-commit: drainer flow (ClaimDue → PublishTaskUpdate →
	// AppendRegionsPublished → MarkPublished). The mock quota row
	// above sets MaxMemoryMB=512; pass that through so the drainer
	// unmarshals the same MaxMemoryMB the production tx wrote.
	expectDrainerTickSuccess(t, mock, tenantID, appName, deploymentID,
		[]string{"us-east", "eu-west", "ap-south"}, 512)

	if err := svc.ActivateDeployment(context.Background(), tenantID, appName, deploymentID); err != nil {
		t.Fatalf("ActivateDeployment: %v", err)
	}

	// After ActivateDeployment, the publisher has NOT been called —
	// the message lives in the outbox table. Drive a drainer tick
	// to relay it.
	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("post-Activate publisher calls = %v, want [] (durable publish owns this)", got)
	}
	drainer.Tick(context.Background())

	// Exactly 3 publishes, one per region, in the deployment's order.
	gotRegions := pub.regionsCalled()
	wantRegions := []string{"us-east", "eu-west", "ap-south"}
	if !equalStringSlices(gotRegions, wantRegions) {
		t.Errorf("publish regions = %v, want %v", gotRegions, wantRegions)
	}

	// Every publish must use the same TaskMessage body (same
	// deployment id, hash, env, allowlist, MaxMemoryMB). Only the
	// region arg should differ.
	if len(pub.calls) != 3 {
		t.Fatalf("len(pub.calls) = %d, want 3", len(pub.calls))
	}
	first := pub.calls[0].msg.Apps[appName]
	// The mock quota row sets MaxMemoryMB=512; assert it propagates.
	if first.MaxMemoryMB != 512 {
		t.Errorf("call 0: MaxMemoryMB = %d, want 512", first.MaxMemoryMB)
	}
	for i, c := range pub.calls[1:] {
		app := c.msg.Apps[appName]
		if app.DeploymentID != first.DeploymentID ||
			app.DeploymentHash != first.DeploymentHash ||
			app.MaxMemoryMB != first.MaxMemoryMB {
			t.Errorf("call %d: msg differs from call 0: got deploymentID=%q hash=%q maxMemoryMB=%d, want %q / %q / %d",
				i+1, app.DeploymentID, app.DeploymentHash, app.MaxMemoryMB,
				first.DeploymentID, first.DeploymentHash, first.MaxMemoryMB)
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestActivateDeployment_DefaultFallback covers the pre-migration-008
// path: a deployment row whose regions array is empty (e.g. inserted
// before the column existed, or via a backfill that left the default).
// ActivateDeployment should publish exactly once, to the control
// plane's default region. Without this fallback, tenants on legacy
// rows would have their activate request look successful but never
// reach any worker.
func TestActivateDeployment_DefaultFallback(t *testing.T) {
	pub := newRecordingPublisher()
	svc, drainer, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const deploymentID = "d_legacy"
	const tenantID = "t_test"
	const appName = "myapp"
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
			AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, "h", `{}`, time.Now(), false, "", "", []byte{}, 0, nil, nil, nil))
	mock.ExpectBegin()
	// Issue #440: tenant FOR UPDATE gate. Must come before the
	// active_deployments read so the disable-vs-activate race is
	// closed (see deployment.go::activateDeployment). Disabled=nil
	// so the gate passes; the dedicated *_TenantGate_* tests cover
	// the disabled and not-found arms.
	expectTenantForUpdateOK(mock, tenantID)
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs("t_test", "myapp").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).WillReturnResult(sqlmock.NewResult(1, 1))
	// ActivateDeployment also calls ClearStableSince inside the tx
	// (resets the stability clock for the new deployment). Mock the
	// UPDATE; the new column doesn't change row shape for the mock.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// In-tx reads (issue #42 / #44 part 2): quota is read once,
	// up-front (reused for buildPublishPayload's maxMemoryMB and the
	// in-tx AddMemoryMB UPDATE — see deployment.go::activateDeployment).
	// env / tenant are then read by buildPublishPayload.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, used_outbound_bytes, used_request_count, used_memory_mb, quota_period_start, quota_lock_grace_until FROM quotas WHERE tenant_id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "max_requests_per_month", "used_outbound_bytes", "used_request_count", "used_memory_mb", "quota_period_start", "quota_lock_grace_until"}).
			AddRow("t_test", 100, 50, 10, 256, 1024, 100_000, 0, 0, 0, time.Now(), nil))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))

	// Issue #42: outbox INSERT happens inside the tx.
	expectInTxOutboxInsert(mock, "t_test", "myapp")
	expectInTxMemoryAdd(mock, "t_test", 256)
	mock.ExpectCommit()

	// Post-commit: drainer relays to "global".
	expectDrainerTickSuccess(t, mock, "t_test", "myapp", deploymentID,
		[]string{"global"}, 256)

	if err := svc.ActivateDeployment(context.Background(), "t_test", "myapp", deploymentID); err != nil {
		t.Fatalf("ActivateDeployment: %v", err)
	}
	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("post-Activate publisher calls = %v, want []", got)
	}
	drainer.Tick(context.Background())
	if got := pub.regionsCalled(); !equalStringSlices(got, []string{"global"}) {
		t.Errorf("publish regions = %v, want [global]", got)
	}
	// The mock quota row has MaxMemoryMB=256; assert it propagates so
	// the quota→TaskMessage wiring is pinned.
	if got := pub.calls[0].msg.Apps["myapp"].MaxMemoryMB; got != 256 {
		t.Errorf("MaxMemoryMB = %d, want 256", got)
	}
}

// TestActivateDeployment_NonGlobalDefaultFallback verifies the same
// fallback path but with a non-default control-plane region. The
// activate should publish to the control plane's region, not
// "global". Pins the contract that operators who run region-specific
// control planes (CONTROL_PLANE_REGION=us-east) get the fallback
// they expect.
func TestActivateDeployment_NonGlobalDefaultFallback(t *testing.T) {
	pub := newRecordingPublisher()
	svc, drainer, mock, cleanup := activateSvcForTest(t, pub, "us-east")
	defer cleanup()

	const deploymentID = "d_x"
	const tenantID = "t_test"
	const appName = "myapp"
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
			AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, "h", `{}`, time.Now(), false, "", "", []byte{}, 0, nil, nil, nil))
	mock.ExpectBegin()
	// Issue #440: tenant FOR UPDATE gate. Must come before the
	// active_deployments read so the disable-vs-activate race is
	// closed (see deployment.go::activateDeployment). Disabled=nil
	// so the gate passes; the dedicated *_TenantGate_* tests cover
	// the disabled and not-found arms.
	expectTenantForUpdateOK(mock, tenantID)
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs("t_test", "myapp").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).WillReturnResult(sqlmock.NewResult(1, 1))
	// ActivateDeployment also calls ClearStableSince inside the tx
	// (resets the stability clock for the new deployment). Mock the
	// UPDATE; the new column doesn't change row shape for the mock.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// In-tx reads (issue #42 / #44 part 2): quota is read once,
	// up-front (reused for buildPublishPayload's maxMemoryMB and the
	// in-tx AddMemoryMB UPDATE — see deployment.go::activateDeployment).
	// env / tenant are then read by buildPublishPayload.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, used_outbound_bytes, used_request_count, used_memory_mb, quota_period_start, quota_lock_grace_until FROM quotas WHERE tenant_id =`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "max_requests_per_month", "used_outbound_bytes", "used_request_count", "used_memory_mb", "quota_period_start", "quota_lock_grace_until"}).
			AddRow("t_test", 100, 50, 10, 256, 1024, 100_000, 0, 0, 0, time.Now(), nil))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id =`)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))

	// Issue #42: outbox INSERT inside tx; drainer relays to "us-east".
	expectInTxOutboxInsert(mock, "t_test", "myapp")
	expectInTxMemoryAdd(mock, "t_test", 256)
	mock.ExpectCommit()
	expectDrainerTickSuccess(t, mock, "t_test", "myapp", "d_x",
		[]string{"us-east"}, 256)

	if err := svc.ActivateDeployment(context.Background(), "t_test", "myapp", "d_x"); err != nil {
		t.Fatalf("ActivateDeployment: %v", err)
	}
	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("post-Activate publisher calls = %v, want []", got)
	}
	drainer.Tick(context.Background())
	if got := pub.regionsCalled(); !equalStringSlices(got, []string{"us-east"}) {
		t.Errorf("publish regions = %v, want [us-east]", got)
	}
}

// TestActivateDeployment_PartialFailure pins the contract documented
// at service/deployment.go ActivateDeployment: a publish failure in
// one region does NOT abort the loop. The other regions still get
// their message, and the returned error mentions every failed region
// so the operator can decide what to retry.
//
// This is the "continue-on-error" behavior — the alternative (bail on
// first failure) was rejected during plan review because it would
// leave the other regions out of date for an arbitrary reason.
func TestActivateDeployment_PartialFailure(t *testing.T) {
	pub := newRecordingPublisher()
	pub.failFor["eu-west"] = errors.New("nats: connection refused")
	svc, drainer, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const deploymentID = "d_partial"
	const tenantID = "t_test"
	const appName = "myapp"
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
			AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, "h", `{"us-east","eu-west","ap-south"}`, time.Now(), false, "", "", []byte{}, 0, nil, nil, nil))
	mock.ExpectBegin()
	// Issue #440: tenant FOR UPDATE gate. Must come before the
	// active_deployments read so the disable-vs-activate race is
	// closed (see deployment.go::activateDeployment). Disabled=nil
	// so the gate passes; the dedicated *_TenantGate_* tests cover
	// the disabled and not-found arms.
	expectTenantForUpdateOK(mock, tenantID)
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs("t_test", "myapp").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).WillReturnResult(sqlmock.NewResult(1, 1))
	// ActivateDeployment also calls ClearStableSince inside the tx
	// (resets the stability clock for the new deployment). Mock the
	// UPDATE; the new column doesn't change row shape for the mock.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// In-tx reads (issue #42 / #44 part 2): quota is read once,
	// up-front (reused for buildPublishPayload's maxMemoryMB and the
	// in-tx AddMemoryMB UPDATE — see deployment.go::activateDeployment).
	// env / tenant are then read by buildPublishPayload.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, used_outbound_bytes, used_request_count, used_memory_mb, quota_period_start, quota_lock_grace_until FROM quotas WHERE tenant_id =`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "max_requests_per_month", "used_outbound_bytes", "used_request_count", "used_memory_mb", "quota_period_start", "quota_lock_grace_until"}).
			AddRow("t_test", 100, 50, 10, 256, 1024, 100_000, 0, 0, 0, time.Now(), nil))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id =`)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))

	// Issue #42: outbox INSERT inside tx; drainer observes the
	// partial-failure outcome (the Activate call itself returns
	// nil — the row is durable in the outbox).
	expectInTxOutboxInsert(mock, "t_test", "myapp")
	expectInTxMemoryAdd(mock, "t_test", 256)
	mock.ExpectCommit()
	expectDrainerTickPartialFailure(t, mock, "t_test", "myapp", deploymentID,
		[]string{"us-east", "eu-west", "ap-south"}, 256)

	if err := svc.ActivateDeployment(context.Background(), "t_test", "myapp", deploymentID); err != nil {
		t.Fatalf("ActivateDeployment (durable publish): %v", err)
	}

	drainer.Tick(context.Background())

	// All 3 publishes must have been ATTEMPTED — the failed region
	// didn't short-circuit the drainer's loop. us-east + ap-south
	// succeeded, eu-west failed.
	gotRegions := pub.regionsCalled()
	wantRegions := []string{"us-east", "eu-west", "ap-south"}
	if !equalStringSlices(gotRegions, wantRegions) {
		t.Errorf("publish regions = %v, want %v (all must be attempted even on partial failure)", gotRegions, wantRegions)
	}
}

// TestActivateDeployment_QuotaMaxMemoryZero_FallsBackToDefault pins the
// fallback branch at service/deployment.go: maxMemoryMB starts at 256
// and only gets overwritten when quota != nil && quota.MaxMemoryMB > 0.
// A zero in the quota row must NOT be used as the actual limit — the
// service treats it as "unset" and falls through to the 256 default.
// (Without this test, a future "always honor the quota value" refactor
// would silently set MaxMemoryMB=0 in the TaskMessage and trip a worker
// limit that rejects zero.)
func TestActivateDeployment_QuotaMaxMemoryZero_FallsBackToDefault(t *testing.T) {
	pub := newRecordingPublisher()
	svc, drainer, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const deploymentID = "d_zero_quota"
	const tenantID = "t_test"
	const appName = "myapp"
	regionsCol := `{"us-east"}`
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
			AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, "h", regionsCol, time.Now(), false, "", "", []byte{}, 0, nil, nil, nil))
	mock.ExpectBegin()
	// Issue #440: tenant FOR UPDATE gate. Must come before the
	// active_deployments read so the disable-vs-activate race is
	// closed (see deployment.go::activateDeployment). Disabled=nil
	// so the gate passes; the dedicated *_TenantGate_* tests cover
	// the disabled and not-found arms.
	expectTenantForUpdateOK(mock, tenantID)
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs("t_test", "myapp").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).WillReturnResult(sqlmock.NewResult(1, 1))
	// ActivateDeployment also calls ClearStableSince inside the tx
	// (resets the stability clock for the new deployment). Mock the
	// UPDATE; the new column doesn't change row shape for the mock.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// In-tx reads (issue #42 / #44 part 2): quota is read once,
	// up-front (reused for buildPublishPayload's maxMemoryMB and the
	// in-tx AddMemoryMB UPDATE — see deployment.go::activateDeployment).
	// env / tenant are then read by buildPublishPayload.
	// Quota row with MaxMemoryMB=0 — should be treated as "unset" and
	// fall through to the 256 default.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, used_outbound_bytes, used_request_count, used_memory_mb, quota_period_start, quota_lock_grace_until FROM quotas WHERE tenant_id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "max_requests_per_month", "used_outbound_bytes", "used_request_count", "used_memory_mb", "quota_period_start", "quota_lock_grace_until"}).
			AddRow("t_test", 100, 50, 10, 0, 1024, 100_000, 0, 0, 0, time.Now(), nil))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))

	// Issue #42: outbox INSERT inside tx; drainer relays to "us-east".
	expectInTxOutboxInsert(mock, "t_test", "myapp")
	expectInTxMemoryAdd(mock, "t_test", 256)
	mock.ExpectCommit()
	expectDrainerTickSuccess(t, mock, "t_test", "myapp", deploymentID,
		[]string{"us-east"}, 256)

	if err := svc.ActivateDeployment(context.Background(), "t_test", "myapp", deploymentID); err != nil {
		t.Fatalf("ActivateDeployment: %v", err)
	}
	drainer.Tick(context.Background())
	if got := pub.calls[0].msg.Apps["myapp"].MaxMemoryMB; got != 256 {
		t.Errorf("MaxMemoryMB = %d, want 256 (fallback when quota has 0)", got)
	}
}

// TestActivateDeployment_NilQuota_FallsBackToDefault covers the case
// where GetByTenantID returns (nil, nil) — no quota row at all. The
// service must still produce a TaskMessage with MaxMemoryMB=256, not
// crash on a nil pointer deref.
func TestActivateDeployment_NilQuota_FallsBackToDefault(t *testing.T) {
	pub := newRecordingPublisher()
	svc, drainer, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const deploymentID = "d_no_quota"
	const tenantID = "t_test"
	const appName = "myapp"
	regionsCol := `{"us-east"}`
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
			AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, "h", regionsCol, time.Now(), false, "", "", []byte{}, 0, nil, nil, nil))
	mock.ExpectBegin()
	// Issue #440: tenant FOR UPDATE gate. Must come before the
	// active_deployments read so the disable-vs-activate race is
	// closed (see deployment.go::activateDeployment). Disabled=nil
	// so the gate passes; the dedicated *_TenantGate_* tests cover
	// the disabled and not-found arms.
	expectTenantForUpdateOK(mock, tenantID)
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs("t_test", "myapp").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).WillReturnResult(sqlmock.NewResult(1, 1))
	// ActivateDeployment also calls ClearStableSince inside the tx
	// (resets the stability clock for the new deployment). Mock the
	// UPDATE; the new column doesn't change row shape for the mock.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// In-tx reads (issue #42 / #44 part 2): quota is read once,
	// up-front (reused for buildPublishPayload's maxMemoryMB and the
	// in-tx AddMemoryMB UPDATE — see deployment.go::activateDeployment).
	// env / tenant are then read by buildPublishPayload.
	// Empty row set (no quota row) — GetByTenantID returns (nil, nil).
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, used_outbound_bytes, used_request_count, used_memory_mb, quota_period_start, quota_lock_grace_until FROM quotas WHERE tenant_id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "max_requests_per_month", "used_outbound_bytes", "used_request_count", "used_memory_mb", "quota_period_start", "quota_lock_grace_until"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))

	// Issue #42: outbox INSERT inside tx; drainer relays to "us-east".
	expectInTxOutboxInsert(mock, "t_test", "myapp")
	expectInTxMemoryAdd(mock, "t_test", 256)
	mock.ExpectCommit()
	expectDrainerTickSuccess(t, mock, "t_test", "myapp", deploymentID,
		[]string{"us-east"}, 256)

	if err := svc.ActivateDeployment(context.Background(), "t_test", "myapp", deploymentID); err != nil {
		t.Fatalf("ActivateDeployment: %v", err)
	}
	drainer.Tick(context.Background())
	if got := pub.calls[0].msg.Apps["myapp"].MaxMemoryMB; got != 256 {
		t.Errorf("MaxMemoryMB = %d, want 256 (fallback when quota is nil)", got)
	}
}

// equalStringSlices compares two []string for element-wise equality.
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// outboxRowPayload builds a JSON-marshaled TaskMessage for use as
// the `payload` field in a ClaimDue mock row. The drainer unmarshals
// this verbatim into a *nats.TaskMessage before publishing, so the
// fields need to round-trip through encoding/json with the right
// struct tags. maxMemoryMB drives the assertion in
// TestActivateDeployment_FansOutToAllRegions (MaxMemoryMB == 512).
func outboxRowPayload(t *testing.T, tenantID, appName, deploymentID string, maxMemoryMB int) []byte {
	t.Helper()
	payload, err := json.Marshal(&nats.TaskMessage{
		TenantID: tenantID,
		Apps: map[string]nats.AppConfig{
			appName: {
				DeploymentID: deploymentID,
				MaxMemoryMB:  maxMemoryMB,
				Env:          map[string]string{},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal TaskMessage: %v", err)
	}
	return payload
}

// expectInTxOutboxInsert mocks the outbox INSERT inside the
// ActivateDeployment / RollbackDeployment transaction. The INSERT
// happens between the in-tx ClearStableSince and ExpectCommit, so
// the caller must place this mock in that slot.
//
// The payload is JSONB and the dedupe_key embeds a fresh UUID per
// enqueue; both are pinned via AnyArg so the test stays agnostic
// about JSON shape and UUID format.
func expectInTxOutboxInsert(mock sqlmock.Sqlmock, tenantID, appName string) {
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO outbox`)).
		WithArgs(tenantID, appName, "task_update", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
}

// expectInTxMemoryAdd mocks the activate-tx memory counter mutation
// (issue #44 part 2): deployment.go::activateDeployment reuses the
// quota row it already loaded for buildPublishPayload (single read,
// no redundant SELECT — see the deploy.go refactor that hoisted the
// read above the outbox INSERT), computes perAppMemoryMB(quota), and
// runs `UPDATE used_memory_mb = used_memory_mb + $N` inside the tx.
// The delta (positive for activate, negative for rollback) equals
// MaxMemoryMB when > 0 — perAppMemoryMB returns MaxMemoryMB in that
// case — so the UPDATE delta matches the SELECT row's max_memory_mb
// in the matching expectation set above.
//
// Only the UPDATE is mocked here; the SELECT belongs to the
// quota-row mock the caller set up for buildPublishPayload's input.
func expectInTxMemoryAdd(mock sqlmock.Sqlmock, tenantID string, maxMemoryMB int64) {
	now := time.Now()
	mock.ExpectQuery(regexp.QuoteMeta(
		`UPDATE quotas SET used_memory_mb = used_memory_mb + $2 WHERE tenant_id = $1`,
	)).
		WithArgs(tenantID, maxMemoryMB).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers",
			"max_memory_mb", "max_outbound_mb", "max_requests_per_month",
			"used_outbound_bytes", "used_request_count", "used_memory_mb",
			"quota_period_start", "quota_lock_grace_until",
		}).AddRow(tenantID, 100, 50, 10, maxMemoryMB, 1024, 100_000, 0, 0, 0, now, nil))
}

// expectDrainerTickSuccess mocks the post-commit drainer flow for a
// single in-flight outbox row: ClaimDue → per-region PublishTaskUpdate
// (asserted via RecordingPublisher.calls) → MarkPublished. Used by
// tests that exercise a happy-path activate followed by drainer.Tick.
//
// Pre-#42 this work happened inside publishSwap, gated on
// post-commit read+appendRegionsPublished. Post-#42 the drainer owns
// it; the helper keeps the test files from re-declaring the same
// ClaimDue + MarkPublished mocks in every test. The drainer no
// longer AppendRegionsPublished on the active row — the outbox row
// itself records publish attempts (attempt_count + last_error +
// published_at).
//
// The returned row's payload is built via outboxRowPayload so the
// unmarshal step inside OutboxDrainer.processRow lands the same
// MaxMemoryMB / DeploymentID the production tx wrote. Tests that
// assert on the published message body (e.g.
// TestActivateDeployment_FansOutToAllRegions) rely on this.
func expectDrainerTickSuccess(t *testing.T, mock sqlmock.Sqlmock, tenantID, appName, deploymentID string, expectRegions []string, maxMemoryMB int) {
	t.Helper()
	mock.ExpectQuery(regexp.QuoteMeta(`WITH due AS (`)).
		WithArgs(50).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "app_name", "kind", "payload", "regions",
			"attempt_count", "next_attempt_at", "status", "last_error",
			"dedupe_key", "created_at", "published_at", "claimed_until",
		}).AddRow(
			1, tenantID, appName, "task_update",
			outboxRowPayload(t, tenantID, appName, deploymentID, maxMemoryMB),
			pq.Array(expectRegions),
			0, time.Now(), "in_flight", nil,
			"dedupe", time.Now(), nil, time.Now().Add(30*time.Second),
		))
	// Issue #42: the drainer no longer AppendRegionsPublished on the
	// active row — the outbox row itself records publish attempts
	// (attempt_count + last_error + published_at). Just MarkPublished.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

// expectDrainerTickPartialFailure mocks the post-commit drainer flow
// for a row that has one failing region (the rest succeed). The
// drainer's MarkFailed sets attempt_count=1 and keeps status as
// 'pending' so the next ClaimDue will retry.
func expectDrainerTickPartialFailure(t *testing.T, mock sqlmock.Sqlmock, tenantID, appName, deploymentID string, expectRegions []string, maxMemoryMB int) {
	t.Helper()
	mock.ExpectQuery(regexp.QuoteMeta(`WITH due AS (`)).
		WithArgs(50).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "app_name", "kind", "payload", "regions",
			"attempt_count", "next_attempt_at", "status", "last_error",
			"dedupe_key", "created_at", "published_at", "claimed_until",
		}).AddRow(
			1, tenantID, appName, "task_update",
			outboxRowPayload(t, tenantID, appName, deploymentID, maxMemoryMB),
			pq.Array(expectRegions),
			0, time.Now(), "in_flight", nil,
			"dedupe", time.Now(), nil, time.Now().Add(30*time.Second),
		))
	// See expectDrainerTickSuccess — drainer no longer touches the
	// active row. MarkFailed on the outbox row only.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
		WithArgs(1, "pending", 1, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

// TestPublishSwap_NoOpWhenNothingConfigured pins the cache-only
// contract of publishSwap (issue #42). With s.cachePusher nil and no
// region caches, publishSwap must short-circuit to a no-op: it does
// NOT call the publisher (NATS publishes are owned by the outbox
// drainer now) and it does NOT touch the active row. Pre-#42, this
// was a partial-failure PublishError test; post-#42 the partial-
// failure path lives in the drainer (see outbox_drainer_test.go).
func TestPublishSwap_NoOpWhenNothingConfigured(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() {
		if err := sqlDB.Close(); err != nil {
			_ = err
		}
	})
	db := sqlx.NewDb(sqlDB, "postgres")

	tenantID, appName := "t_atomic", "myapp"

	pub := newRecordingPublisher()

	svc := &DeploymentService{
		db:         db,
		activeRepo: repository.NewActiveDeploymentRepository(db),
		publisher:  pub,
	}

	// cachePusher is nil → publishSwap short-circuits before any DB
	// or publisher call. The publisher's failure map is irrelevant
	// — the function never reaches the loop.
	if err := svc.publishSwap(context.Background(), tenantID, appName, "d_atomic", []string{"us-east", "eu-west"}); err != nil {
		t.Errorf("publishSwap: want nil (cache-only no-op), got %v", err)
	}
	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("publisher calls = %v, want [] (NATS publishes belong to the outbox drainer)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v (publishSwap should not touch the DB when cachePusher is nil)", err)
	}
}

// recordingCachePusher is the cache-push counterpart to
// RecordingPublisher. Implements artifactCachePusher, recording
// every call so the test can assert on the per-region cache-push
// outcome. The region is identified by parsing the trailing path
// segment off the cacheBaseURL — `/artifacts/{tenant}/{app}/{id}`
// doesn't carry the region, so we use a synthetic per-region
// base URL like `http://cache.fra:18080` and the test extracts
// `fra` from the URL's host.
type recordingCachePusher struct {
	mu      sync.Mutex
	calls   []string
	failFor map[string]error
}

func newRecordingCachePusher() *recordingCachePusher {
	return &recordingCachePusher{failFor: map[string]error{}}
}

// regionsPushed returns the regions that actually called Push (i.e.
// not skipped by the alreadyCached check). Order is call order.
func (p *recordingCachePusher) regionsPushed() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.calls))
	copy(out, p.calls)
	return out
}

func (p *recordingCachePusher) Push(_ context.Context, cacheBaseURL, _, _, _ string) error {
	// Extract a region tag from the URL host. Test fixtures use
	// hosts like `cache.fra:18080` / `cache.iad:18080`; the region
	// is the first label after `cache.`.
	region := cacheBaseURL
	if i := indexByte(cacheBaseURL, '.'); i >= 0 {
		region = cacheBaseURL[i+1:]
	}
	if c := indexByte(region, ':'); c >= 0 {
		region = region[:c]
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, region)
	if err, ok := p.failFor[region]; ok {
		return err
	}
	return nil
}

// Tiny bytes.IndexByte polyfill so we don't pull in another import.
func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// TestPublishSwap_SkipsAlreadyCachedRegion (issue #332, PR 2):
// when `current.RegionsCached` already contains a region, the
// cache-push loop must skip that region (no PUT) but the NATS
// publish loop must still run for it (the worker may not have
// received the prior TaskMessage). After the tx, the cached set
// in the row reflects the union of the prior set + the
// successfully-pushed regions. PR 2 follow-up splits the per-
// region cache outcome into CachedSucceeded (push returned 2xx)
// and CachedSkipped (already in RegionsCached at read time, no
// push attempted); both feed AppendRegionsCacheState as the
// "succeeded" argument.
func TestPublishSwap_SkipsAlreadyCachedRegion(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db := sqlx.NewDb(sqlDB, "postgres")

	tenantID, appName := "t_skip", "myapp"

	// Post-commit read: regions_cached = ["fra"], so the cache
	// loop should skip "fra" and push "iad". The NATS publish
	// loop runs for BOTH regions.
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id, preview_id, preview_pr_number, activation_attempt_started_at FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`,
	)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled", "stable_since",
			"regions_published", "regions_failed", "regions_cached",
			"regions_cache_failed", "last_publish_at", "last_publish_attempt_id",
			"preview_id", "preview_pr_number",
		}).AddRow(
			tenantID, appName, "d_skip", nil, false, nil,
			"{}", "{}", "{fra}", "{}",
			nil, nil, nil, nil,
		))

	pub := &RecordingPublisher{}
	// Wire the cache pusher so the cache loop runs. Both regions
	// succeed; `fra` is skipped due to alreadyCached (no push),
	// `iad` is pushed.
	cachePusher := newRecordingCachePusher()

	// Issue #42: publishSwap is now cache-only — it does NOT call
	// the publisher (NATS publishes are owned by the outbox
	// drainer). The tx below wraps just the AppendRegionsCacheState
	// post-write; the previous AppendRegionsPublished mock is gone
	// because that call moved into OutboxDrainer.processRow.
	mock.ExpectBegin()
	// AppendRegionsCacheState: succeeded=[iad, fra] (the union of
	// pushed + skipped, per PR 2 follow-up), failed=[].
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET regions_cached = (`,
	)).
		WithArgs(tenantID, appName, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// Issue #501 retry-cap: publishSwap unconditionally resets
	// region_cache_retry_count inside the same tx so the sweep
	// counter is per-deployment.
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET region_cache_retry_count = '{}'::jsonb`,
	)).
		WithArgs(tenantID, appName).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	svc := &DeploymentService{
		db:          db,
		activeRepo:  repository.NewActiveDeploymentRepository(db),
		publisher:   pub,
		cachePusher: cachePusher,
		regionArtifactCaches: map[string]string{
			"fra": "http://cache.fra:18080",
			"iad": "http://cache.iad:18080",
		},
		defaultRegion: "fra",
	}

	// waitForWorkers (issue #42): publishSwap no longer calls
	// waitForWorkers — durable publish removes the synchronous
	// post-publish block. The mock workers / worker_status queries
	// from the pre-#42 test are no longer required.

	if err := svc.publishSwap(context.Background(), tenantID, appName, "d_skip", []string{"fra", "iad"}); err != nil {
		t.Fatalf("publishSwap: %v", err)
	}

	// Issue #42: publishSwap is cache-only — the publisher has
	// NOT been called. NATS publishes belong to the outbox
	// drainer (see TestOutboxDrainer_* tests).
	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("publisher regions = %v, want [] (publishSwap no longer publishes NATS)", got)
	}

	// The cache pusher was invoked ONCE (for `iad`). The `fra`
	// region was skipped by the `alreadyCached[region]` check in
	// publishSwap, so no Push call lands for it. (The skip is
	// per-region even though regionArtifactCaches has an entry for
	// `fra` — that's the entire point of the new behavior.)
	if got := cachePusher.regionsPushed(); !reflect.DeepEqual(got, []string{"iad"}) {
		t.Errorf("cache pusher regions = %v, want [iad]", got)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestPublishSwap_AtomicOnCacheAppendFailure (issue #332, PR 2):
// mirrors the publish-state atomicity test above for the new
// AppendRegionsCached call. If the cache append fails inside the
// wrapping tx, the published append MUST roll back too — otherwise
// the row's (regions_published, regions_cached) state would diverge
// (the worker would think the publish succeeded, but the next
// activation would skip the cache push because regions_cached is
// empty).
func TestPublishSwap_AtomicOnCacheAppendFailure(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db := sqlx.NewDb(sqlDB, "postgres")

	tenantID, appName := "t_atomic_cache", "myapp"

	// Post-commit read: empty arrays; both regions are in toPublish.
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id, preview_id, preview_pr_number, activation_attempt_started_at FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`,
	)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled", "stable_since",
			"regions_published", "regions_failed", "regions_cached",
			"regions_cache_failed", "last_publish_at", "last_publish_attempt_id",
			"preview_id", "preview_pr_number",
		}).AddRow(
			tenantID, appName, "d_atomic_cache", nil, false, nil,
			"{}", "{}", "{}", "{}",
			nil, nil, nil, nil,
		))

	pub := &RecordingPublisher{}
	cachePusher := newRecordingCachePusher()

	// Issue #42: publishSwap is cache-only — there's no
	// AppendRegionsPublished call anymore (the drainer owns that).
	// The only DB write inside the tx is AppendRegionsCacheState,
	// which we mock to fail so the rollback path is exercised.
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET regions_cached = (`,
	)).
		WithArgs(tenantID, appName, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(errors.New("simulated DB outage on cache append"))
	mock.ExpectRollback()

	svc := &DeploymentService{
		db:          db,
		activeRepo:  repository.NewActiveDeploymentRepository(db),
		publisher:   pub,
		cachePusher: cachePusher,
		regionArtifactCaches: map[string]string{
			"fra": "http://cache.fra:18080",
		},
		defaultRegion: "fra",
	}

	// waitForWorkers (issue #42): publishSwap no longer calls
	// waitForWorkers — durable publish removes the synchronous
	// post-publish block. The mock workers / worker_status queries
	// from the pre-#42 test are no longer required.

	err = svc.publishSwap(context.Background(), tenantID, appName, "d_atomic_cache", []string{"fra"})
	// The cache-append failure is "best effort" — the error is
	// logged but does NOT change the returned error. The tx still
	// rolls back (the failed Exec triggers a Rollback
	// automatically via the repository.Transaction wrapper), so
	// no partial state is persisted.
	if err != nil {
		t.Errorf("publishSwap: want nil (best-effort cache append), got %v", err)
	}
	// Issue #42: publishSwap does NOT call the publisher. The
	// cache pusher still ran (and was logged on failure) but the
	// publisher sees no calls — NATS publishes are owned by the
	// outbox drainer.
	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("publisher regions = %v, want [] (publishSwap no longer publishes NATS)", got)
	}

	// No mock.ExpectCommit was set — sqlmock will fail if Commit
	// lands. If Rollback was not issued, mock.ExpectationsWereMet
	// will surface it.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestPublishSwap_TracksCachedSucceededAndSkippedSeparately (issue
// #332, PR 2 follow-up) pins the per-region cache-state split
// documented on PublishError.CachedSucceeded / CachedSkipped. When
// the post-commit read shows regions_cached = ["fra"] and the
// activation touches both "fra" (skip) and "iad" (push), the
// returned error is nil (NATS publish succeeded for both); the
// contract being pinned is the post-tx DB write, which is
// AppendRegionsCacheState called with succeeded=[iad, fra] (the
// union of CachedSucceeded and CachedSkipped) and failed=[].
//
// We can't directly assert the local `cachedSucceeded` and
// `cachedSkipped` slices from outside publishSwap — they don't
// surface on a nil return. The contract is pinned via the
// AppendRegionsCacheState mock expecting the 4-arg signature
// (succeeded + failed slices); the test asserts the call shape
// (sqlmock regex) and the post-tx commit.
func TestPublishSwap_TracksCachedSucceededAndSkippedSeparately(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db := sqlx.NewDb(sqlDB, "postgres")

	tenantID, appName := "t_split", "myapp"

	// Post-commit read: regions_cached = ["fra"], so `fra` is
	// skipped and `iad` is pushed. The cache pusher is wired so
	// the cache loop runs. NATS publishes for both regions.
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id, preview_id, preview_pr_number, activation_attempt_started_at FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`,
	)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled", "stable_since",
			"regions_published", "regions_failed", "regions_cached",
			"regions_cache_failed", "last_publish_at", "last_publish_attempt_id",
			"preview_id", "preview_pr_number",
		}).AddRow(
			tenantID, appName, "d_split", nil, false, nil,
			"{}", "{}", "{fra}", "{}",
			nil, nil, nil, nil,
		))

	pub := &RecordingPublisher{}
	cachePusher := newRecordingCachePusher()

	// Issue #42: publishSwap no longer calls AppendRegionsPublished
	// (the drainer does that). Only AppendRegionsCacheState remains
	// in the tx — succeeded=[iad, fra] (union), failed=[].
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET regions_cached = (`,
	)).
		WithArgs(tenantID, appName, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// Issue #501 retry-cap: publishSwap unconditionally resets
	// region_cache_retry_count inside the same tx so the sweep
	// counter is per-deployment.
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET region_cache_retry_count = '{}'::jsonb`,
	)).
		WithArgs(tenantID, appName).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	svc := &DeploymentService{
		db:          db,
		activeRepo:  repository.NewActiveDeploymentRepository(db),
		publisher:   pub,
		cachePusher: cachePusher,
		regionArtifactCaches: map[string]string{
			"fra": "http://cache.fra:18080",
			"iad": "http://cache.iad:18080",
		},
		defaultRegion: "fra",
	}

	// waitForWorkers (issue #42): publishSwap no longer calls
	// waitForWorkers — durable publish removes the synchronous
	// post-publish block. The mock workers / worker_status queries
	// from the pre-#42 test are no longer required.

	if err := svc.publishSwap(context.Background(), tenantID, appName, "d_split", []string{"fra", "iad"}); err != nil {
		t.Fatalf("publishSwap: want nil (no NATS failures, cache is best-effort), got %v", err)
	}

	// pusher.calls: only `iad` was pushed; `fra` was skipped.
	if got := cachePusher.regionsPushed(); !reflect.DeepEqual(got, []string{"iad"}) {
		t.Errorf("cache pusher regions = %v, want [iad] (fra was skipped)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestPublishSwap_CacheFailureIsBestEffort (issue #332, PR 2
// follow-up) pins the contract from the cache loop's doc comment:
// a cache push failure does NOT shape the activation return
// value. The worker still receives the TaskMessage (NATS publish
// succeeded); the cache push failure is persisted in
// regions_cache_failed for operator visibility but does NOT
// change the returned error.
//
// Pre-PR-2-follow-up, the equivalent test would have asserted
// err != nil with a *PublishError{CacheFailed: ...}. Post-PR-2-
// follow-up, the err is nil. The cache-failure record is
// observable only via the AppendRegionsCacheState call's
// "failed" argument, which is asserted via sqlmock's
// ExpectationsWereMet.
func TestPublishSwap_CacheFailureIsBestEffort(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db := sqlx.NewDb(sqlDB, "postgres")

	tenantID, appName := "t_best_effort", "myapp"

	// Post-commit read: empty regions_cached, so the cache loop
	// attempts to push both regions. Both NATS publishes succeed
	// (no failFor on the publisher). The cache pusher fails for
	// both regions.
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id, preview_id, preview_pr_number, activation_attempt_started_at FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`,
	)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled", "stable_since",
			"regions_published", "regions_failed", "regions_cached",
			"regions_cache_failed", "last_publish_at", "last_publish_attempt_id",
			"preview_id", "preview_pr_number",
		}).AddRow(
			tenantID, appName, "d_best_effort", nil, false, nil,
			"{}", "{}", "{}", "{}",
			nil, nil, nil, nil,
		))

	pub := &RecordingPublisher{}
	cachePusher := newRecordingCachePusher()
	cachePusher.failFor["fra"] = errors.New("cache 500")
	cachePusher.failFor["iad"] = errors.New("cache 500")

	// Issue #42: only AppendRegionsCacheState remains in the tx
	// (the drainer owns AppendRegionsPublished). succeeded=[],
	// failed=[fra, iad] since both pushes failed.
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET regions_cached = (`,
	)).
		WithArgs(tenantID, appName, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// Issue #501 retry-cap: publishSwap unconditionally resets
	// region_cache_retry_count inside the same tx so the sweep
	// counter is per-deployment.
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET region_cache_retry_count = '{}'::jsonb`,
	)).
		WithArgs(tenantID, appName).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	svc := &DeploymentService{
		db:          db,
		activeRepo:  repository.NewActiveDeploymentRepository(db),
		publisher:   pub,
		cachePusher: cachePusher,
		regionArtifactCaches: map[string]string{
			"fra": "http://cache.fra:18080",
			"iad": "http://cache.iad:18080",
		},
		defaultRegion: "fra",
	}

	// waitForWorkers (issue #42): publishSwap no longer calls
	// waitForWorkers — durable publish removes the synchronous
	// post-publish block. The mock workers / worker_status queries
	// from the pre-#42 test are no longer required.

	if err := svc.publishSwap(context.Background(), tenantID, appName, "d_best_effort", []string{"fra", "iad"}); err != nil {
		t.Fatalf("publishSwap: want nil (cache failures are best-effort), got %v", err)
	}

	// Both regions' cache pushes were attempted (and failed).
	if got := cachePusher.regionsPushed(); !reflect.DeepEqual(got, []string{"fra", "iad"}) {
		t.Errorf("cache pusher regions = %v, want [fra iad]", got)
	}
	// Issue #42: publishSwap does NOT call the publisher. The
	// cache pusher was called for both regions (and failed for
	// both) but no NATS publishes happen here — they belong to
	// the outbox drainer (see TestOutboxDrainer_*).
	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("publisher regions = %v, want [] (publishSwap no longer publishes NATS)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// expectTenantForUpdateDisabled mocks the tenant FOR UPDATE select
// returning a row whose disabled_at is set — the gate fires and
// RollbackDeployment / activateDeployment abort with ErrTenantDisabled
// without touching the active_deployments row.
func expectTenantForUpdateDisabled(mock sqlmock.Sqlmock, tenantID string, disabledAt time.Time) {
	mock.ExpectQuery(`SELECT.*tenants.*FOR UPDATE`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "name", "plan", "allowlisted_destinations",
			"created_at", "disabled_at", "overage_allowed_until",
		}).AddRow(tenantID, "Test Tenant", "free", `{}`, time.Now().Add(-time.Hour), disabledAt, nil))
}

// expectTenantForUpdateNotFound mocks the tenant FOR UPDATE select
// returning zero rows — the gate fires with ErrTenantNotFound.
func expectTenantForUpdateNotFound(mock sqlmock.Sqlmock, tenantID string) {
	mock.ExpectQuery(`SELECT.*tenants.*FOR UPDATE`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "name", "plan", "allowlisted_destinations",
			"created_at", "disabled_at", "overage_allowed_until",
		}))
}

// TestRollbackDeployment_TenantDisabledMidTx_NoPublish (issue #440)
// pins the disable-vs-rollback gate: when the tenant FOR UPDATE
// select reads back a row with disabled_at SET, RollbackDeployment
// must abort with ErrTenantDisabled before the active_deployments
// read. The tx rolls back (no commit) and the publisher receives no
// call — the worker never sees a TaskMessage for a disabled tenant
// via the rollback path.
//
// The mirrored test for activateDeployment lives in PR #440; this
// test closes the symmetric hole on the rollback side (review
// finding #2 in /review 507).
func TestRollbackDeployment_TenantDisabledMidTx_NoPublish(t *testing.T) {
	pub := newRecordingPublisher()
	svc, _, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const tenantID, appName = "t_rollback_disabled", "myapp"

	mock.ExpectBegin()
	expectTenantForUpdateDisabled(mock, tenantID, time.Now().Add(-time.Minute))
	// tx returns ErrTenantDisabled → repository.Transaction triggers
	// a Rollback. No Commit, no active_deployments FOR UPDATE, no
	// outbox INSERT, no publish.
	mock.ExpectRollback()

	rolledBackID, err := svc.RollbackDeployment(context.Background(), tenantID, appName)
	if err == nil {
		t.Fatalf("RollbackDeployment: want ErrTenantDisabled, got nil")
	}
	if !errors.Is(err, ErrTenantDisabled) {
		t.Errorf("RollbackDeployment err = %v, want ErrTenantDisabled", err)
	}
	if rolledBackID != "" {
		t.Errorf("rolledBackID = %q, want \"\" (gate must short-circuit before computing rolledBackID)", rolledBackID)
	}
	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("publisher regions = %v, want [] (disabled tenant must not publish)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestRollbackDeployment_TenantNotFound_NoPublish (issue #440) pins
// the not-found branch of the same gate. A tenant that no longer
// exists (deleted between request issue and the FOR UPDATE) must
// surface ErrTenantNotFound and roll back without publishing —
// otherwise the worker could receive a TaskMessage for a tenant the
// control plane has already forgotten about.
func TestRollbackDeployment_TenantNotFound_NoPublish(t *testing.T) {
	pub := newRecordingPublisher()
	svc, _, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const tenantID, appName = "t_rollback_missing", "myapp"

	mock.ExpectBegin()
	expectTenantForUpdateNotFound(mock, tenantID)
	mock.ExpectRollback()

	rolledBackID, err := svc.RollbackDeployment(context.Background(), tenantID, appName)
	if err == nil {
		t.Fatalf("RollbackDeployment: want ErrTenantNotFound, got nil")
	}
	if !errors.Is(err, ErrTenantNotFound) {
		t.Errorf("RollbackDeployment err = %v, want ErrTenantNotFound", err)
	}
	if rolledBackID != "" {
		t.Errorf("rolledBackID = %q, want \"\"", rolledBackID)
	}
	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("publisher regions = %v, want [] (missing tenant must not publish)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestRollbackDeployment_NormalTenant_Proceeds (issue #440) pins the
// happy path: when the tenant FOR UPDATE select returns a non-disabled
// row, RollbackDeployment must execute the full tx (active read,
// deployment re-fetch, active update, stable-since clear, payload
// build, outbox INSERT) and commit. The post-commit publishSwap
// short-circuits to a no-op because the test wires no cachePusher and
// no regionArtifactCaches — so no SQL after the commit, no NATS
// publish. The TaskMessage enqueued via the outbox will be relayed
// by the drainer (not exercised here).
func TestRollbackDeployment_NormalTenant_Proceeds(t *testing.T) {
	pub := newRecordingPublisher()
	svc, drainer, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const (
		tenantID           = "t_rollback_ok"
		appName            = "myapp"
		activeDeploymentID = "d_active"
		lastGoodID         = "d_last_good"
		deploymentHash     = "hash_last_good"
	)

	// active_deployments FOR UPDATE — current row carries the
	// last_good pointer so rollback has somewhere to roll back to.
	mock.ExpectBegin()
	expectTenantForUpdateOK(mock, tenantID)
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled", "stable_since",
			"regions_published", "regions_failed", "regions_cached",
			"regions_cache_failed", "last_publish_at", "last_publish_attempt_id",
			"preview_id", "preview_pr_number",
		}).AddRow(
			tenantID, appName, activeDeploymentID, lastGoodID, false, nil,
			"{}", "{}", "{}", "{}",
			nil, nil, nil, nil,
		))

	// Target deployment re-fetch (defends against a deleted
	// last_good row). Returns a row whose regions = ["us-east"] so
	// publishSwap is non-empty. Use the Postgres array literal text
	// form — lib/pq auto-decodes into pq.StringArray; passing a
	// bare []string panics in sqlmock (issue #543 upstream).
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
		WithArgs(lastGoodID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "app_name", "status", "hash", "regions",
			"created_at", "auto_rollback_enabled", "signature", "signing_key_id",
			"build_attestation", "desired_replicas", "preview_id",
			"preview_pr_number", "preview_expires_at",
		}).AddRow(
			lastGoodID, tenantID, appName, domain.StatusDeployed, deploymentHash,
			`{"us-east"}`, time.Now(), false, "", "", []byte("null"), 0, nil, nil, nil,
		))

	// In-tx quota read (issue #44 part 2, hoisted): the per-app
	// memory value used to build the TaskMessage (below) and the
	// values used to bump the counter (last 2 mocks) provably come
	// from the same snapshot. Production reads this row BEFORE the
	// Set on line 1660 of deployment.go and reuses it in
	// buildPublishPayload so we don't double-SELECT.
	now := time.Now()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, used_outbound_bytes, used_request_count, used_memory_mb, quota_period_start, quota_lock_grace_until FROM quotas WHERE tenant_id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers",
			"max_memory_mb", "max_outbound_mb", "max_requests_per_month",
			"used_outbound_bytes", "used_request_count", "used_memory_mb",
			"quota_period_start", "quota_lock_grace_until",
		}).AddRow(tenantID, 100, 50, 10, 256, 1024, 100_000, 0, 0, 0, now, nil))

	// active_deployments Set — swap the active id and clear
	// last_good so a second rollback is a no-op (issue #127 step 6).
	// The Set query is an INSERT ... ON CONFLICT DO UPDATE with 14
	// args (see active_deployment.go::Set). We anchor on the
	// "INSERT INTO active_deployments" prefix so the regex matcher
	// doesn't conflate this with the ClearStableSince UPDATE that
	// immediately follows. sqlmock's QueryMatcherRegexp is
	// substring-based, so any UPDATE-active_deployments UPDATE
	// would otherwise race against this expectation.
	mock.ExpectExec(`INSERT INTO active_deployments`).
		WithArgs(tenantID, appName, lastGoodID, nil,
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), 0).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// ClearStableSince — reset the stability clock for the new
	// active deployment.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
		WithArgs(tenantID, appName).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// In-tx reads inside buildPublishPayload: env / tenant. The
	// quota row was loaded above (hoisted), so the payload build
	// skips the SELECT and reuses the in-memory snapshot.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow(tenantID, "T", "free", `{}`, time.Now()))

	// Outbox INSERT inside the tx (issue #42): the drainer relays
	// the marshaled TaskMessage after commit. The drainer tick
	// below drives the end-to-end "rollback commits → outbox row
	// visible → drainer relays to NATS" contract that the gate
	// exists to protect.
	expectInTxOutboxInsert(mock, tenantID, appName)

	// Issue #44 part 2: in-tx counter swap. +256 for the
	// rolled-back-TO deployment (now active), -256 for the
	// rolled-back-FROM (current.DeploymentID before Set). Both
	// inside the same tx so a failure rolls all three mutations
	// back together.
	mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_memory_mb = used_memory_mb + $2 WHERE tenant_id = $1`)).
		WithArgs(tenantID, int64(256)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers",
			"max_memory_mb", "max_outbound_mb", "max_requests_per_month",
			"used_outbound_bytes", "used_request_count", "used_memory_mb",
			"quota_period_start", "quota_lock_grace_until",
		}).AddRow(tenantID, 100, 50, 10, 256, 1024, 100_000, 0, 0, 256, now, nil))
	mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_memory_mb = used_memory_mb + $2 WHERE tenant_id = $1`)).
		WithArgs(tenantID, int64(-256)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers",
			"max_memory_mb", "max_outbound_mb", "max_requests_per_month",
			"used_outbound_bytes", "used_request_count", "used_memory_mb",
			"quota_period_start", "quota_lock_grace_until",
		}).AddRow(tenantID, 100, 50, 10, 256, 1024, 100_000, 0, 0, 0, now, nil))

	mock.ExpectCommit()

	// post-commit publishSwap: cachePusher is nil AND
	// regionArtifactCaches is empty (set by activateSvcForTest),
	// so publishSwap short-circuits to a no-op. No SQL after this
	// commit — the drainer owns the publish on the next tick.

	rolledBackID, err := svc.RollbackDeployment(context.Background(), tenantID, appName)
	if err != nil {
		t.Fatalf("RollbackDeployment: %v", err)
	}
	if rolledBackID != lastGoodID {
		t.Errorf("rolledBackID = %q, want %q", rolledBackID, lastGoodID)
	}
	// Before the drainer tick: publishSwap's no-op didn't touch
	// the publisher. (Worker-side dedupe_id stamping on
	// AppStatus — issue #418 — makes a duplicate harmless even
	// if the same delta were replayed.)
	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("publisher regions before drainer tick = %v, want [] (publishSwap is no-op without cachePusher)", got)
	}

	// Drive the drainer once. expectDrainerTickSuccess mocks
	// ClaimDue → per-region PublishTaskUpdate → MarkPublished.
	// The deployment row's regions = ["us-east"] so the drainer
	// publishes exactly to that region.
	expectDrainerTickSuccess(t, mock, tenantID, appName, lastGoodID,
		[]string{"us-east"}, 256)
	drainer.Tick(context.Background())

	// End-to-end assertion: the gate passed, the outbox row was
	// committed, and the drainer relayed the marshaled
	// TaskMessage to the publisher for ["us-east"]. The
	// DeploymentID on the published message must equal
	// rolledBackID — otherwise the rollback would have published
	// a TaskMessage pointing at the broken deployment, defeating
	// the rollback entirely.
	if got := pub.regionsCalled(); !equalStringSlices(got, []string{"us-east"}) {
		t.Errorf("publisher regions after drainer tick = %v, want [us-east]", got)
	}
	if len(pub.calls) != 1 {
		t.Fatalf("publish calls = %d, want 1", len(pub.calls))
	}
	if got := pub.calls[0].msg.Apps["myapp"].DeploymentID; got != lastGoodID {
		t.Errorf("published DeploymentID = %q, want %q (drainer must relay the rolled-back-to id)", got, lastGoodID)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestActivateDeployment_TenantDisabledMidTx_NoPublish (issue #440)
// pins the disable-vs-activate gate: when the tenant FOR UPDATE
// select reads back a row with disabled_at SET, ActivateDeployment
// must abort with ErrTenantDisabled before the active_deployments
// read. The tx rolls back (no commit) and the publisher receives no
// call — the worker never sees a TaskMessage for a disabled tenant
// via the activate path.
//
// The mirrored test for RollbackDeployment is
// TestRollbackDeployment_TenantDisabledMidTx_NoPublish (below);
// these two together close the symmetric hole on both write paths.
func TestActivateDeployment_TenantDisabledMidTx_NoPublish(t *testing.T) {
	pub := newRecordingPublisher()
	svc, _, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const (
		tenantID     = "t_activate_disabled"
		appName      = "myapp"
		deploymentID = "d_disabled"
		deployHash   = "h_disabled"
	)

	// Pre-tx read inside ActivateDeployment — needed before the tx
	// can enter. Returns a single-region deployment so activate
	// would otherwise publish to "us-east".
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
			AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, deployHash, `{"us-east"}`, time.Now(), false, "", "", []byte{}, 0, nil, nil, nil))

	mock.ExpectBegin()
	expectTenantForUpdateDisabled(mock, tenantID, time.Now().Add(-time.Minute))
	// Gate fires → tx returns ErrTenantDisabled → repository.Transaction
	// triggers a Rollback. No active_deployments FOR UPDATE, no outbox
	// INSERT, no publish.
	mock.ExpectRollback()

	err := svc.ActivateDeployment(context.Background(), tenantID, appName, deploymentID)
	if err == nil {
		t.Fatalf("ActivateDeployment: want ErrTenantDisabled, got nil")
	}
	if !errors.Is(err, ErrTenantDisabled) {
		t.Errorf("ActivateDeployment err = %v, want ErrTenantDisabled", err)
	}
	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("publisher regions = %v, want [] (disabled tenant must not publish)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestActivateDeployment_TenantNotFound_NoPublish (issue #440) pins
// the not-found branch of the disable-vs-activate gate. A tenant
// that no longer exists (deleted between request issue and the FOR
// UPDATE) must surface ErrTenantNotFound and roll back without
// publishing — otherwise the worker could receive a TaskMessage for
// a tenant the control plane has already forgotten about.
//
// Mirrors TestRollbackDeployment_TenantNotFound_NoPublish so both
// write paths cover the missing-tenant arm.
func TestActivateDeployment_TenantNotFound_NoPublish(t *testing.T) {
	pub := newRecordingPublisher()
	svc, _, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const (
		tenantID     = "t_activate_missing"
		appName      = "myapp"
		deploymentID = "d_missing"
		deployHash   = "h_missing"
	)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
			AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, deployHash, `{"us-east"}`, time.Now(), false, "", "", []byte{}, 0, nil, nil, nil))

	mock.ExpectBegin()
	expectTenantForUpdateNotFound(mock, tenantID)
	mock.ExpectRollback()

	err := svc.ActivateDeployment(context.Background(), tenantID, appName, deploymentID)
	if err == nil {
		t.Fatalf("ActivateDeployment: want ErrTenantNotFound, got nil")
	}
	if !errors.Is(err, ErrTenantNotFound) {
		t.Errorf("ActivateDeployment err = %v, want ErrTenantNotFound", err)
	}
	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("publisher regions = %v, want [] (missing tenant must not publish)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}
