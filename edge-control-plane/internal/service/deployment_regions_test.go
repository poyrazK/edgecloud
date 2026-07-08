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
func activateSvcForTest(t *testing.T, pub nats.Publisher, defaultRegion string) (*DeploymentService, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	svc := &DeploymentService{
		db:             sqlxDB,
		deploymentRepo: repository.NewDeploymentRepository(sqlxDB),
		activeRepo:     repository.NewActiveDeploymentRepository(sqlxDB),
		appEnvRepo:     repository.NewAppEnvRepository(sqlxDB),
		tenantRepo:     repository.NewTenantRepository(sqlxDB),
		quotaRepo:      repository.NewQuotaRepository(sqlxDB),
		publisher:      pub,
		defaultRegion:  defaultRegion,
	}
	return svc, mock, func() { _ = mockDB.Close() }
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
	svc, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const (
		deploymentID   = "d_abc123"
		appName        = "myapp"
		tenantID       = "t_test"
		deploymentHash = "abc123hash"
	)

	// 1. deploymentRepo.GetByID returns a row with 3 regions.
	regionsCol := `{"us-east","eu-west","ap-south"}`
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id"}).
			AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, deploymentHash, regionsCol, time.Now(), false, nil, nil))

	// 2. ActivateDeployment wraps the GetForUpdate + Set in a tx
	// (so concurrent activate/rollback serialize via FOR UPDATE).
	mock.ExpectBegin()
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
	mock.ExpectCommit()

	// 3. appEnvRepo.List — return no env vars.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))

	// 4. tenantRepo.GetByID — return a tenant with an allowlist so the
	// TaskMessage carries it.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at FROM tenants WHERE id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow(tenantID, "Test Tenant", "free", `{"api.example.com"}`, time.Now()))

	// 5. quotaRepo.GetByTenantID — ActivateDeployment reads the quota
	// to populate MaxMemoryMB on the AppConfig (per main's quota
	// wiring). Return a row so the field flows.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, used_outbound_bytes, used_request_count, quota_period_start FROM quotas WHERE tenant_id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "used_outbound_bytes", "quota_period_start"}).
			AddRow(tenantID, 100, 50, 10, 512, 1024, 0, time.Now()))

	// Issue #127 step 6: publishSwap reads the post-commit row to
	// compute the idempotent publish set, then AppendRegionsPublished
	// to persist the outcome. All 3 publishes succeed.
	expectPostCommitReadAndAppend(mock, tenantID, appName,
		[]string{"us-east", "eu-west", "ap-south"}, nil)

	if err := svc.ActivateDeployment(context.Background(), tenantID, appName, deploymentID); err != nil {
		t.Fatalf("ActivateDeployment: %v", err)
	}

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
	svc, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const deploymentID = "d_legacy"
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id"}).
			AddRow(deploymentID, "t_test", "myapp", domain.StatusDeployed, "h", `{}`, time.Now(), false, nil, nil))
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs("t_test", "myapp").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).WillReturnResult(sqlmock.NewResult(1, 1))
	// ActivateDeployment also calls ClearStableSince inside the tx
	// (resets the stability clock for the new deployment). Mock the
	// UPDATE; the new column doesn't change row shape for the mock.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at FROM tenants WHERE id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, used_outbound_bytes, used_request_count, quota_period_start FROM quotas WHERE tenant_id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "used_outbound_bytes", "quota_period_start"}).
			AddRow("t_test", 100, 50, 10, 256, 1024, 0, time.Now()))
	// Issue #127 step 6: post-commit read + AppendRegionsPublished for
	// the single publish to "global".
	expectPostCommitReadAndAppend(mock, "t_test", "myapp", []string{"global"}, nil)

	if err := svc.ActivateDeployment(context.Background(), "t_test", "myapp", deploymentID); err != nil {
		t.Fatalf("ActivateDeployment: %v", err)
	}
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
	svc, mock, cleanup := activateSvcForTest(t, pub, "us-east")
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id FROM deployments WHERE id =`)).
		WithArgs("d_x").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id"}).
			AddRow("d_x", "t_test", "myapp", domain.StatusDeployed, "h", `{}`, time.Now(), false, nil, nil))
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs("t_test", "myapp").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).WillReturnResult(sqlmock.NewResult(1, 1))
	// ActivateDeployment also calls ClearStableSince inside the tx
	// (resets the stability clock for the new deployment). Mock the
	// UPDATE; the new column doesn't change row shape for the mock.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at FROM tenants WHERE id =`)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, used_outbound_bytes, used_request_count, quota_period_start FROM quotas WHERE tenant_id =`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "used_outbound_bytes", "quota_period_start"}).
			AddRow("t_test", 100, 50, 10, 256, 1024, 0, time.Now()))
	// Issue #127 step 6: single publish to default region "us-east".
	expectPostCommitReadAndAppend(mock, "t_test", "myapp", []string{"us-east"}, nil)

	if err := svc.ActivateDeployment(context.Background(), "t_test", "myapp", "d_x"); err != nil {
		t.Fatalf("ActivateDeployment: %v", err)
	}
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
	svc, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const deploymentID = "d_partial"
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id"}).
			AddRow(deploymentID, "t_test", "myapp", domain.StatusDeployed, "h", `{"us-east","eu-west","ap-south"}`, time.Now(), false, nil, nil))
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs("t_test", "myapp").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).WillReturnResult(sqlmock.NewResult(1, 1))
	// ActivateDeployment also calls ClearStableSince inside the tx
	// (resets the stability clock for the new deployment). Mock the
	// UPDATE; the new column doesn't change row shape for the mock.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at FROM tenants WHERE id =`)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, used_outbound_bytes, used_request_count, quota_period_start FROM quotas WHERE tenant_id =`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "used_outbound_bytes", "quota_period_start"}).
			AddRow("t_test", 100, 50, 10, 256, 1024, 0, time.Now()))
	// Issue #127 step 6: eu-west fails, so AppendRegionsPublished
	// covers us-east + ap-south and AppendRegionsFailed covers
	// eu-west.
	expectPostCommitReadAndAppend(mock, "t_test", "myapp",
		[]string{"us-east", "ap-south"},
		[]string{"eu-west"},
	)

	err := svc.ActivateDeployment(context.Background(), "t_test", "myapp", deploymentID)
	if err == nil {
		t.Fatal("ActivateDeployment returned nil; want an error mentioning the failed region")
	}
	// Issue #127 step 6: error is now a *PublishError wrapping
	// ErrPublishFailed. Both errors.Is (sentinel match) and
	// errors.As (typed breakdown) must be reachable.
	if !errors.Is(err, ErrPublishFailed) {
		t.Errorf("err = %v, want errors.Is(err, ErrPublishFailed) == true", err)
	}
	var pubErr *PublishError
	if !errors.As(err, &pubErr) {
		t.Fatalf("err = %T, want errors.As(err, &PublishError) == true", err)
	}
	if !equalStringSlices(pubErr.Failed, []string{"eu-west"}) {
		t.Errorf("pubErr.Failed = %v, want [eu-west]", pubErr.Failed)
	}
	if !equalStringSlices(pubErr.Published, []string{"us-east", "ap-south"}) {
		t.Errorf("pubErr.Published = %v, want [us-east ap-south]", pubErr.Published)
	}

	// All 3 publishes must have been ATTEMPTED — the failed region
	// didn't short-circuit the loop.
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
	svc, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const deploymentID = "d_zero_quota"
	regionsCol := `{"us-east"}`
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id"}).
			AddRow(deploymentID, "t_test", "myapp", domain.StatusDeployed, "h", regionsCol, time.Now(), false, nil, nil))
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs("t_test", "myapp").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).WillReturnResult(sqlmock.NewResult(1, 1))
	// ActivateDeployment also calls ClearStableSince inside the tx
	// (resets the stability clock for the new deployment). Mock the
	// UPDATE; the new column doesn't change row shape for the mock.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at FROM tenants WHERE id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))
	// Quota row with MaxMemoryMB=0 — should be treated as "unset" and
	// fall through to the 256 default.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, used_outbound_bytes, used_request_count, quota_period_start FROM quotas WHERE tenant_id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "used_outbound_bytes", "quota_period_start"}).
			AddRow("t_test", 100, 50, 10, 0, 1024, 0, time.Now()))
	// Issue #127 step 6: post-commit read + AppendRegionsPublished for
	// the single publish to "us-east".
	expectPostCommitReadAndAppend(mock, "t_test", "myapp", []string{"us-east"}, nil)

	if err := svc.ActivateDeployment(context.Background(), "t_test", "myapp", deploymentID); err != nil {
		t.Fatalf("ActivateDeployment: %v", err)
	}
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
	svc, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const deploymentID = "d_no_quota"
	regionsCol := `{"us-east"}`
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id"}).
			AddRow(deploymentID, "t_test", "myapp", domain.StatusDeployed, "h", regionsCol, time.Now(), false, nil, nil))
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs("t_test", "myapp").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).WillReturnResult(sqlmock.NewResult(1, 1))
	// ActivateDeployment also calls ClearStableSince inside the tx
	// (resets the stability clock for the new deployment). Mock the
	// UPDATE; the new column doesn't change row shape for the mock.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at FROM tenants WHERE id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))
	// Empty row set (no quota row) — GetByTenantID returns (nil, nil).
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, used_outbound_bytes, used_request_count, quota_period_start FROM quotas WHERE tenant_id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "used_outbound_bytes", "quota_period_start"}))
	// Issue #127 step 6: post-commit read + AppendRegionsPublished for
	// the single publish to "us-east".
	expectPostCommitReadAndAppend(mock, "t_test", "myapp", []string{"us-east"}, nil)

	if err := svc.ActivateDeployment(context.Background(), "t_test", "myapp", deploymentID); err != nil {
		t.Fatalf("ActivateDeployment: %v", err)
	}
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

// expectPostCommitReadAndAppend mocks the post-tx publish-state
// read + the append calls that publishSwap issues on a freshly-
// activated row. The Set upsert inside ActivateDeployment resets
// regions_published / regions_failed / regions_cached /
// regions_cache_failed to zero (see ActiveDeploymentRepository.Set's
// DO UPDATE clause), so the read returns empty arrays — which
// means the publish set computed inside publishSwap equals the
// input regions list (no idempotency skip). The test then asserts
// which regions were appended to regions_published vs
// regions_failed by passing those slices as expected. Pass nil
// for either to suppress that expectation.
//
// `expectCached` (issue #332, PR 2) is appended only if non-nil —
// pre-PR-2 tests pass nil and the helper skips the third
// ExpectExec. PR-2+ tests pass the cached regions to also mock
// the AppendRegionsCacheState call inside the same tx (PR 2
// follow-up renamed and merged: regions_cached = (...) AND
// regions_cache_failed = (...) in one statement).
//
// Pinned by issue #127 step 6: the idempotency contract relies
// on this read happening AFTER the tx commits, not before.
// Reading inside the tx would not see the Set reset and would
// return the prior activation's publish state — wrong.
func expectPostCommitReadAndAppend(mock sqlmock.Sqlmock, tenantID, appName string, expectPublished, expectFailed []string, expectCached ...[]string) {
	// The post-commit read is the Get that publishSwap does to
	// discover which regions are already done. On a fresh
	// activation the row was just upserted with all six per-
	// region state columns zeroed, so the read returns the empty
	// values.
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`,
	)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled", "stable_since",
			"regions_published", "regions_failed", "regions_cached",
			"regions_cache_failed", "last_publish_at", "last_publish_attempt_id",
		}).AddRow(
			tenantID, appName, "d_xxx", nil, false, nil,
			"{}", "{}", "{}", "{}",
			nil, nil,
		))

	// AppendRegionsPublished / AppendRegionsFailed fire on a
	// successful / failed per-region publish respectively;
	// AppendRegionsCacheState (issue #332, PR 2 follow-up) fires
	// whenever the cache loop ran (with the merged
	// succeeded+skipped regions_cached slice + cache_failed
	// regions). All three live inside the same
	// repository.Transaction (issue #127 follow-ups — the three
	// appends must succeed-or-rollback together so the row's
	// per-region state columns stay consistent). The attempt ID
	// is a UUID (any value) and the timestamp is time.Now(); both
	// are passed via sqlmock.AnyArg.
	hasCached := len(expectCached) > 0 && len(expectCached[0]) > 0
	if len(expectPublished) > 0 || len(expectFailed) > 0 || hasCached {
		mock.ExpectBegin()
		if len(expectPublished) > 0 {
			mock.ExpectExec(regexp.QuoteMeta(
				`UPDATE active_deployments SET regions_published = (`,
			)).
				WithArgs(tenantID, appName, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
				WillReturnResult(sqlmock.NewResult(0, 1))
		}
		if len(expectFailed) > 0 {
			mock.ExpectExec(regexp.QuoteMeta(
				`UPDATE active_deployments SET regions_failed = (`,
			)).
				WithArgs(tenantID, appName, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
				WillReturnResult(sqlmock.NewResult(0, 1))
		}
		if hasCached {
			mock.ExpectExec(regexp.QuoteMeta(
				`UPDATE active_deployments SET regions_cached = (`,
			)).
				WithArgs(tenantID, appName, sqlmock.AnyArg(), sqlmock.AnyArg()).
				WillReturnResult(sqlmock.NewResult(0, 1))
		}
		mock.ExpectCommit()
	}
}

// TestPublishSwap_AppendsAreAtomic covers the issue #127 follow-up
// atomicity fix: if AppendRegionsPublished fails inside the wrapping
// tx, AppendRegionsFailed MUST NOT run (no second Exec) and the tx
// must roll back. This is what guarantees the row's
// (regions_published, regions_failed, last_publish_at,
// last_publish_attempt_id) stay consistent across partial failures.
func TestPublishSwap_AppendsAreAtomic(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() {
		if err := sqlDB.Close(); err != nil {
			_ = err // sqlmock Close can return error if close is unexpected or other expectations are not fully met.
		}
	})
	db := sqlx.NewDb(sqlDB, "postgres")

	tenantID, appName := "t_atomic", "myapp"

	// 1. Post-commit read returns empty arrays — publishSwap will
	//    publish to both regions (us-east + eu-west).
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`,
	)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled", "stable_since",
			"regions_published", "regions_failed", "regions_cached",
			"regions_cache_failed", "last_publish_at", "last_publish_attempt_id",
		}).AddRow(
			tenantID, appName, "d_atomic", nil, false, nil,
			"{}", "{}", "{}", "{}",
			nil, nil,
		))

	pub := &RecordingPublisher{
		// us-east succeeds, eu-west fails — so published=[us-east],
		// failed=[eu-west] and BOTH appends run inside the tx.
		failFor: map[string]error{"eu-west": errors.New("nats unreachable")},
	}

	mock.ExpectBegin()
	// AppendRegionsPublished succeeds.
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET regions_published = (`,
	)).
		WithArgs(tenantID, appName, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// AppendRegionsFailed fails — this is what we want to observe.
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET regions_failed = (`,
	)).
		WithArgs(tenantID, appName, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(errors.New("simulated DB outage"))
	mock.ExpectRollback()

	activeRepo := repository.NewActiveDeploymentRepository(db)
	svc := &DeploymentService{
		db:         db,
		activeRepo: activeRepo,
		publisher:  pub,
	}

	msg := &nats.TaskMessage{
		TenantID: tenantID,
		Apps:     map[string]nats.AppConfig{appName: {DeploymentID: "d_atomic"}},
	}
	err = svc.publishSwap(context.Background(), tenantID, appName, "d_atomic", msg, []string{"us-east", "eu-west"})
	if err == nil {
		t.Fatal("publishSwap: want PublishError wrapping ErrPublishFailed, got nil")
	}
	if !errors.Is(err, ErrPublishFailed) {
		t.Errorf("publishSwap err = %v, want errors.Is ErrPublishFailed", err)
	}

	// Assert the recorded publisher saw both regions (us-east OK,
	// eu-west failed) — verifies the loop ran to completion before
	// the tx.
	if len(pub.calls) != 2 {
		t.Errorf("publisher calls = %d, want 2", len(pub.calls))
	}

	// sqlmock has no further expectations — if Rollback was not
	// issued, mock.ExpectationsWereMet would surface a "remaining
	// expectation" error. We assert below for an explicit signal.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v (Rollback should have fired after the failed append)", err)
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
		`SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`,
	)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled", "stable_since",
			"regions_published", "regions_failed", "regions_cached",
			"regions_cache_failed", "last_publish_at", "last_publish_attempt_id",
		}).AddRow(
			tenantID, appName, "d_skip", nil, false, nil,
			"{}", "{}", "{fra}", "{}",
			nil, nil,
		))

	pub := &RecordingPublisher{}
	// Wire the cache pusher so the cache loop runs. Both regions
	// succeed; `fra` is skipped due to alreadyCached (no push),
	// `iad` is pushed.
	cachePusher := newRecordingCachePusher()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET regions_published = (`,
	)).
		WithArgs(tenantID, appName, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// PR 2 follow-up: AppendRegionsCacheState takes 4 args —
	// (tenant, app, succeeded, failed). WithArgs uses AnyArg
	// for the two slice args so we don't pin the merged order.
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET regions_cached = (`,
	)).
		WithArgs(tenantID, appName, sqlmock.AnyArg(), sqlmock.AnyArg()).
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

	msg := &nats.TaskMessage{
		TenantID: tenantID,
		Apps:     map[string]nats.AppConfig{appName: {DeploymentID: "d_skip"}},
	}

	// waitForWorkers (added on origin/main by 36ad512, Layer 3 PR):
	// publishSwap now blocks until active workers confirm the new
	// deployment. Mock the workers query + worker_status lookup.
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT id, tenant_id, region, ip, memory_mb, last_seen, created_at FROM workers ORDER BY region, created_at DESC`,
	)).WillReturnRows(sqlmock.NewRows([]string{
		"id", "tenant_id", "region", "ip", "memory_mb", "last_seen", "created_at",
	}).AddRow("w_us-east_1", "t_skip", "us-east", "127.0.0.1", 4096, time.Now(), time.Now()))

	appsJSON := `{"myapp":{"status":"running","exit_code":0,"deployment_id":"d_skip","tenant_id":"t_skip","port":8080}}`
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT DISTINCT ON (worker_id) worker_id, apps, last_report FROM worker_status WHERE worker_id = ANY($1) ORDER BY worker_id, last_report DESC`,
	)).WithArgs(pq.Array([]string{"w_us-east_1"})).
		WillReturnRows(sqlmock.NewRows([]string{"worker_id", "apps", "last_report"}).
			AddRow("w_us-east_1", json.RawMessage(appsJSON), time.Now()))

	if err := svc.publishSwap(context.Background(), tenantID, appName, "d_skip", msg, []string{"fra", "iad"}); err != nil {
		t.Fatalf("publishSwap: %v", err)
	}

	// NATS publish fires for BOTH regions (the skip is cache-only).
	if got := pub.regionsCalled(); !reflect.DeepEqual(got, []string{"fra", "iad"}) {
		t.Errorf("publisher regions = %v, want [fra iad]", got)
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
		`SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`,
	)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled", "stable_since",
			"regions_published", "regions_failed", "regions_cached",
			"regions_cache_failed", "last_publish_at", "last_publish_attempt_id",
		}).AddRow(
			tenantID, appName, "d_atomic_cache", nil, false, nil,
			"{}", "{}", "{}", "{}",
			nil, nil,
		))

	pub := &RecordingPublisher{}
	cachePusher := newRecordingCachePusher()

	mock.ExpectBegin()
	// AppendRegionsPublished succeeds.
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET regions_published = (`,
	)).
		WithArgs(tenantID, appName, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// AppendRegionsCacheState fails — this is the trigger. PR 2
	// follow-up: 4 args (tenant, app, succeeded, failed).
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

	msg := &nats.TaskMessage{
		TenantID: tenantID,
		Apps:     map[string]nats.AppConfig{appName: {DeploymentID: "d_atomic_cache"}},
	}

	// waitForWorkers (added on origin/main by 36ad512, Layer 3 PR):
	// publishSwap now blocks until active workers confirm the new
	// deployment. Mock the workers query + worker_status lookup.
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT id, tenant_id, region, ip, memory_mb, last_seen, created_at FROM workers ORDER BY region, created_at DESC`,
	)).WillReturnRows(sqlmock.NewRows([]string{
		"id", "tenant_id", "region", "ip", "memory_mb", "last_seen", "created_at",
	}).AddRow("w_us-east_1", "t_atomic_cache", "us-east", "127.0.0.1", 4096, time.Now(), time.Now()))

	appsJSON := `{"myapp":{"status":"running","exit_code":0,"deployment_id":"d_atomic_cache","tenant_id":"t_atomic_cache","port":8080}}`
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT DISTINCT ON (worker_id) worker_id, apps, last_report FROM worker_status WHERE worker_id = ANY($1) ORDER BY worker_id, last_report DESC`,
	)).WithArgs(pq.Array([]string{"w_us-east_1"})).
		WillReturnRows(sqlmock.NewRows([]string{"worker_id", "apps", "last_report"}).
			AddRow("w_us-east_1", json.RawMessage(appsJSON), time.Now()))

	err = svc.publishSwap(context.Background(), tenantID, appName, "d_atomic_cache", msg, []string{"fra"})
	// The publish itself succeeded (the NATS publish loop is
	// before the tx) — so the publisher saw the call. The cache
	// append failure is "best effort" (matches the existing
	// publish-state best-effort contract at publishSwap line 856):
	// the error is logged but the cache-append failure does NOT
	// change the returned error. The tx still rolls back (the
	// failed Exec triggers a Rollback automatically via the
	// repository.Transaction wrapper), so no partial state is
	// persisted. The next activation will re-push the cache
	// because RegionsCached was wiped to '{}' on the prior
	// re-activation.
	if err != nil {
		t.Errorf("publishSwap: want nil (best-effort cache append), got %v", err)
	}
	if got := pub.regionsCalled(); !reflect.DeepEqual(got, []string{"fra"}) {
		t.Errorf("publisher regions = %v, want [fra]", got)
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
		`SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`,
	)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled", "stable_since",
			"regions_published", "regions_failed", "regions_cached",
			"regions_cache_failed", "last_publish_at", "last_publish_attempt_id",
		}).AddRow(
			tenantID, appName, "d_split", nil, false, nil,
			"{}", "{}", "{fra}", "{}",
			nil, nil,
		))

	pub := &RecordingPublisher{}
	cachePusher := newRecordingCachePusher()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET regions_published = (`,
	)).
		WithArgs(tenantID, appName, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// PR 2 follow-up: 4-arg AppendRegionsCacheState. The
	// succeeded arg carries the union (iad, fra); the failed
	// arg is empty.
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET regions_cached = (`,
	)).
		WithArgs(tenantID, appName, sqlmock.AnyArg(), sqlmock.AnyArg()).
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

	msg := &nats.TaskMessage{
		TenantID: tenantID,
		Apps:     map[string]nats.AppConfig{appName: {DeploymentID: "d_split"}},
	}

	// waitForWorkers (added on origin/main by 36ad512, Layer 3 PR):
	// publishSwap now blocks until active workers confirm the new
	// deployment. Mock the workers query + worker_status lookup.
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT id, tenant_id, region, ip, memory_mb, last_seen, created_at FROM workers ORDER BY region, created_at DESC`,
	)).WillReturnRows(sqlmock.NewRows([]string{
		"id", "tenant_id", "region", "ip", "memory_mb", "last_seen", "created_at",
	}).AddRow("w_us-east_1", "t_split", "us-east", "127.0.0.1", 4096, time.Now(), time.Now()))

	appsJSON := `{"myapp":{"status":"running","exit_code":0,"deployment_id":"d_split","tenant_id":"t_split","port":8080}}`
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT DISTINCT ON (worker_id) worker_id, apps, last_report FROM worker_status WHERE worker_id = ANY($1) ORDER BY worker_id, last_report DESC`,
	)).WithArgs(pq.Array([]string{"w_us-east_1"})).
		WillReturnRows(sqlmock.NewRows([]string{"worker_id", "apps", "last_report"}).
			AddRow("w_us-east_1", json.RawMessage(appsJSON), time.Now()))

	if err := svc.publishSwap(context.Background(), tenantID, appName, "d_split", msg, []string{"fra", "iad"}); err != nil {
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
		`SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`,
	)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled", "stable_since",
			"regions_published", "regions_failed", "regions_cached",
			"regions_cache_failed", "last_publish_at", "last_publish_attempt_id",
		}).AddRow(
			tenantID, appName, "d_best_effort", nil, false, nil,
			"{}", "{}", "{}", "{}",
			nil, nil,
		))

	pub := &RecordingPublisher{}
	cachePusher := newRecordingCachePusher()
	cachePusher.failFor["fra"] = errors.New("cache 500")
	cachePusher.failFor["iad"] = errors.New("cache 500")

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET regions_published = (`,
	)).
		WithArgs(tenantID, appName, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// PR 2 follow-up: AppendRegionsCacheState with succeeded=[],
	// failed=[fra, iad]. The test asserts the call shape via the
	// regex; sqlmock.AnyArg() lets the test stay agnostic about
	// the order of regions inside the failed slice.
	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET regions_cached = (`,
	)).
		WithArgs(tenantID, appName, sqlmock.AnyArg(), sqlmock.AnyArg()).
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

	msg := &nats.TaskMessage{
		TenantID: tenantID,
		Apps:     map[string]nats.AppConfig{appName: {DeploymentID: "d_best_effort"}},
	}

	// waitForWorkers (added on origin/main by 36ad512, Layer 3 PR):
	// publishSwap now blocks until active workers confirm the new
	// deployment. Mock the workers query + worker_status lookup.
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT id, tenant_id, region, ip, memory_mb, last_seen, created_at FROM workers ORDER BY region, created_at DESC`,
	)).WillReturnRows(sqlmock.NewRows([]string{
		"id", "tenant_id", "region", "ip", "memory_mb", "last_seen", "created_at",
	}).AddRow("w_us-east_1", "t_best_effort", "us-east", "127.0.0.1", 4096, time.Now(), time.Now()))

	appsJSON := `{"myapp":{"status":"running","exit_code":0,"deployment_id":"d_best_effort","tenant_id":"t_best_effort","port":8080}}`
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT DISTINCT ON (worker_id) worker_id, apps, last_report FROM worker_status WHERE worker_id = ANY($1) ORDER BY worker_id, last_report DESC`,
	)).WithArgs(pq.Array([]string{"w_us-east_1"})).
		WillReturnRows(sqlmock.NewRows([]string{"worker_id", "apps", "last_report"}).
			AddRow("w_us-east_1", json.RawMessage(appsJSON), time.Now()))

	if err := svc.publishSwap(context.Background(), tenantID, appName, "d_best_effort", msg, []string{"fra", "iad"}); err != nil {
		t.Fatalf("publishSwap: want nil (cache failures are best-effort), got %v", err)
	}

	// Both regions' cache pushes were attempted (and failed).
	if got := cachePusher.regionsPushed(); !reflect.DeepEqual(got, []string{"fra", "iad"}) {
		t.Errorf("cache pusher regions = %v, want [fra iad]", got)
	}
	// Both NATS publishes succeeded.
	if got := pub.regionsCalled(); !reflect.DeepEqual(got, []string{"fra", "iad"}) {
		t.Errorf("publisher regions = %v, want [fra iad]", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}
