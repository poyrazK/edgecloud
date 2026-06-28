package service

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/jmoiron/sqlx"
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled"}).
			AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, deploymentHash, regionsCol, time.Now(), false))

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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at FROM tenants WHERE id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow(tenantID, "Test Tenant", "free", `{"api.example.com"}`, time.Now()))

	// 5. quotaRepo.GetByTenantID — ActivateDeployment reads the quota
	// to populate MaxMemoryMB on the AppConfig (per main's quota
	// wiring). Return a row so the field flows.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, used_outbound_bytes, quota_period_start FROM quotas WHERE tenant_id =`)).
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled"}).
			AddRow(deploymentID, "t_test", "myapp", domain.StatusDeployed, "h", `{}`, time.Now(), false))
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at FROM tenants WHERE id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, used_outbound_bytes, quota_period_start FROM quotas WHERE tenant_id =`)).
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

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled FROM deployments WHERE id =`)).
		WithArgs("d_x").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled"}).
			AddRow("d_x", "t_test", "myapp", domain.StatusDeployed, "h", `{}`, time.Now(), false))
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at FROM tenants WHERE id =`)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, used_outbound_bytes, quota_period_start FROM quotas WHERE tenant_id =`)).
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled"}).
			AddRow(deploymentID, "t_test", "myapp", domain.StatusDeployed, "h", `{"us-east","eu-west","ap-south"}`, time.Now(), false))
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at FROM tenants WHERE id =`)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, used_outbound_bytes, quota_period_start FROM quotas WHERE tenant_id =`)).
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled"}).
			AddRow(deploymentID, "t_test", "myapp", domain.StatusDeployed, "h", regionsCol, time.Now(), false))
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at FROM tenants WHERE id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))
	// Quota row with MaxMemoryMB=0 — should be treated as "unset" and
	// fall through to the 256 default.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, used_outbound_bytes, quota_period_start FROM quotas WHERE tenant_id =`)).
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled"}).
			AddRow(deploymentID, "t_test", "myapp", domain.StatusDeployed, "h", regionsCol, time.Now(), false))
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at FROM tenants WHERE id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))
	// Empty row set (no quota row) — GetByTenantID returns (nil, nil).
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, used_outbound_bytes, quota_period_start FROM quotas WHERE tenant_id =`)).
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
// read + the two append calls that publishSwap issues on a
// freshly-activated row. The Set upsert inside ActivateDeployment
// resets regions_published / regions_failed to zero (see
// ActiveDeploymentRepository.Set's DO UPDATE clause), so the
// read returns empty arrays — which means the publish set
// computed inside publishSwap equals the input regions list
// (no idempotency skip). The test then asserts which regions
// were appended to regions_published vs regions_failed by
// passing those slices as expected. Pass nil for either to
// suppress that expectation.
//
// Pinned by issue #127 step 6: the idempotency contract relies
// on this read happening AFTER the tx commits, not before.
// Reading inside the tx would not see the Set reset and would
// return the prior activation's publish state — wrong.
func expectPostCommitReadAndAppend(mock sqlmock.Sqlmock, tenantID, appName string, expectPublished, expectFailed []string) {
	// The post-commit read is the Get that publishSwap does to
	// discover which regions are already done. On a fresh
	// activation the row was just upserted with all four publish-
	// state columns zeroed, so the read returns the empty values.
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, last_publish_at, last_publish_attempt_id FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`,
	)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled", "stable_since",
			"regions_published", "regions_failed",
			"last_publish_at", "last_publish_attempt_id",
		}).AddRow(
			tenantID, appName, "d_xxx", nil, false, nil,
			"{}", "{}",
			nil, nil,
		))

	// AppendRegionsPublished / AppendRegionsFailed fire on a
	// successful / failed per-region publish respectively, inside a
	// single repository.Transaction (issue #127 follow-ups — both
	// appends must succeed-or-rollback together so the row's
	// publish-state columns stay consistent). The attempt ID is a
	// UUID (any value) and the timestamp is time.Now(); both are
	// passed via sqlmock.AnyArg.
	if len(expectPublished) > 0 || len(expectFailed) > 0 {
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
		`SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, last_publish_at, last_publish_attempt_id FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`,
	)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled", "stable_since",
			"regions_published", "regions_failed",
			"last_publish_at", "last_publish_attempt_id",
		}).AddRow(
			tenantID, appName, "d_atomic", nil, false, nil,
			"{}", "{}",
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
