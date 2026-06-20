package service

import (
	"context"
	"errors"
	"regexp"
	"strings"
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at"}).
			AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, deploymentHash, regionsCol, time.Now()))

	// 2. activeRepo.Set — INSERT ... ON CONFLICT DO UPDATE.
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).
		WillReturnResult(sqlmock.NewResult(1, 1))

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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb FROM quotas WHERE tenant_id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb"}).
			AddRow(tenantID, 100, 50, 10, 512, 1024))

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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at"}).
			AddRow(deploymentID, "t_test", "myapp", domain.StatusDeployed, "h", `{}`, time.Now()))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at FROM tenants WHERE id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb FROM quotas WHERE tenant_id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb"}).
			AddRow("t_test", 100, 50, 10, 256, 1024))

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

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at FROM deployments WHERE id =`)).
		WithArgs("d_x").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at"}).
			AddRow("d_x", "t_test", "myapp", domain.StatusDeployed, "h", `{}`, time.Now()))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at FROM tenants WHERE id =`)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb FROM quotas WHERE tenant_id =`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb"}).
			AddRow("t_test", 100, 50, 10, 256, 1024))

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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at"}).
			AddRow(deploymentID, "t_test", "myapp", domain.StatusDeployed, "h", `{"us-east","eu-west","ap-south"}`, time.Now()))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at FROM tenants WHERE id =`)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb FROM quotas WHERE tenant_id =`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb"}).
			AddRow("t_test", 100, 50, 10, 256, 1024))

	err := svc.ActivateDeployment(context.Background(), "t_test", "myapp", deploymentID)
	if err == nil {
		t.Fatal("ActivateDeployment returned nil; want an error mentioning the failed region")
	}
	if !strings.Contains(err.Error(), "eu-west") {
		t.Errorf("error %q should mention the failed region 'eu-west'", err.Error())
	}
	if !strings.Contains(err.Error(), "1 region") {
		t.Errorf("error %q should report the failed-region count", err.Error())
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at"}).
			AddRow(deploymentID, "t_test", "myapp", domain.StatusDeployed, "h", regionsCol, time.Now()))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at FROM tenants WHERE id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))
	// Quota row with MaxMemoryMB=0 — should be treated as "unset" and
	// fall through to the 256 default.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb FROM quotas WHERE tenant_id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb"}).
			AddRow("t_test", 100, 50, 10, 0, 1024))

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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at"}).
			AddRow(deploymentID, "t_test", "myapp", domain.StatusDeployed, "h", regionsCol, time.Now()))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at FROM tenants WHERE id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_test", "T", "free", `{}`, time.Now()))
	// Empty row set (no quota row) — GetByTenantID returns (nil, nil).
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb FROM quotas WHERE tenant_id =`)).
		WithArgs("t_test").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb"}))

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
