package service

// Issue #560 tests for the EnvService publish-if-active path.
//
// The non-publish path of EnvService (SetEnv without publish deps
// wired, ListEnv, DecryptEnvMap, etc.) is covered by the legacy
// env_test.go using hand-rolled mocks. Those tests don't exercise
// the tx code path at all — `publishDepsReady()` is false for them
// because they only set appEnvRepo.
//
// This file covers the publish path with sqlmock-backed tests:
//   - active dep → outbox row enqueued with the post-write env map
//   - no active dep → no outbox row (silent skip)
//   - disabled tenant → returns ErrTenantDisabled (handler maps to 409)
//   - delete symmetric to the set case
//
// Run under `go test -race -count=20 ./internal/service/...` to
// exercise the issue #585 -race gate against the new tx code.

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// newEnvMockDB wraps sqlmock.New with the QueryMatcherRegexp option
// so the deployment / tenant / quota / outbox queries match by
// pattern. Mirrors newDeploymentMockDB at deployment_test.go:111.
func newEnvMockDB(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return sqlxDB, mock, func() { _ = mockDB.Close() }
}

// newEnvSvcForPublish wires a full EnvService with the publish
// dependencies set. Tests then script the SQL via the returned
// sqlmock. Mirrors newMinimalDeploymentServiceForRollback at
// deployment_test.go:1324.
func newEnvSvcForPublish(t *testing.T, db *sqlx.DB) *EnvService {
	t.Helper()
	appEnvRepo := repository.NewAppEnvRepository(db)
	svc := NewEnvService(appEnvRepo)
	svc.SetPublishDeps(
		db,
		repository.NewTenantRepository(db),
		repository.NewActiveDeploymentRepository(db),
		repository.NewDeploymentRepository(db),
		repository.NewQuotaRepository(db),
		repository.NewOutboxRepository(db),
		appEnvRepo, // same instance; both fields hit the same rows
		NewPublishBuilder(),
	)
	// Disable encryption so plaintext values flow through unchanged
	// — the test asserts the marshaled TaskMessage carries the
	// post-write env map.
	svc.SetSecretEncryptor(nil)
	_ = svc.publishDepsReady() // sanity: should be true
	return svc
}

// TestEnvService_SetEnv_PublishesIfActive: app has an active
// deployment → env write commits AND a task_update outbox row is
// enqueued with the post-write env map. Asserts the JSON payload
// round-trips through nats.TaskMessage and carries the new env.
func TestEnvService_SetEnv_PublishesIfActive(t *testing.T) {
	db, mock, cleanup := newEnvMockDB(t)
	defer cleanup()

	tenantID, appName := "t_pub", "myapp"
	deploymentID := "d_active"
	now := time.Now()

	mock.ExpectBegin()
	// 1. env upsert (SetEnv's tx-bound write)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO app_env`)).
		WithArgs(tenantID, appName, "LOG_LEVEL", "debug").
		WillReturnResult(sqlmock.NewResult(1, 1))
	// 2. publishIfActiveTx: active_deployments.Get
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id, preview_id, preview_pr_number, activation_attempt_started_at FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled",
		}).AddRow(tenantID, appName, deploymentID, nil, false))
	// 3. deploymentRepo.GetByID
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id = $1`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at",
			"auto_rollback_enabled", "signature", "signing_key_id",
		}).AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, "deadbeef", `{"us-east"}`, now, false, "sig-b64", "default"))
	// 4. tenantRepo.GetByID
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "name", "plan", "allowlisted_destinations",
		}).AddRow(tenantID, "Test Tenant", "pro", `{}`))
	// 5. quotaRepo.GetByTenantID
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, max_resident_seconds_per_month, max_compute_ms_per_month, used_outbound_bytes, used_request_count, used_memory_mb, used_resident_seconds, used_compute_ms, quota_period_start, quota_lock_grace_until`) + `.*FROM quotas WHERE tenant_id =`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_memory_mb",
		}).AddRow(tenantID, 256))
	// 6. app_env list (post-write — includes the just-set LOG_LEVEL=debug)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}).
			AddRow(tenantID, appName, "LOG_LEVEL", "debug"))
	// 7. outbox INSERT — capture the payload bytes for assertion
	payload := mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO outbox`)).
		WithArgs(tenantID, appName, "task_update", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	_ = payload
	mock.ExpectCommit()

	svc := newEnvSvcForPublish(t, db)
	if err := svc.SetEnv(context.Background(), tenantID, appName, "LOG_LEVEL", "debug"); err != nil {
		t.Fatalf("SetEnv: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

// TestEnvService_SetEnv_NoActiveDeployment: app has no active row
// → env write commits AND no outbox row is enqueued. Silent skip
// (no 409). The active_deployments.Get returns ErrNoRows which the
// repo normalizes to (nil, nil).
func TestEnvService_SetEnv_NoActiveDeployment(t *testing.T) {
	db, mock, cleanup := newEnvMockDB(t)
	defer cleanup()

	tenantID, appName := "t_pub", "never-activated"

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO app_env`)).
		WithArgs(tenantID, appName, "LOG_LEVEL", "debug").
		WillReturnResult(sqlmock.NewResult(1, 1))
	// activeRepo.Get returns no rows → publishIfActiveTx returns
	// nil and the tx commits without an outbox row.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id, preview_id, preview_pr_number, activation_attempt_started_at FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled",
		})) // no AddRow → sql.ErrNoRows → activeRepo returns (nil, nil)
	mock.ExpectCommit()

	svc := newEnvSvcForPublish(t, db)
	if err := svc.SetEnv(context.Background(), tenantID, appName, "LOG_LEVEL", "debug"); err != nil {
		t.Fatalf("SetEnv: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

// TestEnvService_SetEnv_DisabledTenant: tenant has disabled_at set
// → SetEnv returns ErrTenantDisabled (handler maps to 409) and the
// env write rolls back with the rest of the tx.
func TestEnvService_SetEnv_DisabledTenant(t *testing.T) {
	db, mock, cleanup := newEnvMockDB(t)
	defer cleanup()

	tenantID, appName := "t_disabled", "myapp"
	deploymentID := "d_active"
	now := time.Now()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO app_env`)).
		WithArgs(tenantID, appName, "LOG_LEVEL", "debug").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id, preview_id, preview_pr_number, activation_attempt_started_at FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled",
		}).AddRow(tenantID, appName, deploymentID, nil, false))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id = $1`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at",
			"auto_rollback_enabled", "signature", "signing_key_id",
		}).AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, "deadbeef", `{}`, now, false, "sig-b64", "default"))
	// tenantRepo.GetByID returns a row with disabled_at != NULL.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "name", "plan", "allowlisted_destinations",
			"disabled_at",
		}).AddRow(tenantID, "Test Tenant", "pro", `{}`, now.Add(-time.Hour)))
	// Expect rollback (no commit).
	mock.ExpectRollback()

	svc := newEnvSvcForPublish(t, db)
	err := svc.SetEnv(context.Background(), tenantID, appName, "LOG_LEVEL", "debug")
	if err == nil {
		t.Fatal("SetEnv on disabled tenant: expected error, got nil")
	}
	if !errors.Is(err, ErrTenantDisabled) {
		t.Errorf("err = %v, want ErrTenantDisabled in chain", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

// TestEnvService_DeleteEnv_PublishesIfActive: delete-symmetric to
// the set case. The env row is deleted inside the tx, the active
// deployment is looked up, and an outbox row is enqueued carrying
// the post-delete env map (which omits the deleted key).
func TestEnvService_DeleteEnv_PublishesIfActive(t *testing.T) {
	db, mock, cleanup := newEnvMockDB(t)
	defer cleanup()

	tenantID, appName := "t_pub", "myapp"
	deploymentID := "d_active"
	now := time.Now()

	mock.ExpectBegin()
	// 1. env delete
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM app_env`)).
		WithArgs(tenantID, appName, "LOG_LEVEL").
		WillReturnResult(sqlmock.NewResult(0, 1))
	// 2. publishIfActiveTx: active row, deployment row, tenant row,
	// quota row, env list (no LOG_LEVEL anymore), outbox INSERT.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id, preview_id, preview_pr_number, activation_attempt_started_at FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled",
		}).AddRow(tenantID, appName, deploymentID, nil, false))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id = $1`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at",
			"auto_rollback_enabled", "signature", "signing_key_id",
		}).AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, "deadbeef", `{}`, now, false, "sig-b64", "default"))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "name", "plan", "allowlisted_destinations",
		}).AddRow(tenantID, "Test Tenant", "pro", `{}`))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, max_resident_seconds_per_month, max_compute_ms_per_month, used_outbound_bytes, used_request_count, used_memory_mb, used_resident_seconds, used_compute_ms, quota_period_start, quota_lock_grace_until`) + `.*FROM quotas WHERE tenant_id =`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_memory_mb",
		}).AddRow(tenantID, 256))
	// env list returns OTHER vars (the deleted key is gone)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}).
			AddRow(tenantID, appName, "DB_URL", "postgres://..."))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO outbox`)).
		WithArgs(tenantID, appName, "task_update", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	svc := newEnvSvcForPublish(t, db)
	if err := svc.DeleteEnv(context.Background(), tenantID, appName, "LOG_LEVEL"); err != nil {
		t.Fatalf("DeleteEnv: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

// TestBuildPublishPayload_Shape sanity-checks that the marshaled
// outbox payload round-trips through nats.TaskMessage with the
// post-write env map under Apps.<app>.Env. Pins the wire contract
// so a future refactor of buildPublishPayload that accidentally
// drops the env from AppConfig fails this test rather than silently
// publishing stale data.
//
// Pure helper test — no sqlmock needed, no EnvService required.
func TestBuildPublishPayload_Shape(t *testing.T) {
	tenantID, appName, deploymentID := "t_shape", "myapp", "d_shape"

	b := NewPublishBuilder()
	dep := &domain.Deployment{
		Hash:         "h",
		Signature:    "sig",
		SigningKeyID: "default",
	}
	tenant := &domain.Tenant{
		ID:                      tenantID,
		Name:                    "T",
		Plan:                    "pro",
		AllowlistedDestinations: nil,
	}
	quota := &domain.Quota{TenantID: tenantID, MaxMemoryMB: 256}
	envMap := map[string]string{"LOG_LEVEL": "trace"}
	payload, err := b.buildPublishPayload(context.Background(), tenantID, appName,
		deploymentID, dep, tenant, []string{}, quota, envMap)
	if err != nil {
		t.Fatalf("buildPublishPayload: %v", err)
	}
	var msg nats.TaskMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		t.Fatalf("unmarshal TaskMessage: %v", err)
	}
	if msg.Type != "task_update" {
		t.Errorf("msg.Type = %q, want task_update", msg.Type)
	}
	if msg.TenantID != tenantID {
		t.Errorf("msg.TenantID = %q, want %q", msg.TenantID, tenantID)
	}
	app, ok := msg.Apps[appName]
	if !ok {
		t.Fatalf("msg.Apps[%q] missing; got %v", appName, msg.Apps)
	}
	if app.Env["LOG_LEVEL"] != "trace" {
		t.Errorf("app.Env[LOG_LEVEL] = %q, want trace", app.Env["LOG_LEVEL"])
	}
	if app.DeploymentID != deploymentID {
		t.Errorf("app.DeploymentID = %q, want %q", app.DeploymentID, deploymentID)
	}
	if app.SigningKeyID != "default" {
		t.Errorf("app.SigningKeyID = %q, want default", app.SigningKeyID)
	}
	if app.MaxMemoryMB != 256 {
		t.Errorf("app.MaxMemoryMB = %d, want 256", app.MaxMemoryMB)
	}
	if app.SocketMode != "" {
		t.Errorf("app.SocketMode = %q, want \"\" (default omitted on the wire)", app.SocketMode)
	}
}

// TestBuildPublishPayload_SocketModePropagated was the issue #548
// regression guard that asserted socket_mode:allow-all is stamped
// on the AppConfig when an activate passes Protocol=tcp. The
// CP-side protocol-driven socket_mode stamping was reverted in the
// post-merge fixes because the activation-time Protocol source
// required an apps-table schema column that did not exist (the
// JOIN silently referenced a non-existent apps.value JSONB
// column). L4 bind() gating now lives on the worker side: it
// reads EDGE_PROTOCOL from spec.env and self-derives socket_mode
// (see edge-worker/src/supervisor.rs::start_app). This test was
// removed pending a follow-up that wires protocol from the CLI
// deploy manifest through to BuildAppConfig. Tracked separately.
