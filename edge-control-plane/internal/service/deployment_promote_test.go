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
)

// expectActiveGet mocks an activeRepo.Get call (post-commit read in
// the same connection pool). Issue #546: sqlmock cannot introspect the
// parameter list of `INSERT … ON CONFLICT DO UPDATE` in
// ActiveDeploymentRepository.Set, so the value of
// last_good_deployment_id is invisible at the write boundary. The
// standard fix in this package is a follow-up read of the row after
// the tx commits: we mock the SELECT and then call svc.activeRepo.Get
// directly, asserting on the *string field of *domain.ActiveDeployment.
//
// `lastGood *string` matches the field type on domain.ActiveDeployment
// — pass nil for the first-promote (NULL) case, `pointerTo("d_prior")`
// for the second-promote (captured-prior) case. The 15-col projection
// mirrors ActiveDeploymentRepository.Get / GetForUpdate in
// repository/active_deployment.go:277-298:
//
//	tenant_id, app_name, deployment_id, last_good_deployment_id,
//	auto_rollback_enabled, stable_since, regions_published,
//	regions_failed, regions_cached, regions_cache_failed,
//	last_publish_at, last_publish_attempt_id, preview_id,
//	preview_pr_number, activation_attempt_started_at
//
// The mock must list 15 columns in that order — sqlmock enforces row
// arity via `AddRow(values...)` against the column list at mock time.
//
// Call AFTER drainer.Tick so the connection pool is free.
func expectActiveGet(mock sqlmock.Sqlmock, tenantID, appName, deploymentID string, lastGood *string) {
	mock.ExpectQuery(regexp.QuoteMeta(`FROM active_deployments WHERE tenant_id =`)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id", "last_good_deployment_id",
			"auto_rollback_enabled", "stable_since",
			"regions_published", "regions_failed", "regions_cached", "regions_cache_failed",
			"last_publish_at", "last_publish_attempt_id",
			"preview_id", "preview_pr_number", "activation_attempt_started_at",
		}).AddRow(
			tenantID, appName, deploymentID, lastGood, false, nil,
			nil, nil, nil, nil, nil, nil, nil, nil, nil,
		))
}

// pointerTo is a tiny helper used to build a *string literal at the
// call site. Equivalent to taking the address of a local variable
// but keeps the test bodies free of `tmp := "x"; return &tmp` ceremony.
func pointerTo[T any](v T) *T { return &v }

// TestPromoteDeployment_HappyPath_FirstPromote_LastGoodIsNull pins
// issue #546 contract (1): on the very first promote into a
// (tenant, app) slot (no prior active row), the new active row's
// last_good_deployment_id column is NULL. Failure of this test means
// "first promote now writes a non-NULL last_good" — a regression of
// the rollback UX (rollback relies on last_good being the
// previously-active id, which on a first-ever promote is NULL rather
// than a phantom old id).
//
// The deployment row carries preview metadata (issue #308) because
// promote is fundamentally the canary → production promotion
// workflow: deployment.AppName = "myapp--pr-42", promote target =
// "myapp". No prior active row exists for (tenant, myapp), so
// GetForUpdate returns sql.ErrNoRows and the Set INSERT writes
// last_good_deployment_id = NULL.
func TestPromoteDeployment_HappyPath_FirstPromote_LastGoodIsNull(t *testing.T) {
	pub := newRecordingPublisher()
	svc, drainer, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const (
		deploymentID    = "d_canary"
		tenantID        = "t_test"
		appName         = "myapp" // promote target — differs from deployment.AppName below
		deploymentHash  = "canaryhash42"
		previewID       = "pr-42"
		previewPRNumber = 42
	)
	previewExpires := time.Now().Add(1 * time.Hour)

	// 1. Deployment row — preview metadata stamped (issue #308):
	//    promote in production is the preview → production path, so
	//    this fixture represents the realistic canary case the issue
	//    describes. AppName has the `--pr-42` suffix the preview
	//    scaffolder stamps; the call promotes into "myapp".
	regionsCol := `{"us-east","eu-west","ap-south"}`
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
			AddRow(deploymentID, tenantID, "myapp--pr-42", domain.StatusDeployed, deploymentHash, regionsCol, time.Now(), false, "", "", []byte{}, 0, previewID, previewPRNumber, previewExpires))

	// 2. Tx body mirrors TestActivateDeployment_FansOutToAllRegions
	//    (deployment_regions_test.go:200-318) step for step — promote
	//    reuses activateDeployment verbatim.
	mock.ExpectBegin()
	expectTenantForUpdateOK(mock, tenantID)
	// First-ever promote for (tenant, app) → no prior active row.
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs(tenantID, appName).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, max_resident_seconds_per_month, max_compute_ms_per_month, used_outbound_bytes, used_request_count, used_memory_mb, used_resident_seconds, used_compute_ms, quota_period_start, quota_lock_grace_until FROM quotas WHERE tenant_id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "max_requests_per_month", "max_resident_seconds_per_month", "max_compute_ms_per_month", "used_outbound_bytes", "used_request_count", "used_memory_mb", "used_resident_seconds", "used_compute_ms", "quota_period_start", "quota_lock_grace_until"}).
			AddRow(tenantID, 100, 50, 10, 512, 1024, 100_000, 0, 0, 0, 0, 0, 0, 0, time.Now(), nil))
	// Env read is keyed on the promote target, NOT the deployment's
	// original AppName — that is one of the contracts under test.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))

	expectInTxOutboxInsert(mock, tenantID, appName)
	expectInTxMemoryAdd(mock, tenantID, 512)

	mock.ExpectCommit()

	// 3. Post-commit drainer tick: drives a single TaskMessage through
	//    3 publishes (one per region).
	expectDrainerTickSuccess(t, mock, tenantID, appName, deploymentID,
		[]string{"us-east", "eu-west", "ap-south"}, 512)

	if err := svc.PromoteDeployment(context.Background(), tenantID, appName, deploymentID, ""); err != nil {
		t.Fatalf("PromoteDeployment: %v", err)
	}
	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("post-Promote publisher calls = %v, want [] (durable outbox owns publish)", got)
	}
	drainer.Tick(context.Background())

	// 4. Published message body — keyed on the PROMOTE TARGET, not on
	//    deployment.AppName. This is one half of the app-name swap
	//    contract; the second half (Apps["myapp--pr-42"] absent) is
	//    the same check applied in TestPromoteDeployment_AppNameSwap_HappyPath.
	if len(pub.calls) != 3 {
		t.Fatalf("len(pub.calls) = %d, want 3", len(pub.calls))
	}
	for i, c := range pub.calls {
		app, ok := c.msg.Apps[appName]
		if !ok {
			t.Fatalf("call %d: pub.calls[%d].msg.Apps[%q] missing — promote target must be the wire key", i, i, appName)
		}
		if app.DeploymentID != deploymentID {
			t.Errorf("call %d: Apps[%q].DeploymentID = %q, want %q", i, appName, app.DeploymentID, deploymentID)
		}
		if app.MaxMemoryMB != 512 {
			t.Errorf("call %d: Apps[%q].MaxMemoryMB = %d, want 512", i, appName, app.MaxMemoryMB)
		}
	}

	// 5. last_good_deployment_id is NULL on the active row.
	expectActiveGet(mock, tenantID, appName, deploymentID, nil)
	ad, err := svc.activeRepo.Get(context.Background(), tenantID, appName)
	if err != nil {
		t.Fatalf("activeRepo.Get: %v", err)
	}
	if ad == nil {
		t.Fatalf("activeRepo.Get: nil row")
	}
	if ad.LastGoodDeploymentID != nil {
		t.Errorf("first promote: LastGoodDeploymentID = %q, want nil", *ad.LastGoodDeploymentID)
	}
	if ad.DeploymentID != deploymentID {
		t.Errorf("first promote: DeploymentID = %q, want %q", ad.DeploymentID, deploymentID)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// silenceUnused pulls in `database/sql` and `errors` only when the
// file is also built with later tests added; not strictly needed
// today but keeps the import set honest for the future
// negative-coverage commit (which uses `sql.ErrNoRows` and
// `errors.Is`). Lint-clean without it at present, but having it as a
// one-line guard avoids "unused import" churn when Test 4 lands.
var _ = sql.ErrNoRows
var _ = errors.Is
