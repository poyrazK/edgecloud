package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/lib/pq"
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

// TestPromoteDeployment_HappyPath_SecondPromote_CapturesPriorID
// pins issue #546 contract (2): with a pre-existing active row at
// (tenant, app) carrying deployment_id = "d_canary", a subsequent
// promote of "d_canary_2" into the same slot writes
// last_good_deployment_id = pointerTo("d_canary") so rollback
// (issue #74) can swap them back atomically.
//
// Failure mode this catches: a future refactor of activateDeployment
// stops copying current.DeploymentID into lastGood → a re-promote
// silently loses the prior active id, and rollback silently 404s
// because last_good is now NULL on a row that should know its
// predecessor.
func TestPromoteDeployment_HappyPath_SecondPromote_CapturesPriorID(t *testing.T) {
	pub := newRecordingPublisher()
	svc, drainer, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const (
		priorDeploymentID = "d_canary"
		newDeploymentID   = "d_canary_2"
		tenantID          = "t_test"
		appName           = "myapp"
		deploymentHash    = "canaryhash43"
		previewID         = "pr-43"
		previewPRNumber   = 43
	)
	previewExpires := time.Now().Add(1 * time.Hour)

	// 1. GetByID — second promote targets the same (tenant, app) slot
	//    as the prior promote. Deployment row has preview metadata
	//    stamped; AppName still uses the --pr-43 suffix because the
	//    preview app name is unchanged on the deployments row even
	//    though promote will move the active pointer for "myapp".
	regionsCol := `{"us-east"}`
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
		WithArgs(newDeploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
			AddRow(newDeploymentID, tenantID, "myapp--pr-43", domain.StatusDeployed, deploymentHash, regionsCol, time.Now(), false, "", "", []byte{}, 0, previewID, previewPRNumber, previewExpires))

	// 2. Tx body mirrors Test 1 exactly, except GetForUpdate now
	//    returns the prior active row. 15-col projection matches
	//    ActiveDeploymentRepository.GetForUpdate at
	//    repository/active_deployment.go:290-298.
	mock.ExpectBegin()
	expectTenantForUpdateOK(mock, tenantID)
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id", "last_good_deployment_id",
			"auto_rollback_enabled", "stable_since",
			"regions_published", "regions_failed", "regions_cached", "regions_cache_failed",
			"last_publish_at", "last_publish_attempt_id",
			"preview_id", "preview_pr_number", "activation_attempt_started_at",
		}).AddRow(
			tenantID, appName, priorDeploymentID, nil /* last_good on the existing row */, false, nil,
			nil, nil, nil, nil, nil, nil, nil, nil, nil,
		))
	// The INSERT … ON CONFLICT DO UPDATE writes the new active row.
	// The matcher regex covers both arms; sqlmock cannot introspect
	// which on-conflict path was taken at runtime, only that one of
	// the two ran.
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, max_resident_seconds_per_month, max_compute_ms_per_month, used_outbound_bytes, used_request_count, used_memory_mb, used_resident_seconds, used_compute_ms, quota_period_start, quota_lock_grace_until FROM quotas WHERE tenant_id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "max_requests_per_month", "max_resident_seconds_per_month", "max_compute_ms_per_month", "used_outbound_bytes", "used_request_count", "used_memory_mb", "used_resident_seconds", "used_compute_ms", "quota_period_start", "quota_lock_grace_until"}).
			AddRow(tenantID, 100, 50, 10, 512, 1024, 100_000, 0, 0, 0, 0, 0, 0, 0, time.Now(), nil))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))

	expectInTxOutboxInsert(mock, tenantID, appName)
	expectInTxMemoryAdd(mock, tenantID, 512)

	mock.ExpectCommit()
	expectDrainerTickSuccess(t, mock, tenantID, appName, newDeploymentID,
		[]string{"us-east"}, 512)

	if err := svc.PromoteDeployment(context.Background(), tenantID, appName, newDeploymentID, ""); err != nil {
		t.Fatalf("PromoteDeployment: %v", err)
	}
	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("post-Promote publisher calls = %v, want [] (durable outbox owns publish)", got)
	}
	drainer.Tick(context.Background())

	// After the drainer tick, pub.calls carries the per-region
	// publish. body sanity (key on promote target, single region).
	if len(pub.calls) != 1 {
		t.Fatalf("len(pub.calls) = %d, want 1", len(pub.calls))
	}
	if app, ok := pub.calls[0].msg.Apps[appName]; !ok {
		t.Fatalf("Apps[%q] missing on the wire", appName)
	} else if app.DeploymentID != newDeploymentID {
		t.Errorf("Apps[%q].DeploymentID = %q, want %q", appName, app.DeploymentID, newDeploymentID)
	}

	// The contract: last_good_deployment_id is pointerTo(priorDeploymentID).
	// sqlmock cannot prove this at the write site (INSERT … ON CONFLICT
	// DO UPDATE args are opaque), so we read the row back via
	// activeRepo.Get after the tx commits.
	expectActiveGet(mock, tenantID, appName, newDeploymentID, pointerTo(priorDeploymentID))
	ad, err := svc.activeRepo.Get(context.Background(), tenantID, appName)
	if err != nil {
		t.Fatalf("activeRepo.Get: %v", err)
	}
	if ad == nil {
		t.Fatalf("activeRepo.Get: nil row")
	}
	if ad.LastGoodDeploymentID == nil {
		t.Fatalf("second promote: LastGoodDeploymentID = nil, want %q", priorDeploymentID)
	}
	if *ad.LastGoodDeploymentID != priorDeploymentID {
		t.Errorf("second promote: LastGoodDeploymentID = %q, want %q", *ad.LastGoodDeploymentID, priorDeploymentID)
	}
	if ad.DeploymentID != newDeploymentID {
		t.Errorf("second promote: DeploymentID = %q, want %q", ad.DeploymentID, newDeploymentID)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestPromoteDeployment_AppNameSwap_HappyPath pins the issue's
// defining contract: a deployment with AppName = "myapp--pr-N" (the
// preview app) is promoted into "myapp" (the production app). The
// published TaskMessage is keyed on "myapp", NOT on
// "myapp--pr-N"; exactly one entry in Apps; preview metadata from
// the deployments row is carried onto the wire (issue #308).
//
// Failure mode this catches: a future refactor that threads
// deployment.AppName through activateDeployment instead of
// targetAppName → workers receive TaskMessages with
// `<tenant>-myapp--pr-N.edgecloud.dev` app configs instead of
// `<tenant>-myapp.edgecloud.dev`. The route-table key is the wire
// Apps key (see edge-ingress routing), so this regression would
// silently break the production app's traffic.
//
// Setup mirrors Test 2 (active row already exists); we reuse the
// same GetForUpdate row so this test reads as "and in the
// app-name-swap branch, the wire key is X, not Y".
func TestPromoteDeployment_AppNameSwap_HappyPath(t *testing.T) {
	pub := newRecordingPublisher()
	svc, drainer, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const (
		priorDeploymentID = "d_canary"
		newDeploymentID   = "d_canary_2"
		tenantID          = "t_test"
		appName           = "myapp"        // promote target
		deployAppName     = "myapp--pr-42" // preview app name on the deployments row
		deploymentHash    = "canaryhash42"
		previewID         = "pr-42"
		previewPRNumber   = 42
	)
	previewExpires := time.Now().Add(1 * time.Hour)

	regionsCol := `{"us-east"}`
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
		WithArgs(newDeploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
			AddRow(newDeploymentID, tenantID, deployAppName, domain.StatusDeployed, deploymentHash, regionsCol, time.Now(), false, "", "", []byte{}, 0, previewID, previewPRNumber, previewExpires))

	mock.ExpectBegin()
	expectTenantForUpdateOK(mock, tenantID)
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id", "last_good_deployment_id",
			"auto_rollback_enabled", "stable_since",
			"regions_published", "regions_failed", "regions_cached", "regions_cache_failed",
			"last_publish_at", "last_publish_attempt_id",
			"preview_id", "preview_pr_number", "activation_attempt_started_at",
		}).AddRow(
			tenantID, appName, priorDeploymentID, nil, false, nil,
			nil, nil, nil, nil, nil, nil, nil, nil, nil,
		))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, max_resident_seconds_per_month, max_compute_ms_per_month, used_outbound_bytes, used_request_count, used_memory_mb, used_resident_seconds, used_compute_ms, quota_period_start, quota_lock_grace_until FROM quotas WHERE tenant_id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "max_requests_per_month", "max_resident_seconds_per_month", "max_compute_ms_per_month", "used_outbound_bytes", "used_request_count", "used_memory_mb", "used_resident_seconds", "used_compute_ms", "quota_period_start", "quota_lock_grace_until"}).
			AddRow(tenantID, 100, 50, 10, 512, 1024, 100_000, 0, 0, 0, 0, 0, 0, 0, time.Now(), nil))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))

	expectInTxOutboxInsert(mock, tenantID, appName)
	expectInTxMemoryAdd(mock, tenantID, 512)

	mock.ExpectCommit()

	// 3. Drainer tick — use a custom payload that includes the
	//    preview fields on the wire so the AppNameSwap_HappyPath
	//    assertions on Apps["myapp"].PreviewID / .PreviewPRNumber
	//    exercise the production payload shape. The shared
	//    expectDrainerTickSuccess helper builds a payload with only
	//    {DeploymentID, MaxMemoryMB, Env}, dropping preview fields;
	//    we want the realistic wire shape here, so we inline.
	payload, err := json.Marshal(&nats.TaskMessage{
		Type:     nats.TaskMessageKindTaskUpdate,
		TenantID: tenantID,
		Apps: map[string]nats.AppConfig{
			appName: {
				DeploymentID:    newDeploymentID,
				MaxMemoryMB:     512,
				Env:             map[string]string{},
				PreviewID:       previewID,
				PreviewPRNumber: previewPRNumber,
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal TaskMessage: %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta(`WITH due AS (`)).
		WithArgs(50).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "app_name", "kind", "payload", "regions",
			"attempt_count", "next_attempt_at", "status", "last_error",
			"dedupe_key", "created_at", "published_at", "claimed_until",
		}).AddRow(
			1, tenantID, appName, "task_update",
			payload,
			pq.Array([]string{"us-east"}),
			0, time.Now(), "in_flight", nil,
			"dedupe", time.Now(), nil, time.Now().Add(30*time.Second),
		))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := svc.PromoteDeployment(context.Background(), tenantID, appName, newDeploymentID, ""); err != nil {
		t.Fatalf("PromoteDeployment: %v", err)
	}
	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("post-Promote publisher calls = %v, want [] (durable outbox owns publish)", got)
	}
	drainer.Tick(context.Background())

	if len(pub.calls) != 1 {
		t.Fatalf("len(pub.calls) = %d, want 1", len(pub.calls))
	}
	wire := pub.calls[0].msg

	// 1. Exactly one entry in Apps — promote produces one wire key.
	if got, want := len(wire.Apps), 1; got != want {
		t.Errorf("len(Apps) = %d, want %d (promote target must be the only key)", got, want)
	}
	// 2. The promote target IS the key.
	app, ok := wire.Apps[appName]
	if !ok {
		t.Fatalf("Apps[%q] missing — promote target must be the wire key", appName)
	}
	// 3. The preview app name from the deployments row is NOT a key
	//    (would route to a non-existent WorkerApps for "myapp--pr-42").
	if _, leaked := wire.Apps[deployAppName]; leaked {
		t.Errorf("Apps[%q] unexpectedly present — preview app name must not leak onto the wire", deployAppName)
	}
	// 4. Preview metadata propagates from the deployments row onto
	//    the wire (issue #308). Even after promote, downstream
	//    observers can tell this row was promoted from a preview.
	if app.PreviewID != previewID {
		t.Errorf("Apps[%q].PreviewID = %q, want %q", appName, app.PreviewID, previewID)
	}
	if app.PreviewPRNumber != previewPRNumber {
		t.Errorf("Apps[%q].PreviewPRNumber = %d, want %d", appName, app.PreviewPRNumber, previewPRNumber)
	}
	if app.DeploymentID != newDeploymentID {
		t.Errorf("Apps[%q].DeploymentID = %q, want %q", appName, app.DeploymentID, newDeploymentID)
	}
	if app.MaxMemoryMB != 512 {
		t.Errorf("Apps[%q].MaxMemoryMB = %d, want 512", appName, app.MaxMemoryMB)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestPromoteDeployment_HappyPath_FansOutToAllRegions is the
// regression-safety net for the per-region publish loop
// specifically. PromoteDeployment is a 7-line wrapper around the
// same activateDeployment helper that ActivateDeployment uses
// (internal/service/deployment.go:1310-1323), so the per-region
// fan-out is inherited verbatim — but a future refactor that
// collapses PromoteDeployment into a separate code path could
// silently lose the loop. This test fails immediately on such a
// regression with a clear "promote broke fan-out" message.
//
// Near-clone of TestActivateDeployment_FansOutToAllRegions
// (deployment_regions_test.go:200-318); only deltas are
// (a) deployment row's AppName is a preview name with preview
// metadata stamped, and (b) the call uses PromoteDeployment
// instead of ActivateDeployment. Setup is otherwise identical:
// 3 regions in, 3 publishes out, identical task message body
// across all three.
func TestPromoteDeployment_HappyPath_FansOutToAllRegions(t *testing.T) {
	pub := newRecordingPublisher()
	svc, drainer, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const (
		deploymentID    = "d_canary"
		tenantID        = "t_test"
		appName         = "myapp"
		deploymentHash  = "canaryhash42"
		previewID       = "pr-42"
		previewPRNumber = 42
	)
	previewExpires := time.Now().Add(1 * time.Hour)

	regionsCol := `{"us-east","eu-west","ap-south"}`
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
			AddRow(deploymentID, tenantID, "myapp--pr-42", domain.StatusDeployed, deploymentHash, regionsCol, time.Now(), false, "", "", []byte{}, 0, previewID, previewPRNumber, previewExpires))

	mock.ExpectBegin()
	expectTenantForUpdateOK(mock, tenantID)
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))

	expectInTxOutboxInsert(mock, tenantID, appName)
	expectInTxMemoryAdd(mock, tenantID, 512)

	mock.ExpectCommit()
	expectDrainerTickSuccess(t, mock, tenantID, appName, deploymentID,
		[]string{"us-east", "eu-west", "ap-south"}, 512)

	if err := svc.PromoteDeployment(context.Background(), tenantID, appName, deploymentID, ""); err != nil {
		t.Fatalf("PromoteDeployment: %v", err)
	}
	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("post-Promote publisher calls = %v, want [] (durable outbox owns publish)", got)
	}
	drainer.Tick(context.Background())

	// 3 publishes, one per region, in deployment row's order.
	gotRegions := pub.regionsCalled()
	wantRegions := []string{"us-east", "eu-west", "ap-south"}
	if !equalStringSlices(gotRegions, wantRegions) {
		t.Errorf("publish regions = %v, want %v", gotRegions, wantRegions)
	}

	// All three publishes must use the same TaskMessage body — only
	// the region arg differs. Specifically: same DeploymentID, hash
	// (zero here, but the slot is identical), and MaxMemoryMB.
	if len(pub.calls) != 3 {
		t.Fatalf("len(pub.calls) = %d, want 3", len(pub.calls))
	}
	first := pub.calls[0].msg.Apps[appName]
	if first.MaxMemoryMB != 512 {
		t.Errorf("call 0: MaxMemoryMB = %d, want 512", first.MaxMemoryMB)
	}
	if first.DeploymentID != deploymentID {
		t.Errorf("call 0: DeploymentID = %q, want %q", first.DeploymentID, deploymentID)
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

// TestPromoteDeployment_DeploymentNotFound_404AtServiceLayer pins
// issue #546 contract (6): PromoteDeployment surfaces
// ErrDeploymentNotFound (the typed sentinel at
// internal/service/deployment.go:227-229) for both "row absent"
// (GetByID returns (nil, nil) on sql.ErrNoRows) and "wrong tenant"
// (row exists with mismatched tenant_id). The handler maps this
// sentinel to 404 via errors.Is at deployment.go:1017. Both
// branches short-circuit before any tx work — no ExpectBegin.
//
// Mirrors the handler-level 404 mapping that is not currently
// covered by deployment_promote_test.go (only the 409 and
// idempotency-key paths are). Service-layer coverage is the issue
// scope; handler-level coverage is a separate gap.
func TestPromoteDeployment_DeploymentNotFound_404AtServiceLayer(t *testing.T) {
	t.Run("RowAbsent", func(t *testing.T) {
		pub := newRecordingPublisher()
		svc, _, mock, cleanup := activateSvcForTest(t, pub, "global")
		defer cleanup()

		const (
			deploymentID = "d_missing"
			tenantID     = "t_test"
			appName      = "myapp"
		)

		// DeploymentRepository.GetByID returns (nil, nil) on
		// sql.ErrNoRows (repository/deployment.go:69-71), so the
		// sentinel is returned on err != nil OR deployment == nil.
		// sqlmock surfaces the ErrNoRows at the mock level.
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
			WithArgs(deploymentID).
			WillReturnError(sql.ErrNoRows)

		err := svc.PromoteDeployment(context.Background(), tenantID, appName, deploymentID, "")
		if !errors.Is(err, ErrDeploymentNotFound) {
			t.Errorf("err = %v, want ErrDeploymentNotFound", err)
		}
		if got := pub.regionsCalled(); len(got) != 0 {
			t.Errorf("publisher calls = %v, want [] (no tx → no outbox → no publish)", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("sqlmock expectations not met: %v", err)
		}
	})

	t.Run("WrongTenant", func(t *testing.T) {
		pub := newRecordingPublisher()
		svc, _, mock, cleanup := activateSvcForTest(t, pub, "global")
		defer cleanup()

		const (
			deploymentID     = "d_other"
			callerTenantID   = "t_test"
			deploymentTenant = "t_other" // different from callerTenantID
			appName          = "myapp--pr-99"
		)

		// GetByID returns a real row, but deployment.TenantID !=
		// callerTenantID → the second branch in PromoteDeployment
		// returns ErrDeploymentNotFound. Production code intentionally
		// collapses 'wrong tenant' into 'not found' so a tenant can't
		// probe deployment ids belonging to other tenants.
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
			WithArgs(deploymentID).
			WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
				AddRow(deploymentID, deploymentTenant, appName, domain.StatusDeployed, "h", `{"us-east"}`, time.Now(), false, "", "", []byte{}, 0, nil, nil, nil))

		err := svc.PromoteDeployment(context.Background(), callerTenantID, "myapp", deploymentID, "")
		if !errors.Is(err, ErrDeploymentNotFound) {
			t.Errorf("err = %v, want ErrDeploymentNotFound", err)
		}
		if got := pub.regionsCalled(); len(got) != 0 {
			t.Errorf("publisher calls = %v, want [] (no tx → no outbox → no publish)", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("sqlmock expectations not met: %v", err)
		}
	})
}
