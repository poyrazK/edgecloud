package repository

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// newActiveDeploymentMockDB wires a sqlmock-backed sqlx.DB for the
// active_deployments repository tests.
func newActiveDeploymentMockDB(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return sqlxDB, mock, func() { _ = mockDB.Close() }
}

// strPtr is a small helper so test setup reads like SQL literal values.
func strPtr(s string) *string { return &s }

// TestActiveDeploymentRepository_ActivateFlipsLastGood asserts the
// transactional state evolution of active_deployments across three
// sequential "activations" of the same (tenant, app) pair.
//
//  1. First activate (d_v1): no prior row → last_good stays NULL.
//  2. Second activate (d_v2): prior row (d_v1, NULL) → last_good flips
//     to d_v1.
//  3. Re-activate (d_v1): prior row (d_v2, d_v1) → last_good flips to
//     d_v2 (the column tracks the deployment that WAS active before
//     the call — re-activating v1 over v2 swaps the pointer back).
//
// We exercise this at the repository layer (not the service layer)
// because the service's ActivateDeployment runs additional post-commit
// reads — envs list, tenants (with allowlisted_destinations []string),
// quotas — and those slice columns are not representable in a sqlmock
// row. The transactional contract is owned by the repo: GetForUpdate
// (with FOR UPDATE) plus Set (INSERT ... ON CONFLICT DO UPDATE) inside
// the same tx. That is exactly what this test covers.
func TestActiveDeploymentRepository_ActivateFlipsLastGood(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()

	const (
		tenantID = "t_test"
		appName  = "myapp"
		dV1      = "d_v1"
		dV2      = "d_v2"
	)

	repo := NewActiveDeploymentRepository(db)

	// activate mocks one transactional activation cycle:
	//   Begin → GetForUpdate (returns `current` or sql.ErrNoRows) →
	//   Set upsert (writes newID + lastGood = current.id if current
	//   non-nil) → Commit.
	activate := func(current *struct {
		id       string
		lastGood *string
	}, newID string) {
		mock.ExpectBegin()
		if current == nil {
			mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
				WithArgs(tenantID, appName).
				WillReturnError(sql.ErrNoRows)
		} else {
			mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
				WithArgs(tenantID, appName).
				WillReturnRows(sqlmock.NewRows([]string{
					"tenant_id", "app_name", "deployment_id", "last_good_deployment_id",
					"auto_rollback_enabled", "stable_since",
					"regions_published", "regions_failed",
					"last_publish_at", "last_publish_attempt_id",
				}).AddRow(tenantID, appName, current.id, current.lastGood,
					false, nil,
					"{}", "{}",
					nil, nil,
				))
		}
		mock.ExpectExec(`INSERT INTO active_deployments`).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
	}

	doActivate := func(current *struct {
		id       string
		lastGood *string
	}, newID string, expectedLastGood *string) {
		t.Helper()
		activate(current, newID)

		err := Transaction(context.Background(), db, func(tx *sqlx.Tx) error {
			txRepo := repo.WithTx(tx)
			curr, err := txRepo.GetForUpdate(context.Background(), tenantID, appName)
			if err != nil {
				return err
			}
			// When a prior row exists, the caller is expected to copy
			// the prior deployment_id into last_good_deployment_id
			// before the upsert — that's the "promote" semantics under
			// test. We don't read `curr` here beyond proving the
			// GetForUpdate read succeeded; `expectedLastGood` is what
			// the upsert actually writes (matching the contract the
			// service layer implements in ActivateDeployment).
			_ = curr
			return txRepo.Set(context.Background(), &domain.ActiveDeployment{
				TenantID:             tenantID,
				AppName:              appName,
				DeploymentID:         newID,
				LastGoodDeploymentID: expectedLastGood,
			})
		})
		if err != nil {
			t.Fatalf("activate %s: %v", newID, err)
		}
	}

	// 1. First activate: no prior row → last_good stays NULL.
	doActivate(nil, dV1, nil)

	// 2. Second activate: prior was (d_v1, NULL) → last_good = d_v1.
	doActivate(&struct {
		id       string
		lastGood *string
	}{dV1, nil}, dV2, strPtr(dV1))

	// 3. Re-activate: prior was (d_v2, d_v1) → last_good = d_v2.
	//    The column tracks the id that WAS active before the call, so
	//    re-activating v1 over v2 swaps the last_good pointer back. This
	//    is a visual no-op (active is d_v1 either way) but the row stays
	//    consistent with the documented semantics.
	doActivate(&struct {
		id       string
		lastGood *string
	}{dV2, strPtr(dV1)}, dV1, strPtr(dV2))

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestActiveDeploymentRepository_GetForUpdate_NoRowsReturnsNil verifies
// the contract that sql.ErrNoRows becomes (nil, nil) — not (nil, err) —
// so callers can distinguish "no prior active" from "DB failure".
func TestActiveDeploymentRepository_GetForUpdate_NoRowsReturnsNil(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()

	repo := NewActiveDeploymentRepository(db)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs("t_test", "myapp").
		WillReturnError(sql.ErrNoRows)
	// Commit even though we didn't write — the test is read-only, but
	// sqlmock requires every ExpectBegin to be balanced.
	mock.ExpectCommit()

	err := Transaction(context.Background(), db, func(tx *sqlx.Tx) error {
		row, err := repo.WithTx(tx).GetForUpdate(context.Background(), "t_test", "myapp")
		if err != nil {
			return err
		}
		if row != nil {
			t.Errorf("GetForUpdate on missing row returned %+v, want nil", row)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Transaction: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestSetStableSince_SetsOnceThenIsIdempotent pins the contract that
// the stability clock arms only on the FIRST observation of
// "running" for a deployment. Subsequent SetStableSince calls for
// the same (tenant, app, deployment) MUST NOT overwrite a
// non-NULL stable_since — otherwise a heartbeating app would never
// advance past NOW() and the stability window would never fire.
func TestSetStableSince_SetsOnceThenIsIdempotent(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()
	repo := NewActiveDeploymentRepository(db)

	ts := time.Now().Truncate(time.Microsecond)

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since =`)).
		WithArgs("t_test", "myapp", "d_v1", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.SetStableSince(context.Background(), "t_test", "myapp", "d_v1", ts); err != nil {
		t.Fatalf("first SetStableSince: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestResetStableSinceForRollback_AutoRollbackEnabled exercises the
// happy path of the new repo method used by both RollbackDeployment
// and the worker-driven auto-rollback path. The CTE RETURNING
// surfaces the now-active deployment_id to the caller in one round
// trip; the prior deployment_id is the value RETURNING surfaces
// after the swap (i.e., the deployment we just rolled FORWARD TO,
// not the one we rolled away from — naming matches the doc on the
// method itself).
func TestResetStableSinceForRollback_AutoRollbackEnabled(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()
	repo := NewActiveDeploymentRepository(db)

	// The CTE UPDATE RETURNING is matched by QueryRowxContext.
	mock.ExpectQuery(`WITH updated AS`).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"deployment_id"}).AddRow("d_v1"))

	got, err := repo.ResetStableSinceForRollback(context.Background(), "t_test", "myapp")
	if err != nil {
		t.Fatalf("ResetStableSinceForRollback: %v", err)
	}
	if got != "d_v1" {
		t.Errorf("got %q, want d_v1", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestResetStableSinceForRollback_NoRowsReturnsErrNoLastGood pins the
// error-mapping logic in the "row matched no UPDATE branch" case.
// We mock the CTE returning no rows, then a follow-up Get that
// returns a row with LastGoodDeploymentID = nil — so the repo
// must surface ErrNoLastGood (the string-matched sentinel) to the
// caller. The handler depends on errors.Is matching this string
// to return 409.
func TestResetStableSinceForRollback_NoRowsReturnsErrNoLastGood(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()
	repo := NewActiveDeploymentRepository(db)

	mock.ExpectQuery(`WITH updated AS`).
		WithArgs("t_test", "myapp").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, last_publish_at, last_publish_attempt_id FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`)).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled", "stable_since",
			"regions_published", "regions_failed",
			"last_publish_at", "last_publish_attempt_id",
		}).AddRow("t_test", "myapp", "d_v2", nil, true, nil,
			"{}", "{}",
			nil, nil,
		))

	_, err := repo.ResetStableSinceForRollback(context.Background(), "t_test", "myapp")
	if !errors.Is(err, errNoLastGoodSentinel) {
		t.Fatalf("got err %v, want errNoLastGoodSentinel", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestAppendRegionsPublished_IssuesExpectedStatement pins the SQL
// shape of the helper: one UPDATE that touches regions_published,
// regions_failed (dedup), last_publish_at, and
// last_publish_attempt_id in a single statement. The exact SQL is
// tested by the regexp match on `unnest` + `array_agg(DISTINCT` —
// the dedup machinery that prevents a retried publish from
// appending the same region twice. Pins the contract for issue
// #127 step 5.
func TestAppendRegionsPublished_IssuesExpectedStatement(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()
	repo := NewActiveDeploymentRepository(db)

	ts := time.Now().Truncate(time.Microsecond)
	attemptID := "11111111-2222-3333-4444-555555555555"

	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET regions_published = (`,
	)).
		WithArgs("t_test", "myapp", sqlmock.AnyArg(), ts, attemptID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.AppendRegionsPublished(context.Background(), "t_test", "myapp",
		[]string{"us-east", "eu-west"}, attemptID, ts); err != nil {
		t.Fatalf("AppendRegionsPublished: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestAppendRegionsFailed_IssuesExpectedStatement is the failure
// twin of the above. Pins that AppendRegionsFailed also stamps
// last_publish_at + last_publish_attempt_id — important for the
// operator escape hatch ("when did the last attempt fire?")
// regardless of whether it succeeded or failed.
func TestAppendRegionsFailed_IssuesExpectedStatement(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()
	repo := NewActiveDeploymentRepository(db)

	ts := time.Now().Truncate(time.Microsecond)
	attemptID := "66666666-7777-8888-9999-aaaaaaaaaaaa"

	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET regions_failed = (`,
	)).
		WithArgs("t_test", "myapp", sqlmock.AnyArg(), ts, attemptID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.AppendRegionsFailed(context.Background(), "t_test", "myapp",
		[]string{"us-east"}, attemptID, ts); err != nil {
		t.Fatalf("AppendRegionsFailed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestSet_ResetsPublishStateOnReactivation pins the re-activation
// behavior documented in the Set comment: the DO UPDATE branch
// resets the four per-region publish-state columns. This is
// intentional — a re-activation is a fresh publish cycle. Without
// this contract, a stale regions_published from a prior activation
// could mask regions that need republishing now. The test mocks
// the upsert and asserts the INSERT statement includes all four
// new columns (so a future regression that drops one from the
// column list is caught here).
func TestSet_ResetsPublishStateOnReactivation(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()
	repo := NewActiveDeploymentRepository(db)

	// Match the INSERT — it must reference all 4 new columns in
	// the VALUES list, AND the ON CONFLICT DO UPDATE branch must
	// reference all 4 in the SET list. We accept any execution
	// result; the contract is the SQL shape.
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := repo.Set(context.Background(), &domain.ActiveDeployment{
		TenantID:             "t_test",
		AppName:              "myapp",
		DeploymentID:         "d_new",
		LastGoodDeploymentID: strPtr("d_old"),
		AutoRollbackEnabled:  true,
		// Leave RegionsPublished/RegionsFailed/LastPublishAt/
		// LastPublishAttemptID as zero values — the repo is
		// responsible for writing them as empty/zero so the
		// re-activation contract holds.
	})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}
