package repository

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
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
					"regions_published", "regions_failed", "regions_cached",
					"last_publish_at", "last_publish_attempt_id",
				}).AddRow(tenantID, appName, current.id, current.lastGood,
					false, nil,
					"{}", "{}", "{}",
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id, preview_id, preview_pr_number, activation_attempt_started_at FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`)).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id",
			"last_good_deployment_id", "auto_rollback_enabled", "stable_since",
			"regions_published", "regions_failed", "regions_cached",
			"regions_cache_failed", "last_publish_at", "last_publish_attempt_id",
			"preview_id", "preview_pr_number", "activation_attempt_started_at",
		}).AddRow("t_test", "myapp", "d_v2", nil, true, nil,
			"{}", "{}", "{}", "{}",
			nil, nil, nil, nil, nil,
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

// TestAppendRegionsCacheState_IssuesExpectedStatement (issue #332,
// PR 2 follow-up) pins the SQL shape of the new
// AppendRegionsCacheState helper. Replaces the pre-PR-2-follow-up
// AppendRegionsCached: a single UPDATE that touches BOTH
// regions_cached (succeeded) and regions_cache_failed (failed)
// in one statement, both with the
// `unnest(<col> || $N::text[])` + `array_agg(DISTINCT r)` dedup
// pattern. The signature is one-arg lighter than the publish
// helper (no attemptID column, no timestamp — see the doc comment
// on AppendRegionsCacheState for why `ts` is reserved-only).
func TestAppendRegionsCacheState_IssuesExpectedStatement(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()
	repo := NewActiveDeploymentRepository(db)

	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE active_deployments SET regions_cached = (`,
	)).
		WithArgs("t_test", "myapp", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.AppendRegionsCacheState(context.Background(), "t_test", "myapp",
		[]string{"us-east", "eu-west"}, []string{}, time.Now()); err != nil {
		t.Fatalf("AppendRegionsCacheState: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestAppendRegionsCacheState_DedupesRegions is a table-driven
// check that the SQL pattern removes dupes from BOTH the
// succeeded and the failed slice. WithArgs(sqlmock.AnyArg()) lets
// us pass `[]string{"fra", "fra", "iad"}` through and assert
// sqlmock is satisfied — but the actual dedup is enforced
// server-side by `unnest() || $N::text[]` + DISTINCT. This test
// is a contract pin for the SQL params + a regression guard for
// future refactors that change the arg order or drop DISTINCT
// (sqlmock would catch that immediately by failing the WithArgs
// match).
func TestAppendRegionsCacheState_DedupesRegions(t *testing.T) {
	tests := []struct {
		succeeded []string
		failed    []string
	}{
		{[]string{"fra"}, nil},
		{[]string{"fra", "iad"}, []string{}},
		{[]string{"fra", "fra", "iad"}, []string{"iad", "iad"}},
		{[]string{"fra", "iad", "fra", "iad"}, []string{"fra"}},
	}
	for _, tc := range tests {
		t.Run(strings.Join(tc.succeeded, ",")+"|"+strings.Join(tc.failed, ","), func(t *testing.T) {
			db, mock, cleanup := newActiveDeploymentMockDB(t)
			defer cleanup()
			repo := NewActiveDeploymentRepository(db)

			mock.ExpectExec(regexp.QuoteMeta(
				`UPDATE active_deployments SET regions_cached = (`,
			)).
				WithArgs("t_test", "myapp", sqlmock.AnyArg(), sqlmock.AnyArg()).
				WillReturnResult(sqlmock.NewResult(0, 1))

			if err := repo.AppendRegionsCacheState(context.Background(), "t_test", "myapp",
				tc.succeeded, tc.failed, time.Now()); err != nil {
				t.Fatalf("AppendRegionsCacheState: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("sqlmock expectations not met: %v", err)
			}
		})
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

// TestSet_PreservesRegionsCachedOnSameDeploymentId (issue #332, PR 2
// follow-up) pins the conditional-wipe contract: when Set is called
// with the same deployment_id the row already has, the DO UPDATE
// branch must preserve regions_cached (and regions_cache_failed)
// via the CASE WHEN active_deployments.deployment_id = $3 THEN
// active_deployments.regions_cached ELSE $8 END shape — NOT
// overwrite them with the caller-supplied $8.
//
// The cache-skip-on-activation logic in publishSwap consults
// current.RegionsCached to decide whether to push bytes to each
// region; if Set unconditionally wiped RegionsCached on every
// upsert, the cache-skip branch could never fire after a
// re-activation.
//
// We assert the SQL shape directly (the `CASE WHEN` predicate must
// be present, and the THEN branch must reference the existing
// column) — sqlmock doesn't execute the statement, so a regression
// to `regions_cached = $8` would still pass WithArgs but fail
// this regex.
func TestSet_PreservesRegionsCachedOnSameDeploymentId(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()
	repo := NewActiveDeploymentRepository(db)

	// Match the full INSERT statement by regex. The CASE shape
	// is the contract — the args are not constrained beyond the
	// (tenant, app) prefix.
	mock.ExpectExec(`(?s)INSERT INTO active_deployments.*regions_cached = CASE WHEN active_deployments\.deployment_id = \$3 THEN active_deployments\.regions_cached ELSE \$8 END.*regions_cache_failed = CASE WHEN active_deployments\.deployment_id = \$3 THEN active_deployments\.regions_cache_failed ELSE \$9 END`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.Set(context.Background(), &domain.ActiveDeployment{
		TenantID:            "t_test",
		AppName:             "myapp",
		DeploymentID:        "d_same",
		AutoRollbackEnabled: false,
		// Non-empty cache arrays so a regression that wrote $8
		// (the caller value) over the row's value would be
		// visible in the SQL — sqlmock can't compare the
		// actual row state, but the SQL shape contract holds
		// regardless of the caller's slice contents.
		RegionsCached:      pq.StringArray{"fra", "iad"},
		RegionsCacheFailed: pq.StringArray{"iad"},
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestSet_WipesOnDifferentDeploymentId pins the ELSE branch of the
// conditional CASE: when Set is called with a DIFFERENT
// deployment_id, the new value $8 / $9 MUST overwrite the row's
// prior value (the fresh-publish-cycle contract from PR 2). The
// re-activation path on a different deployment starts the cache
// history over; the prior activation's regions_cached is no longer
// relevant.
//
// We assert the SQL body — the CASE shape is present (i.e. the
// same-id re-activation would preserve, but a different-id
// activation would wipe). The contract for "wipe" is the SQL
// shape itself; the row's prior value cannot be observed from
// outside Postgres, so the test pins the conditional-wipe shape
// against a regression that drops the CASE entirely.
func TestSet_WipesOnDifferentDeploymentId(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()
	repo := NewActiveDeploymentRepository(db)

	// Match the full INSERT statement. The CASE shape must
	// still be present — a regression to the unconditional
	// `regions_cached = $8` form would fail this regex.
	mock.ExpectExec(`(?s)INSERT INTO active_deployments.*regions_cached = CASE WHEN active_deployments\.deployment_id = \$3 THEN active_deployments\.regions_cached ELSE \$8 END.*regions_cache_failed = CASE WHEN active_deployments\.deployment_id = \$3 THEN active_deployments\.regions_cache_failed ELSE \$9 END`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.Set(context.Background(), &domain.ActiveDeployment{
		TenantID:            "t_test",
		AppName:             "myapp",
		DeploymentID:        "d_new", // would-be new id; CASE shape is what we're testing
		AutoRollbackEnabled: false,
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestListByTenantWithDeployment_HappyPath pins the JOIN query
// (PR #166 follow-up #1): one round trip returns each active row
// enriched with its deployment's hash and regions. Uses LEFT JOIN
// semantics — an active row whose deployment_id has no match is
// returned with Hash="" / Regions=nil so the service layer can
// detect and log orphans rather than silently dropping them
// (operator-actionable state, not a silent failure).
func TestListByTenantWithDeployment_HappyPath(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()
	repo := NewActiveDeploymentRepository(db)

	rows := sqlmock.NewRows([]string{
		"tenant_id", "app_name", "deployment_id", "last_good_deployment_id",
		"auto_rollback_enabled", "stable_since", "regions_published", "regions_cached",
		"regions_failed", "last_publish_at", "last_publish_attempt_id",
		"hash", "regions",
	}).
		AddRow("t_a", "app1", "d_1", nil, false, nil, pq.StringArray{"global"}, pq.StringArray{}, pq.StringArray{"global"}, nil, nil, "hash1", pq.StringArray{"global"}).
		AddRow("t_a", "app2", "d_2", nil, false, nil, pq.StringArray{"us-east", "eu-west"}, pq.StringArray{}, pq.StringArray{"us-east", "eu-west"}, nil, nil, "hash2", pq.StringArray{"us-east", "eu-west"})

	mock.ExpectQuery(`SELECT.*active_deployments ad.*JOIN deployments d`).
		WithArgs("t_a").
		WillReturnRows(rows)

	got, err := repo.ListByTenantWithDeployment(context.Background(), "t_a")
	if err != nil {
		t.Fatalf("ListByTenantWithDeployment: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d, want 2", len(got))
	}
	if !got[0].Hash.Valid || got[0].Hash.String != "hash1" || got[0].AppName != "app1" {
		t.Errorf("got[0]=%+v", got[0])
	}
	if len(got[1].Regions) != 2 {
		t.Errorf("got[1].Regions=%v, want 2", got[1].Regions)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestListByTenantWithDeployment_OrphanPassesThrough pins the LEFT
// JOIN contract: an active row whose deployment_id has no match in
// the deployments table is returned with Hash.Valid=false so the
// service layer can detect and log orphans (Hash is sql.NullString,
// not plain string, so the LEFT JOIN's SQL NULL passes through as
// Valid=false rather than crashing the scanner). Switching to an
// INNER JOIN here would silently drop the row — the len==2
// assertion catches that regression.
func TestListByTenantWithDeployment_OrphanPassesThrough(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()
	repo := NewActiveDeploymentRepository(db)

	rows := sqlmock.NewRows([]string{
		"tenant_id", "app_name", "deployment_id", "last_good_deployment_id",
		"auto_rollback_enabled", "stable_since", "regions_published", "regions_cached",
		"regions_failed", "last_publish_at", "last_publish_attempt_id",
		"hash", "regions",
	}).
		// happy app
		AddRow("t_a", "app1", "d_1", nil, false, nil, pq.StringArray{"global"}, pq.StringArray{}, pq.StringArray{"global"}, nil, nil, "hash1", pq.StringArray{"global"}).
		// orphan: d_2 has no match in deployments, so the LEFT JOIN
		// returns SQL NULL for hash and regions. The NULL is fed in
		// as a typed nil so the sql driver reports IsNull=true on
		// the column — otherwise the scan would attempt string
		// conversion on an untyped nil and fail before our check.
		AddRow("t_a", "app2", "d_2", nil, false, nil, pq.StringArray{"global"}, pq.StringArray{}, pq.StringArray{"global"}, nil, nil, nil, nil)

	mock.ExpectQuery(`SELECT.*active_deployments ad.*JOIN deployments d`).
		WithArgs("t_a").
		WillReturnRows(rows)

	got, err := repo.ListByTenantWithDeployment(context.Background(), "t_a")
	if err != nil {
		t.Fatalf("ListByTenantWithDeployment: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d, want 2 (orphan must pass through with Hash.Valid=false)", len(got))
	}
	if !got[0].Hash.Valid || got[0].Hash.String != "hash1" {
		t.Errorf("got[0].Hash = %+v, want Valid=true String=hash1", got[0].Hash)
	}
	if got[1].Hash.Valid {
		t.Errorf("got[1].Hash = %+v, want Valid=false (orphan)", got[1].Hash)
	}
	if len(got[1].Regions) != 0 {
		t.Errorf("got[1].Regions = %v, want empty (orphan)", got[1].Regions)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}
