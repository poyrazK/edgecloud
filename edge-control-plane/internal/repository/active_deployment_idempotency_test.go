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
)

// TestActiveDeploymentIdempotencyKeyRepo_Lookup_Found asserts the
// happy-path: a fresh row (created_at within IdempotencyTTL) is
// returned with all five columns populated.
func TestActiveDeploymentIdempotencyKeyRepo_Lookup_Found(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()
	repo := NewActiveDeploymentIdempotencyKeyRepo(db)

	const (
		tenantID = "t_lookup_found"
		key      = "01234567-89ab-cdef-0123-456789abcdef"
		appName  = "myapp"
		depID    = "d_xyz"
	)
	now := time.Now()
	rows := sqlmock.NewRows([]string{"tenant_id", "idempotency_key", "app_name", "deployment_id", "created_at"}).
		AddRow(tenantID, key, appName, depID, now)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, idempotency_key, app_name, deployment_id, created_at`)).
		WithArgs(tenantID, key, int64(IdempotencyTTL.Seconds())).
		WillReturnRows(rows)

	got, err := repo.Lookup(context.Background(), tenantID, key)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if got == nil {
		t.Fatal("got = nil, want populated *ActiveDeploymentIdempotencyKey")
	}
	if got.TenantID != tenantID || got.IdempotencyKey != key || got.AppName != appName || got.DeploymentID != depID {
		t.Errorf("Lookup row mismatch: got %+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestActiveDeploymentIdempotencyKeyRepo_Lookup_Missing asserts the
// repo returns (nil, nil) when no row exists for (tenant, key).
// This is the contract callers rely on to distinguish "first call"
// from a hard error.
func TestActiveDeploymentIdempotencyKeyRepo_Lookup_Missing(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()
	repo := NewActiveDeploymentIdempotencyKeyRepo(db)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, idempotency_key, app_name, deployment_id, created_at`)).
		WithArgs("t_missing", "k_missing", int64(IdempotencyTTL.Seconds())).
		WillReturnError(sql.ErrNoRows)

	got, err := repo.Lookup(context.Background(), "t_missing", "k_missing")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if got != nil {
		t.Errorf("got = %+v, want nil", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestActiveDeploymentIdempotencyKeyRepo_Lookup_Expired documents
// that the TTL filter is enforced in the SQL itself
// (`created_at > NOW() - make_interval(...)`), so a row that's
// already past the cutoff simply won't match — the SELECT returns
// sql.ErrNoRows, the repo normalises that to (nil, nil), and the
// caller computes fresh-publish semantics. We exercise the same
// SELECT shape so a future schema change that drops the TTL
// filter would surface here.
func TestActiveDeploymentIdempotencyKeyRepo_Lookup_Expired(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()
	repo := NewActiveDeploymentIdempotencyKeyRepo(db)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, idempotency_key, app_name, deployment_id, created_at`)).
		WithArgs("t_expired", "k_expired", int64(IdempotencyTTL.Seconds())).
		WillReturnError(sql.ErrNoRows)

	got, err := repo.Lookup(context.Background(), "t_expired", "k_expired")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if got != nil {
		t.Errorf("got = %+v, want nil (expired row treated as cache miss)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestActiveDeploymentIdempotencyKeyRepo_Insert_First asserts a
// fresh INSERT populates the row with NOW() server-side (no
// created_at passed from the caller).
func TestActiveDeploymentIdempotencyKeyRepo_Insert_First(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()
	repo := NewActiveDeploymentIdempotencyKeyRepo(db)

	const (
		tenantID = "t_ins_first"
		key      = "k_first"
		appName  = "myapp"
		depID    = "d_first"
	)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployment_idempotency_keys`)).
		WithArgs(tenantID, key, appName, depID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Insert(context.Background(), &domain.ActiveDeploymentIdempotencyKey{
		TenantID:       tenantID,
		IdempotencyKey: key,
		AppName:        appName,
		DeploymentID:   depID,
	}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestActiveDeploymentIdempotencyKeyRepo_Insert_DuplicateDoesNothing
// asserts that ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
// turns a second INSERT against the same (tenant, key) into a
// successful no-op. We can't observe the conflict from inside
// sqlmock without a real unique index, so we model it as an empty
// result-set from the upsert: sqlmock's Exec returns
// RowsAffected=0 for a DO NOTHING on conflict, and we expect
// nil error. The contract the service layer relies on is
// "concurrent retry doesn't error" — that's what this guards.
func TestActiveDeploymentIdempotencyKeyRepo_Insert_DuplicateDoesNothing(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()
	repo := NewActiveDeploymentIdempotencyKeyRepo(db)

	const (
		tenantID = "t_ins_dup"
		key      = "k_dup"
		appName  = "myapp"
		depID    = "d_dup"
	)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployment_idempotency_keys`)).
		WithArgs(tenantID, key, appName, depID).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := repo.Insert(context.Background(), &domain.ActiveDeploymentIdempotencyKey{
		TenantID:       tenantID,
		IdempotencyKey: key,
		AppName:        appName,
		DeploymentID:   depID,
	}); err != nil {
		t.Fatalf("Insert (duplicate): %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestActiveDeploymentIdempotencyKeyRepo_Lookup_PropagatesDBError
// asserts the repo doesn't swallow errors that aren't sql.ErrNoRows.
// A real DB outage must surface to the service layer so the
// handler can return 500 instead of silently falling through to
// fresh-publish semantics.
func TestActiveDeploymentIdempotencyKeyRepo_Lookup_PropagatesDBError(t *testing.T) {
	db, mock, cleanup := newActiveDeploymentMockDB(t)
	defer cleanup()
	repo := NewActiveDeploymentIdempotencyKeyRepo(db)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, idempotency_key, app_name, deployment_id, created_at`)).
		WithArgs("t_db_err", "k_db_err", int64(IdempotencyTTL.Seconds())).
		WillReturnError(errors.New("connection refused"))

	got, err := repo.Lookup(context.Background(), "t_db_err", "k_db_err")
	if err == nil {
		t.Fatal("err = nil, want propagated db error")
	}
	if got != nil {
		t.Errorf("got = %+v, want nil on db error", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}
