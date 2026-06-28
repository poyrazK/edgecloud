package repository

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

func newTrafficSplitMockRepo(t *testing.T) (*TrafficSplitRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return NewTrafficSplitRepository(sqlxDB), mock, func() { _ = mockDB.Close() }
}

func TestTrafficSplitRepository_Get(t *testing.T) {
	repo, mock, cleanup := newTrafficSplitMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"tenant_id", "app_name", "deployment_id", "weight", "created_at"})

	mock.ExpectQuery(`SELECT tenant_id.*FROM app_traffic_splits WHERE.*ORDER BY created_at ASC`).
		WithArgs("t_1", "hello").
		WillReturnRows(rows)

	splits, err := repo.Get(context.Background(), "t_1", "hello")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(splits) != 0 {
		t.Errorf("len = %d, want 0", len(splits))
	}
}

func TestTrafficSplitRepository_DeleteAllForApp(t *testing.T) {
	repo, mock, cleanup := newTrafficSplitMockRepo(t)
	defer cleanup()

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM app_traffic_splits WHERE`)).
		WithArgs("t_1", "hello").
		WillReturnResult(sqlmock.NewResult(0, 2))

	if err := repo.DeleteAllForApp(context.Background(), "t_1", "hello"); err != nil {
		t.Fatalf("DeleteAllForApp: %v", err)
	}
}

func TestSetTrafficSplits_EmptySlice(t *testing.T) {
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	db := sqlx.NewDb(mockDB, "postgres")
	defer func() { _ = mockDB.Close() }()

	// Empty splits → returns nil immediately, no DB interactions
	if err := SetTrafficSplits(context.Background(), db, nil); err != nil {
		t.Fatalf("SetTrafficSplits: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestSetTrafficSplits_SingleSplit(t *testing.T) {
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	db := sqlx.NewDb(mockDB, "postgres")
	defer func() { _ = mockDB.Close() }()

	splits := []*domain.TrafficSplit{
		{TenantID: "t_1", AppName: "hello", DeploymentID: "d_1", Weight: 100},
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM app_traffic_splits WHERE`)).
		WithArgs("t_1", "hello").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO app_traffic_splits`)).
		WithArgs("t_1", "hello", "d_1", 100).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := SetTrafficSplits(context.Background(), db, splits); err != nil {
		t.Fatalf("SetTrafficSplits: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestSetTrafficSplits_MultipleSplits(t *testing.T) {
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	db := sqlx.NewDb(mockDB, "postgres")
	defer func() { _ = mockDB.Close() }()

	splits := []*domain.TrafficSplit{
		{TenantID: "t_1", AppName: "hello", DeploymentID: "d_1", Weight: 70},
		{TenantID: "t_1", AppName: "hello", DeploymentID: "d_2", Weight: 30},
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM app_traffic_splits WHERE`)).
		WithArgs("t_1", "hello").
		WillReturnResult(sqlmock.NewResult(0, 2))
	// Two INSERTs in the loop
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO app_traffic_splits`)).
		WithArgs("t_1", "hello", "d_1", 70).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO app_traffic_splits`)).
		WithArgs("t_1", "hello", "d_2", 30).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := SetTrafficSplits(context.Background(), db, splits); err != nil {
		t.Fatalf("SetTrafficSplits: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestSetTrafficSplits_InsertFails_Rollback(t *testing.T) {
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	db := sqlx.NewDb(mockDB, "postgres")
	defer func() { _ = mockDB.Close() }()

	splits := []*domain.TrafficSplit{
		{TenantID: "t_1", AppName: "hello", DeploymentID: "d_1", Weight: 100},
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM app_traffic_splits WHERE`)).
		WithArgs("t_1", "hello").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO app_traffic_splits`)).
		WithArgs("t_1", "hello", "d_1", 100).
		WillReturnError(context.DeadlineExceeded)
	mock.ExpectRollback()

	err = SetTrafficSplits(context.Background(), db, splits)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}
