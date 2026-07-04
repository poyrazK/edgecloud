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

func newAppMockRepo(t *testing.T) (*AppRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return NewAppRepository(sqlxDB), mock, func() { _ = mockDB.Close() }
}

func TestAppRepository_Create(t *testing.T) {
	repo, mock, cleanup := newAppMockRepo(t)
	defer cleanup()

	now := time.Now()
	desc := "my-app"
	app := &domain.App{
		ID:          "a_1",
		TenantID:    "t_1",
		Name:        "hello",
		Description: &desc,
		CreatedAt:   now,
	}
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO apps`)).
		WithArgs(app.ID, app.TenantID, app.Name, app.Description, app.CreatedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Create(context.Background(), app); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestAppRepository_Get(t *testing.T) {
	repo, mock, cleanup := newAppMockRepo(t)
	defer cleanup()

	now := time.Now()
	desc := "my-app"
	rows := sqlmock.NewRows([]string{"id", "tenant_id", "name", "description", "created_at"}).
		AddRow("a_1", "t_1", "hello", &desc, now)

	mock.ExpectQuery(`SELECT id.*FROM apps WHERE`).
		WithArgs("t_1", "hello").
		WillReturnRows(rows)

	got, err := repo.Get(context.Background(), "t_1", "hello")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("got nil, want *domain.App")
	}
	if got.ID != "a_1" {
		t.Errorf("ID = %q, want a_1", got.ID)
	}
	if got.Description == nil || *got.Description != "my-app" {
		t.Errorf("Description = %v", got.Description)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestAppRepository_Get_NotFound(t *testing.T) {
	repo, mock, cleanup := newAppMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT id.*FROM apps WHERE`).
		WithArgs("t_1", "missing").
		WillReturnError(sql.ErrNoRows)

	got, err := repo.Get(context.Background(), "t_1", "missing")
	if err != nil {
		t.Fatalf("expected nil error for sql.ErrNoRows, got %v", err)
	}
	if got != nil {
		t.Errorf("got %+v, want nil", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestAppRepository_GetForUpdate(t *testing.T) {
	repo, mock, cleanup := newAppMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"id", "tenant_id", "name", "description", "created_at"}).
		AddRow("a_1", "t_1", "hello", nil, time.Now())

	mock.ExpectQuery(`SELECT id.*FROM apps WHERE.*FOR UPDATE`).
		WithArgs("t_1", "hello").
		WillReturnRows(rows)

	got, err := repo.GetForUpdate(context.Background(), "t_1", "hello")
	if err != nil {
		t.Fatalf("GetForUpdate: %v", err)
	}
	if got.ID != "a_1" {
		t.Errorf("ID = %q, want a_1", got.ID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestAppRepository_List(t *testing.T) {
	repo, mock, cleanup := newAppMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"id", "tenant_id", "name", "description", "created_at"})
	mock.ExpectQuery(`SELECT id.*FROM apps WHERE.*ORDER BY name LIMIT.*OFFSET`).
		WithArgs("t_1", 50, 0).
		WillReturnRows(rows)

	apps, err := repo.List(context.Background(), "t_1", 50, 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(apps) != 0 {
		t.Errorf("expected empty slice, got %d elements", len(apps))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestAppRepository_Delete(t *testing.T) {
	repo, mock, cleanup := newAppMockRepo(t)
	defer cleanup()

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM apps WHERE`)).
		WithArgs("t_1", "hello").
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := repo.Delete(context.Background(), "t_1", "hello"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestAppRepository_Exists_True(t *testing.T) {
	repo, mock, cleanup := newAppMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"exists"}).AddRow(true)
	mock.ExpectQuery(`SELECT EXISTS.*FROM apps`).
		WithArgs("t_1", "hello").
		WillReturnRows(rows)

	got, err := repo.Exists(context.Background(), "t_1", "hello")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !got {
		t.Errorf("Exists = false, want true")
	}
}

func TestAppRepository_Exists_False(t *testing.T) {
	repo, mock, cleanup := newAppMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"exists"}).AddRow(false)
	mock.ExpectQuery(`SELECT EXISTS.*FROM apps`).
		WithArgs("t_1", "nope").
		WillReturnRows(rows)

	got, err := repo.Exists(context.Background(), "t_1", "nope")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if got {
		t.Errorf("Exists = true, want false")
	}
}

func TestAppRepository_AtomicDelete_Found(t *testing.T) {
	repo, mock, cleanup := newAppMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"bool"}).AddRow(true)
	mock.ExpectQuery(regexp.QuoteMeta(`DELETE FROM apps WHERE`)).
		WithArgs("t_1", "hello").
		WillReturnRows(rows)

	got, err := repo.AtomicDelete(context.Background(), "t_1", "hello")
	if err != nil {
		t.Fatalf("AtomicDelete: %v", err)
	}
	if !got {
		t.Errorf("AtomicDelete = false, want true")
	}
}

func TestAppRepository_AtomicDelete_NotFound(t *testing.T) {
	repo, mock, cleanup := newAppMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`DELETE FROM apps WHERE`)).
		WithArgs("t_1", "missing").
		WillReturnError(sql.ErrNoRows)

	got, err := repo.AtomicDelete(context.Background(), "t_1", "missing")
	if err != nil {
		t.Fatalf("AtomicDelete: %v", err)
	}
	if got {
		t.Errorf("AtomicDelete = true, want false for not-found")
	}
}

func TestAppRepository_DeleteIfNoDeployments_Found(t *testing.T) {
	repo, mock, cleanup := newAppMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"bool"}).AddRow(true)
	mock.ExpectQuery(regexp.QuoteMeta(`DELETE FROM apps`)).
		WithArgs("t_1", "hello").
		WillReturnRows(rows)

	got, err := repo.DeleteIfNoDeployments(context.Background(), "t_1", "hello")
	if err != nil {
		t.Fatalf("DeleteIfNoDeployments: %v", err)
	}
	if !got {
		t.Errorf("DeleteIfNoDeployments = false, want true (no deployments, row deleted)")
	}
}

func TestAppRepository_DeleteIfNoDeployments_NoRows(t *testing.T) {
	repo, mock, cleanup := newAppMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`DELETE FROM apps`)).
		WithArgs("t_1", "withdeployments").
		WillReturnError(sql.ErrNoRows)

	got, err := repo.DeleteIfNoDeployments(context.Background(), "t_1", "withdeployments")
	if err != nil {
		t.Fatalf("DeleteIfNoDeployments: %v", err)
	}
	if got {
		t.Errorf("DeleteIfNoDeployments = true, want false (has deployments, NOT EXISTS suppressed)")
	}
}

func TestAppRepository_CountByTenant(t *testing.T) {
	repo, mock, cleanup := newAppMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"count"}).AddRow(3)
	mock.ExpectQuery(`SELECT COUNT.*FROM apps`).
		WithArgs("t_1").
		WillReturnRows(rows)

	got, err := repo.CountByTenant(context.Background(), "t_1")
	if err != nil {
		t.Fatalf("CountByTenant: %v", err)
	}
	if got != 3 {
		t.Errorf("CountByTenant = %d, want 3", got)
	}
}

func TestAppRepository_InsertIfNotExists_Success(t *testing.T) {
	repo, mock, cleanup := newAppMockRepo(t)
	defer cleanup()

	now := time.Now()
	app := &domain.App{ID: "a_1", TenantID: "t_1", Name: "hello", CreatedAt: now}

	rows := sqlmock.NewRows([]string{"bool"}).AddRow(true)
	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO apps`)).
		WithArgs(app.ID, app.TenantID, app.Name, app.Description, app.CreatedAt).
		WillReturnRows(rows)

	got, err := repo.InsertIfNotExists(context.Background(), app)
	if err != nil {
		t.Fatalf("InsertIfNotExists: %v", err)
	}
	if !got {
		t.Errorf("InsertIfNotExists = false, want true (new row)")
	}
}

func TestAppRepository_InsertIfNotExists_Conflict(t *testing.T) {
	repo, mock, cleanup := newAppMockRepo(t)
	defer cleanup()

	app := &domain.App{ID: "a_1", TenantID: "t_1", Name: "hello", CreatedAt: time.Now()}

	rows := sqlmock.NewRows([]string{"bool"}).AddRow(false)
	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO apps`)).
		WithArgs(app.ID, app.TenantID, app.Name, app.Description, app.CreatedAt).
		WillReturnRows(rows)

	got, err := repo.InsertIfNotExists(context.Background(), app)
	if err != nil {
		t.Fatalf("InsertIfNotExists: %v", err)
	}
	if got {
		t.Errorf("InsertIfNotExists = true, want false (conflict)")
	}
}

func TestAppRepository_Update_Success(t *testing.T) {
	repo, mock, cleanup := newAppMockRepo(t)
	defer cleanup()

	desc := "updated description"
	app := &domain.App{ID: "a_1", TenantID: "t_1", Name: "hello", Description: &desc}

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE apps SET description = $1 WHERE id = $2 AND tenant_id = $3`)).
		WithArgs(app.Description, app.ID, app.TenantID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.Update(context.Background(), app); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestAppRepository_Update_ClearsDescription(t *testing.T) {
	repo, mock, cleanup := newAppMockRepo(t)
	defer cleanup()

	app := &domain.App{ID: "a_1", TenantID: "t_1", Name: "hello", Description: nil}

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE apps SET description = $1 WHERE id = $2 AND tenant_id = $3`)).
		WithArgs(nil, app.ID, app.TenantID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.Update(context.Background(), app); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestAppRepository_Get_DBError(t *testing.T) {
	repo, mock, cleanup := newAppMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT id.*FROM apps WHERE`).
		WithArgs("t_1", "hello").
		WillReturnError(errors.New("connection refused"))

	_, err := repo.Get(context.Background(), "t_1", "hello")
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "connection refused" {
		t.Errorf("error = %v, want 'connection refused'", err)
	}
}
