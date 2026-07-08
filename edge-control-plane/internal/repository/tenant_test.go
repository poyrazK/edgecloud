package repository

import (
	"context"
	"database/sql"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

func newTenantMockRepo(t *testing.T) (*TenantRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return NewTenantRepository(sqlxDB), mock, func() { _ = mockDB.Close() }
}

func TestTenantRepository_Create(t *testing.T) {
	repo, mock, cleanup := newTenantMockRepo(t)
	defer cleanup()

	tnt := &domain.Tenant{
		ID:                      "t_1",
		Name:                    "acme",
		Plan:                    "free",
		AllowlistedDestinations: pq.StringArray{"*.example.com"},
	}

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO tenants`)).
		WithArgs(tnt.ID, tnt.Name, tnt.Plan, pq.Array(tnt.AllowlistedDestinations)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Create(context.Background(), tnt); err != nil {
		t.Fatalf("Create: %v", err)
	}
}

func TestTenantRepository_Create_EmptyAllowlist(t *testing.T) {
	repo, mock, cleanup := newTenantMockRepo(t)
	defer cleanup()

	tnt := &domain.Tenant{
		ID:                      "t_2",
		Name:                    "empty",
		Plan:                    "free",
		AllowlistedDestinations: pq.StringArray{},
	}

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO tenants`)).
		WithArgs(tnt.ID, tnt.Name, tnt.Plan, pq.Array(tnt.AllowlistedDestinations)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Create(context.Background(), tnt); err != nil {
		t.Fatalf("Create: %v", err)
	}
}

func TestTenantRepository_GetByID(t *testing.T) {
	repo, mock, cleanup := newTenantMockRepo(t)
	defer cleanup()

	now := time.Now()
	rows := sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
		AddRow("t_1", "acme", "free", pq.StringArray{"*.example.com", "api.internal"}, now)

	mock.ExpectQuery(`SELECT id.*FROM tenants WHERE`).
		WithArgs("t_1").
		WillReturnRows(rows)

	got, err := repo.GetByID(context.Background(), "t_1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Name != "acme" {
		t.Errorf("Name = %q, want acme", got.Name)
	}
	if len(got.AllowlistedDestinations) != 2 {
		t.Errorf("AllowlistedDestinations len = %d, want 2", len(got.AllowlistedDestinations))
	}
}

func TestTenantRepository_GetByID_NotFound(t *testing.T) {
	repo, mock, cleanup := newTenantMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT id.*FROM tenants WHERE`).
		WithArgs("t_missing").
		WillReturnError(sql.ErrNoRows)

	got, err := repo.GetByID(context.Background(), "t_missing")
	if err != nil {
		t.Fatalf("expected nil error for sql.ErrNoRows, got %v", err)
	}
	if got != nil {
		t.Errorf("got %+v, want nil", got)
	}
}

func TestTenantRepository_List(t *testing.T) {
	repo, mock, cleanup := newTenantMockRepo(t)
	defer cleanup()

	now := time.Now()
	rows := sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
		AddRow("t_1", "acme", "pro", pq.StringArray{}, now).
		AddRow("t_2", "globex", "free", pq.StringArray{"*"}, now)

	mock.ExpectQuery(`SELECT id.*FROM tenants ORDER BY created_at DESC`).
		WillReturnRows(rows)

	tenants, err := repo.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(tenants) != 2 {
		t.Errorf("len = %d, want 2", len(tenants))
	}
}

func TestTenantRepository_Update(t *testing.T) {
	repo, mock, cleanup := newTenantMockRepo(t)
	defer cleanup()

	tnt := &domain.Tenant{
		ID:                      "t_1",
		Name:                    "acme-v2",
		Plan:                    "pro",
		AllowlistedDestinations: pq.StringArray{"*.acme.com"},
	}

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE tenants SET`)).
		WithArgs(tnt.ID, tnt.Name, tnt.Plan, pq.Array(tnt.AllowlistedDestinations)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.Update(context.Background(), tnt); err != nil {
		t.Fatalf("Update: %v", err)
	}
}

func TestTenantRepository_Delete(t *testing.T) {
	repo, mock, cleanup := newTenantMockRepo(t)
	defer cleanup()

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM tenants WHERE`)).
		WithArgs("t_1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.Delete(context.Background(), "t_1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}
