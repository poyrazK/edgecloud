package repository

import (
	"context"
	"database/sql"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

func newAppEnvMockRepo(t *testing.T) (*AppEnvRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return NewAppEnvRepository(sqlxDB), mock, func() { _ = mockDB.Close() }
}

func TestAppEnvRepository_Set(t *testing.T) {
	repo, mock, cleanup := newAppEnvMockRepo(t)
	defer cleanup()

	env := &domain.AppEnv{
		TenantID: "t_1",
		AppName:  "hello",
		EnvKey:   "DATABASE_URL",
		EnvValue: "postgres://localhost",
	}

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO app_env`)).
		WithArgs(env.TenantID, env.AppName, env.EnvKey, env.EnvValue).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Set(context.Background(), env); err != nil {
		t.Fatalf("Set: %v", err)
	}
}

func TestAppEnvRepository_Get(t *testing.T) {
	repo, mock, cleanup := newAppEnvMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}).
		AddRow("t_1", "hello", "DATABASE_URL", "postgres://localhost")

	mock.ExpectQuery(`SELECT tenant_id.*FROM app_env WHERE`).
		WithArgs("t_1", "hello", "DATABASE_URL").
		WillReturnRows(rows)

	got, err := repo.Get(context.Background(), "t_1", "hello", "DATABASE_URL")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.EnvKey != "DATABASE_URL" {
		t.Errorf("EnvKey = %q, want DATABASE_URL", got.EnvKey)
	}
	if got.EnvValue != "postgres://localhost" {
		t.Errorf("EnvValue = %q", got.EnvValue)
	}
}

func TestAppEnvRepository_Get_NotFound(t *testing.T) {
	repo, mock, cleanup := newAppEnvMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT tenant_id.*FROM app_env WHERE`).
		WithArgs("t_1", "hello", "MISSING_KEY").
		WillReturnError(sql.ErrNoRows)

	got, err := repo.Get(context.Background(), "t_1", "hello", "MISSING_KEY")
	if err != nil {
		t.Fatalf("expected nil error for sql.ErrNoRows, got %v", err)
	}
	if got != nil {
		t.Errorf("got %+v, want nil", got)
	}
}

func TestAppEnvRepository_List(t *testing.T) {
	repo, mock, cleanup := newAppEnvMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}).
		AddRow("t_1", "hello", "DATABASE_URL", "postgres://localhost").
		AddRow("t_1", "hello", "LOG_LEVEL", "info")

	mock.ExpectQuery(`SELECT tenant_id.*FROM app_env WHERE.*ORDER BY env_key`).
		WithArgs("t_1", "hello").
		WillReturnRows(rows)

	envs, err := repo.List(context.Background(), "t_1", "hello")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(envs) != 2 {
		t.Errorf("len = %d, want 2", len(envs))
	}
}

func TestAppEnvRepository_Delete(t *testing.T) {
	repo, mock, cleanup := newAppEnvMockRepo(t)
	defer cleanup()

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM app_env WHERE`)).
		WithArgs("t_1", "hello", "DATABASE_URL").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.Delete(context.Background(), "t_1", "hello", "DATABASE_URL"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

func TestAppEnvRepository_DeleteByApp(t *testing.T) {
	repo, mock, cleanup := newAppEnvMockRepo(t)
	defer cleanup()

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM app_env WHERE`)).
		WithArgs("t_1", "hello").
		WillReturnResult(sqlmock.NewResult(0, 3))

	if err := repo.DeleteByApp(context.Background(), "t_1", "hello"); err != nil {
		t.Fatalf("DeleteByApp: %v", err)
	}
}

// TestListByApps_HappyPath pins the bulk-env query introduced in
// PR #166 follow-up: one round trip with `app_name = ANY($2)` returns
// env vars for every requested app. The previous implementation called
// List once per app (N+1); this test pins the single-call shape so a
// regression that reintroduces the per-app loop would fail the regex
// assertion below (the `app_name = ANY` predicate is single-query
// specific).
func TestListByApps_HappyPath(t *testing.T) {
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = mockDB.Close() }()
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	repo := NewAppEnvRepository(sqlxDB)

	rows := sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}).
		AddRow("t_a", "app1", "K1", "v1").
		AddRow("t_a", "app2", "K2", "v2")

	mock.ExpectQuery(`SELECT.*app_env.*app_name = ANY`).
		WithArgs("t_a", sqlmock.AnyArg()).
		WillReturnRows(rows)

	got, err := repo.ListByApps(context.Background(), "t_a", []string{"app1", "app2"})
	if err != nil {
		t.Fatalf("ListByApps: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("len=%d, want 2", len(got))
	}
	if got[0].EnvKey != "K1" || got[1].EnvKey != "K2" {
		t.Errorf("got=%+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}
