package repository

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

// newWorkerMockRepo wires a sqlmock-backed sqlx.DB into a
// WorkerRepository. Mirrors the helpers in log_entry_test.go.
func newWorkerMockRepo(t *testing.T) (*WorkerRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return &WorkerRepository{db: sqlxDB}, mock, func() { _ = mockDB.Close() }
}

// TestWorkerRepository_GetAppStatus_Running pins the happy path: a
// worker has reported on (tenant, app), status = "running", and we
// surface the row with Region, WorkerID, LastHeartbeat populated.
// ExitCode is absent (the worker did not provide one); the column
// projection coerces the missing JSON value to NULL via NULLIF(..., ”)::int4.
func TestWorkerRepository_GetAppStatus_Running(t *testing.T) {
	repo, mock, cleanup := newWorkerMockRepo(t)
	defer cleanup()

	hb := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{
		"app_name", "status", "last_heartbeat", "region", "worker_id", "exit_code",
	}).AddRow("myapp", "running", hb, "us-east-1", "w_us-east-1_h01", nil)

	mock.ExpectQuery(`SELECT\s+apps\.key.*FROM workers`).
		WithArgs("myapp", "t_test").
		WillReturnRows(rows)

	got, err := repo.GetAppStatus(context.Background(), "t_test", "myapp")
	if err != nil {
		t.Fatalf("GetAppStatus: %v", err)
	}
	if got == nil {
		t.Fatal("got nil, want *AppWorkerStatus")
	}
	if got.AppName != "myapp" {
		t.Errorf("AppName = %q, want myapp", got.AppName)
	}
	if got.Status != "running" {
		t.Errorf("Status = %q, want running", got.Status)
	}
	if got.Region != "us-east-1" {
		t.Errorf("Region = %q, want us-east-1", got.Region)
	}
	if got.WorkerID != "w_us-east-1_h01" {
		t.Errorf("WorkerID = %q, want w_us-east-1_h01", got.WorkerID)
	}
	if got.LastHeartbeat == nil || !got.LastHeartbeat.Equal(hb) {
		t.Errorf("LastHeartbeat = %v, want %v", got.LastHeartbeat, hb)
	}
	if got.ExitCode != nil {
		t.Errorf("ExitCode = %v, want nil", *got.ExitCode)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWorkerRepository_GetAppStatus_CrashedWithExitCode pins the
// path that the issue #77 §5 hint depends on: a worker has reported
// status = "crashed" with an exit code, and we surface both. The
// CLI uses Status == "crashed" to decide whether to print the
// `edge rollback` hint.
func TestWorkerRepository_GetAppStatus_CrashedWithExitCode(t *testing.T) {
	repo, mock, cleanup := newWorkerMockRepo(t)
	defer cleanup()

	hb := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	var exit int32 = 137
	rows := sqlmock.NewRows([]string{
		"app_name", "status", "last_heartbeat", "region", "worker_id", "exit_code",
	}).AddRow("myapp", "crashed", hb, "us-east-1", "w_us-east-1_h01", exit)

	mock.ExpectQuery(`SELECT\s+apps\.key.*FROM workers`).
		WithArgs("myapp", "t_test").
		WillReturnRows(rows)

	got, err := repo.GetAppStatus(context.Background(), "t_test", "myapp")
	if err != nil {
		t.Fatalf("GetAppStatus: %v", err)
	}
	if got.Status != "crashed" {
		t.Errorf("Status = %q, want crashed", got.Status)
	}
	if got.ExitCode == nil || *got.ExitCode != 137 {
		t.Errorf("ExitCode = %v, want 137", got.ExitCode)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWorkerRepository_GetAppStatus_Hung pins the other "bad" status
// the worker can publish. The CLI hint only fires on `crashed`, but
// the endpoint must surface the raw status string verbatim so a
// future CLI filter (e.g. "is my app hung?") can match on it.
func TestWorkerRepository_GetAppStatus_Hung(t *testing.T) {
	repo, mock, cleanup := newWorkerMockRepo(t)
	defer cleanup()

	hb := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{
		"app_name", "status", "last_heartbeat", "region", "worker_id", "exit_code",
	}).AddRow("myapp", "hung", hb, "us-east-1", "w_us-east-1_h01", nil)

	mock.ExpectQuery(`SELECT\s+apps\.key.*FROM workers`).
		WithArgs("myapp", "t_test").
		WillReturnRows(rows)

	got, err := repo.GetAppStatus(context.Background(), "t_test", "myapp")
	if err != nil {
		t.Fatalf("GetAppStatus: %v", err)
	}
	if got == nil || got.Status != "hung" {
		t.Errorf("Status = %v, want hung", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWorkerRepository_GetAppStatus_NotFound pins the no-rows path:
// no worker has reported on this (tenant, app) pair. The repo
// returns (nil, nil) so the service can translate to
// AppWorkerStatus{Status: "unknown"} without surfacing a DB error
// to the tenant.
//
// This is also the path that fires for cross-tenant requests: a
// t_evil query for an app_name that only t_victim has deployed
// returns the same (nil, nil) — no information leak (the t_evil
// tenant cannot distinguish "no such app" from "exists but is not
// yours", which is the desired property).
func TestWorkerRepository_GetAppStatus_NotFound(t *testing.T) {
	repo, mock, cleanup := newWorkerMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT\s+apps\.key.*FROM workers`).
		WithArgs("myapp", "t_test").
		WillReturnRows(sqlmock.NewRows([]string{
			"app_name", "status", "last_heartbeat", "region", "worker_id", "exit_code",
		}))

	got, err := repo.GetAppStatus(context.Background(), "t_test", "myapp")
	if err != nil {
		t.Fatalf("GetAppStatus: %v", err)
	}
	if got != nil {
		t.Errorf("got %+v, want nil for no-rows path", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWorkerRepository_TenantsHostedBy_Single pins the happy path: one
// app row, one tenant, returns a one-element slice. Verifies the SQL
// filter (`status = 'running'`) and the result decoder wiring.
func TestWorkerRepository_TenantsHostedBy_Single(t *testing.T) {
	repo, mock, cleanup := newWorkerMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"tenant_id"}).
		AddRow("t_a")
	mock.ExpectQuery(`SELECT DISTINCT apps\.value->>'tenant_id'`).
		WithArgs("w_us_fra_1").
		WillReturnRows(rows)

	got, err := repo.TenantsHostedBy(context.Background(), "w_us_fra_1")
	if err != nil {
		t.Fatalf("TenantsHostedBy: %v", err)
	}
	if len(got) != 1 || got[0] != "t_a" {
		t.Errorf("got %v, want [t_a]", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWorkerRepository_TenantsHostedBy_Multiple pins DISTINCT
// collapsing: the SQL returns three raw rows (with a duplicate), the
// repo returns a two-element slice. The duplicate tenant_id must
// appear only once.
func TestWorkerRepository_TenantsHostedBy_Multiple(t *testing.T) {
	repo, mock, cleanup := newWorkerMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"tenant_id"}).
		AddRow("t_a").
		AddRow("t_b").
		AddRow("t_a")
	mock.ExpectQuery(`SELECT DISTINCT apps\.value->>'tenant_id'`).
		WithArgs("w_us_fra_1").
		WillReturnRows(rows)

	got, err := repo.TenantsHostedBy(context.Background(), "w_us_fra_1")
	if err != nil {
		t.Fatalf("TenantsHostedBy: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d tenants, want 2 (DISTINCT must collapse)", len(got))
	}
	set := map[string]bool{}
	for _, x := range got {
		set[x] = true
	}
	if !set["t_a"] || !set["t_b"] {
		t.Errorf("got %v, want set{t_a, t_b}", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWorkerRepository_TenantsHostedBy_Empty pins the no-rows path:
// the worker has either no worker_status row at all, or an empty apps
// JSONB. The repo must return ([]string{}, nil), not nil — the
// handler iterates the slice, ranging over nil is a no-op but a
// caller doing `len(hosted)` would get 0 either way; the slice
// non-nil invariant is a defensive shape only.
func TestWorkerRepository_TenantsHostedBy_Empty(t *testing.T) {
	repo, mock, cleanup := newWorkerMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT DISTINCT apps\.value->>'tenant_id'`).
		WithArgs("w_us_fra_1").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}))

	got, err := repo.TenantsHostedBy(context.Background(), "w_us_fra_1")
	if err != nil {
		t.Fatalf("TenantsHostedBy: %v", err)
	}
	if got == nil {
		t.Errorf("got nil, want empty slice (handler relies on non-nil for range)")
	}
	if len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWorkerRepository_TenantsHostedBy_NoWorkerStatusRow pins the
// "JOIN returns no rows" path: a worker has been registered (so the
// `workers` row exists) but has never heartbeated (no `worker_status`
// row). The repo must return ([]string{}, nil) — NOT propagate a DB
// error. The handler maps this empty slice to 403 for any tenant.
func TestWorkerRepository_TenantsHostedBy_NoWorkerStatusRow(t *testing.T) {
	repo, mock, cleanup := newWorkerMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT DISTINCT apps\.value->>'tenant_id'`).
		WithArgs("w_us_fra_1").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}))

	got, err := repo.TenantsHostedBy(context.Background(), "w_us_fra_1")
	if err != nil {
		t.Fatalf("TenantsHostedBy: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Errorf("got %v, want empty (no worker_status row)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWorkerRepository_GetAppStatus_PicksLatestHeartbeat pins the
// ORDER BY / LIMIT 1 clause: when multiple workers in different
// regions have reported on the same app, the repo returns the
// most recent heartbeat. The CLI hint treats the returned
// LastHeartbeat as authoritative for the staleness check, so
// picking the wrong (older) row would suppress a real crash hint.
func TestWorkerRepository_GetAppStatus_PicksLatestHeartbeat(t *testing.T) {
	repo, mock, cleanup := newWorkerMockRepo(t)
	defer cleanup()

	hb := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	// The query orders DESC and limits 1, so sqlmock only needs
	// to return the single row the caller should see. The
	// ORDER BY / LIMIT 1 are exercised by the regex matcher
	// (they appear in the query text); the row count is the
	// repo's contract.
	rows := sqlmock.NewRows([]string{
		"app_name", "status", "last_heartbeat", "region", "worker_id", "exit_code",
	}).AddRow("myapp", "crashed", hb, "us-west-2", "w_us-west-2_h02", nil)

	mock.ExpectQuery(`SELECT\s+apps\.key.*ORDER BY worker_status\.last_report DESC\s+LIMIT 1`).
		WithArgs("myapp", "t_test").
		WillReturnRows(rows)

	got, err := repo.GetAppStatus(context.Background(), "t_test", "myapp")
	if err != nil {
		t.Fatalf("GetAppStatus: %v", err)
	}
	if got.Region != "us-west-2" {
		t.Errorf("Region = %q, want us-west-2 (latest heartbeat)", got.Region)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}
