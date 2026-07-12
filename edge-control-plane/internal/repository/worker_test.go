package repository

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
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

// TestWorkerRepository_SetPublicKey pins the issue #430 enrollment
// writer: SetPublicKey issues a single UPDATE keyed by worker_id, sets
// the column to the hex-encoded pubkey, and reports the affected row
// count. The handler uses 0 to detect "worker not registered" — a
// guard against the bootstrap/enroll race.
func TestWorkerRepository_SetPublicKey(t *testing.T) {
	repo, mock, cleanup := newWorkerMockRepo(t)
	defer cleanup()

	mock.ExpectExec(`UPDATE workers SET public_key = \$2 WHERE id = \$1`).
		WithArgs("w_fra_1", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899").
		WillReturnResult(sqlmock.NewResult(0, 1))

	rows, err := repo.SetPublicKey(context.Background(),
		"w_fra_1",
		"aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
	if err != nil {
		t.Fatalf("SetPublicKey: %v", err)
	}
	if rows != 1 {
		t.Errorf("rows = %d, want 1", rows)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWorkerRepository_SetPublicKey_WorkerNotRegistered pins the
// not-affected path: a worker enrolled without ever calling
// /api/internal/workers (register). The handler surfaces 0 to the
// EnrollWorker logic, which then refuses to return a derived secret.
func TestWorkerRepository_SetPublicKey_WorkerNotRegistered(t *testing.T) {
	repo, mock, cleanup := newWorkerMockRepo(t)
	defer cleanup()

	mock.ExpectExec(`UPDATE workers SET public_key = \$2 WHERE id = \$1`).
		WithArgs("w_fra_ghost", regexp.MustCompile(`.+`).
			FindString("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	rows, err := repo.SetPublicKey(context.Background(),
		"w_fra_ghost",
		"aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
	if err != nil {
		t.Fatalf("SetPublicKey: %v", err)
	}
	if rows != 0 {
		t.Errorf("rows = %d, want 0 (worker not registered)", rows)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWorkerRepository_GetPublicKey_Hit pins the happy path: the
// worker has enrolled and the hex pubkey is returned verbatim. WorkerAuth
// uses this to recompute the HKDF-derived verification secret.
func TestWorkerRepository_GetPublicKey_Hit(t *testing.T) {
	repo, mock, cleanup := newWorkerMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"public_key"}).
		AddRow("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
	mock.ExpectQuery(`SELECT public_key FROM workers WHERE id = \$1`).
		WithArgs("w_fra_1").
		WillReturnRows(rows)

	got, err := repo.GetPublicKey(context.Background(), "w_fra_1")
	if err != nil {
		t.Fatalf("GetPublicKey: %v", err)
	}
	want := "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	if got != want {
		t.Errorf("GetPublicKey = %q, want %q", got, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWorkerRepository_GetPublicKey_NullColumn pins the
// "row exists but public_key IS NULL" path: a worker has been
// registered but never enrolled. WorkerAuth treats the empty return
// as "no verifiable kid" and 401s the request — a pre-#430 worker
// cannot present a valid wkr_ kid because it never enrolled.
func TestWorkerRepository_GetPublicKey_NullColumn(t *testing.T) {
	repo, mock, cleanup := newWorkerMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"public_key"}).AddRow(nil)
	mock.ExpectQuery(`SELECT public_key FROM workers WHERE id = \$1`).
		WithArgs("w_fra_legacy").
		WillReturnRows(rows)

	got, err := repo.GetPublicKey(context.Background(), "w_fra_legacy")
	if err != nil {
		t.Fatalf("GetPublicKey: %v", err)
	}
	if got != "" {
		t.Errorf("GetPublicKey = %q, want empty (NULL column)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWorkerRepository_GetPublicKey_NoRow pins the no-rows path: the
// worker_id has never been registered. Returns ("", nil) per the
// GetByID convention so the middleware can treat absence identically
// to NULL — both reject the kid without surfacing a DB error.
func TestWorkerRepository_GetPublicKey_NoRow(t *testing.T) {
	repo, mock, cleanup := newWorkerMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT public_key FROM workers WHERE id = \$1`).
		WithArgs("w_fra_missing").
		WillReturnRows(sqlmock.NewRows([]string{"public_key"}))

	got, err := repo.GetPublicKey(context.Background(), "w_fra_missing")
	if err != nil {
		t.Fatalf("GetPublicKey: %v", err)
	}
	if got != "" {
		t.Errorf("GetPublicKey = %q, want empty (no row)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWorkerRepository_SumFreeSlotsByRegion_PartialSaturation pins
// the deploy-time 402 gate (issue #641): one region reports zero
// free slots, another reports headroom. The map must surface both
// numbers so DeploymentService.Deploy can short-circuit before
// any artifact work. The query regex matches the freshness-window
// clause + GREATEST(free_slots, 0) aggregate + ANY($1) regions
// filter so a future refactor that drops any of those clauses
// fails this test.
func TestWorkerRepository_SumFreeSlotsByRegion_PartialSaturation(t *testing.T) {
	repo, mock, cleanup := newWorkerMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"region", "free_slots"}).
		AddRow("fra", int64(0)).
		AddRow("nyc", int64(7))
	mock.ExpectQuery(`SELECT workers\.region.*SUM\(GREATEST\(worker_status\.free_slots.*workers\.region = ANY\(\$1\)`).
		WithArgs(pq.Array([]string{"fra", "nyc"})).
		WillReturnRows(rows)

	got, err := repo.SumFreeSlotsByRegion(context.Background(), []string{"fra", "nyc"})
	if err != nil {
		t.Fatalf("SumFreeSlotsByRegion: %v", err)
	}
	if got["fra"] != 0 {
		t.Errorf("fra = %d, want 0 (saturated)", got["fra"])
	}
	if got["nyc"] != 7 {
		t.Errorf("nyc = %d, want 7 (headroom)", got["nyc"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWorkerRepository_SumFreeSlotsByRegion_NoWorkers pins the
// "no rows" path: every target region has zero recently-reporting
// workers. The repo must return (map, nil), NOT propagate a DB
// error. The deploy-gate interprets an empty result as
// "every region saturated" → 402 region_at_capacity.
func TestWorkerRepository_SumFreeSlotsByRegion_NoWorkers(t *testing.T) {
	repo, mock, cleanup := newWorkerMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT workers\.region.*SUM\(GREATEST\(worker_status\.free_slots.*workers\.region = ANY\(\$1\)`).
		WithArgs(pq.Array([]string{"fra"})).
		WillReturnRows(sqlmock.NewRows([]string{"region", "free_slots"}))

	got, err := repo.SumFreeSlotsByRegion(context.Background(), []string{"fra"})
	if err != nil {
		t.Fatalf("SumFreeSlotsByRegion: %v", err)
	}
	if got == nil {
		t.Errorf("got nil map, want empty (deploy-gate relies on non-nil)")
	}
	if v, ok := got["fra"]; ok {
		t.Errorf("fra key present in empty result: got=%d", v)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWorkerRepository_SumFreeSlotsByRegion_EmptyInput pins the
// "no regions asked" short-circuit: the repo must return
// (empty-map, nil) WITHOUT issuing a SQL query. This is a defensive
// shape — the deploy path resolves `effectiveRegions` before the
// call, but a future caller that forgets to default might pass
// `nil`. The handler iterates the result; ranging over nil is a
// no-op but `map[k]` on a nil map returns the zero value (which
// the deploy-gate treats as saturated), so we defensively short-circuit
// before the SQL fires.
func TestWorkerRepository_SumFreeSlotsByRegion_EmptyInput(t *testing.T) {
	repo, _, cleanup := newWorkerMockRepo(t)
	defer cleanup()

	// No ExpectQuery — if the repo issues SQL the test fails on
	// unmet expectations via mock cleanup.
	got, err := repo.SumFreeSlotsByRegion(context.Background(), nil)
	if err != nil {
		t.Fatalf("SumFreeSlotsByRegion: %v", err)
	}
	if got == nil {
		t.Errorf("got nil map, want empty (defensive non-nil invariant)")
	}
	if len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
}
