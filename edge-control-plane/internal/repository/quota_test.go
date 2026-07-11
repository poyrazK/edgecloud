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
)

func newQuotaMockRepo(t *testing.T) (*QuotaRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return NewQuotaRepository(sqlxDB), mock, func() { _ = mockDB.Close() }
}

// newQuotaMockMemoryRepo returns a *MemoryQuotaRepository bound to a
// mock tx so AddMemoryMB tests can exercise the tx-scoped code path
// (issue #44, part 2). The mock doesn't really start a tx — every
// query the wrapper issues routes through sqlmock just like the
// outer repo's queries do. The returned *sqlx.Tx is just a handle
// that satisfies the constructor; the mock matches queries by regex.
func newQuotaMockMemoryRepo(t *testing.T) (*MemoryQuotaRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	// We need a *sqlx.Tx-shaped value but sqlmock doesn't actually
	// begin a real transaction for these tests — the BeginTx method
	// on sqlxDB returns a real tx that wraps the mock, so we start
	// one and let the test drive its expectations. The closer ends
	// the tx with Rollback so expectations are satisfied.
	mock.ExpectBegin()
	tx, err := sqlxDB.BeginTxx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTxx: %v", err)
	}
	// ExpectBegin is satisfied by the BeginTxx call above; the test
	// closes the tx via tx.Rollback() in the returned closer.
	return NewMemoryQuotaRepository(tx), mock, func() {
		_ = tx.Rollback()
		_ = mockDB.Close()
	}
}

func TestQuotaRepository_Create(t *testing.T) {
	repo, mock, cleanup := newQuotaMockRepo(t)
	defer cleanup()

	q, err := domain.QuotaForPlan("free")
	if err != nil {
		t.Fatalf("QuotaForPlan(free): %v", err)
	}
	q.TenantID = "t_1"

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO quotas`)).
		WithArgs(q.TenantID, q.MaxDeployments, q.MaxApps, q.MaxWorkers, q.MaxMemoryMB, q.MaxOutboundMB, q.MaxRequestsPerMonth).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Create(context.Background(), &q); err != nil {
		t.Fatalf("Create: %v", err)
	}
}

func TestQuotaRepository_GetByTenantID(t *testing.T) {
	repo, mock, cleanup := newQuotaMockRepo(t)
	defer cleanup()

	periodStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{
		"tenant_id", "max_deployments", "max_apps", "max_workers",
		"max_memory_mb", "max_outbound_mb", "max_requests_per_month",
		"max_resident_seconds_per_month", "used_outbound_bytes",
		"used_request_count", "used_memory_mb", "used_resident_seconds",
		"quota_period_start",
	}).AddRow("t_1", 10, 5, 3, 256, 1000, 100_000, 2_592_000, 0, 0, 0, 0, periodStart)

	mock.ExpectQuery(`SELECT tenant_id.*FROM quotas WHERE`).
		WithArgs("t_1").
		WillReturnRows(rows)

	got, err := repo.GetByTenantID(context.Background(), "t_1")
	if err != nil {
		t.Fatalf("GetByTenantID: %v", err)
	}
	if got.MaxApps != 5 {
		t.Errorf("MaxApps = %d, want 5", got.MaxApps)
	}
	if got.MaxRequestsPerMonth != 100_000 {
		t.Errorf("MaxRequestsPerMonth = %d, want 100000", got.MaxRequestsPerMonth)
	}
	if got.MaxResidentSecondsPerMonth != 2_592_000 {
		t.Errorf("MaxResidentSecondsPerMonth = %d, want 2592000", got.MaxResidentSecondsPerMonth)
	}
	if got.UsedMemoryMB != 0 {
		t.Errorf("UsedMemoryMB = %d, want 0", got.UsedMemoryMB)
	}
}

func TestQuotaRepository_GetByTenantID_NotFound(t *testing.T) {
	repo, mock, cleanup := newQuotaMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT tenant_id.*FROM quotas WHERE`).
		WithArgs("t_missing").
		WillReturnError(sql.ErrNoRows)

	got, err := repo.GetByTenantID(context.Background(), "t_missing")
	if err != nil {
		t.Fatalf("expected nil error for sql.ErrNoRows, got %v", err)
	}
	if got != nil {
		t.Errorf("got %+v, want nil", got)
	}
}

func TestQuotaRepository_Update(t *testing.T) {
	repo, mock, cleanup := newQuotaMockRepo(t)
	defer cleanup()

	q, err := domain.QuotaForPlan("free")
	if err != nil {
		t.Fatalf("QuotaForPlan(free): %v", err)
	}
	q.TenantID = "t_1"
	q.MaxDeployments = 50
	q.MaxApps = 20

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE quotas SET`)).
		WithArgs(q.TenantID, q.MaxDeployments, q.MaxApps, q.MaxWorkers, q.MaxMemoryMB, q.MaxOutboundMB, q.MaxRequestsPerMonth).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.Update(context.Background(), &q); err != nil {
		t.Fatalf("Update: %v", err)
	}
}

func TestQuotaRepository_AddOutboundBytes(t *testing.T) {
	repo, mock, cleanup := newQuotaMockRepo(t)
	defer cleanup()

	periodStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{
		"tenant_id", "max_deployments", "max_apps", "max_workers",
		"max_memory_mb", "max_outbound_mb", "max_requests_per_month",
		"max_resident_seconds_per_month", "used_outbound_bytes",
		"used_request_count", "used_memory_mb", "used_resident_seconds",
		"quota_period_start",
	}).AddRow("t_1", 10, 5, 3, 256, 1000, 100_000, 2_592_000, 42, 0, 0, 0, periodStart)

	mock.ExpectQuery(`UPDATE quotas SET`).
		WithArgs("t_1", int64(42)).
		WillReturnRows(rows)

	got, err := repo.AddOutboundBytes(context.Background(), "t_1", 42)
	if err != nil {
		t.Fatalf("AddOutboundBytes: %v", err)
	}
	if got.UsedOutboundBytes != 42 {
		t.Errorf("UsedOutboundBytes = %d, want 42", got.UsedOutboundBytes)
	}
}

func TestQuotaRepository_AddOutboundBytes_NotFound(t *testing.T) {
	repo, mock, cleanup := newQuotaMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`UPDATE quotas SET`).
		WithArgs("t_missing", int64(10)).
		WillReturnError(sql.ErrNoRows)

	got, err := repo.AddOutboundBytes(context.Background(), "t_missing", 10)
	if err != nil {
		t.Fatalf("expected nil error for sql.ErrNoRows, got %v", err)
	}
	if got != nil {
		t.Errorf("got %+v, want nil", got)
	}
}

func TestQuotaRepository_AddRequestCount(t *testing.T) {
	repo, mock, cleanup := newQuotaMockRepo(t)
	defer cleanup()

	periodStart := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{
		"tenant_id", "max_deployments", "max_apps", "max_workers",
		"max_memory_mb", "max_outbound_mb", "max_requests_per_month",
		"max_resident_seconds_per_month", "used_outbound_bytes",
		"used_request_count", "used_memory_mb", "used_resident_seconds",
		"quota_period_start",
	}).AddRow("t_1", 50, 20, 10, 512, 10_000, 5_000_000, 7_776_000, 0, 17, 0, 0, periodStart)

	mock.ExpectQuery(`UPDATE quotas SET`).
		WithArgs("t_1", int64(17)).
		WillReturnRows(rows)

	got, err := repo.AddRequestCount(context.Background(), "t_1", 17)
	if err != nil {
		t.Fatalf("AddRequestCount: %v", err)
	}
	if got.UsedRequestCount != 17 {
		t.Errorf("UsedRequestCount = %d, want 17", got.UsedRequestCount)
	}
	if got.MaxRequestsPerMonth != 5_000_000 {
		t.Errorf("MaxRequestsPerMonth = %d, want 5000000", got.MaxRequestsPerMonth)
	}
}

func TestQuotaRepository_AddRequestCount_NotFound(t *testing.T) {
	repo, mock, cleanup := newQuotaMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`UPDATE quotas SET`).
		WithArgs("t_missing", int64(5)).
		WillReturnError(sql.ErrNoRows)

	got, err := repo.AddRequestCount(context.Background(), "t_missing", 5)
	if err != nil {
		t.Fatalf("expected nil error for sql.ErrNoRows, got %v", err)
	}
	if got != nil {
		t.Errorf("got %+v, want nil", got)
	}
}

// TestQuotaRepository_AddMemoryMB_Accumulates (issue #44, part 2):
// a positive delta increments used_memory_mb. This is the activate
// path's tx-scoped counter write.
func TestQuotaRepository_AddMemoryMB_Accumulates(t *testing.T) {
	repo, mock, cleanup := newQuotaMockMemoryRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{
		"tenant_id", "max_deployments", "max_apps", "max_workers",
		"max_memory_mb", "max_outbound_mb", "max_requests_per_month",
		"max_resident_seconds_per_month", "used_outbound_bytes",
		"used_request_count", "used_memory_mb", "used_resident_seconds",
		"quota_period_start",
	}).AddRow("t_1", 10, 5, 3, 256, 1000, 100_000, 2_592_000, 0, 0, 256, 0, time.Time{})

	mock.ExpectQuery(`UPDATE quotas SET used_memory_mb = used_memory_mb \+ \$2`).
		WithArgs("t_1", int64(256)).
		WillReturnRows(rows)

	got, err := repo.AddMemoryMB(context.Background(), "t_1", 256)
	if err != nil {
		t.Fatalf("AddMemoryMB: %v", err)
	}
	if got.UsedMemoryMB != 256 {
		t.Errorf("UsedMemoryMB = %d, want 256", got.UsedMemoryMB)
	}
}

// TestQuotaRepository_AddMemoryMB_NegativeDelta (issue #44, part 2):
// the rollback path passes a negative delta. The repo method is
// int64-signed so negative inputs flow through unchanged.
func TestQuotaRepository_AddMemoryMB_NegativeDelta(t *testing.T) {
	repo, mock, cleanup := newQuotaMockMemoryRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{
		"tenant_id", "max_deployments", "max_apps", "max_workers",
		"max_memory_mb", "max_outbound_mb", "max_requests_per_month",
		"max_resident_seconds_per_month", "used_outbound_bytes",
		"used_request_count", "used_memory_mb", "used_resident_seconds",
		"quota_period_start",
	}).AddRow("t_1", 10, 5, 3, 256, 1000, 100_000, 2_592_000, 0, 0, -512, 0, time.Time{})

	mock.ExpectQuery(`UPDATE quotas SET used_memory_mb = used_memory_mb \+ \$2`).
		WithArgs("t_1", int64(-512)).
		WillReturnRows(rows)

	got, err := repo.AddMemoryMB(context.Background(), "t_1", -512)
	if err != nil {
		t.Fatalf("AddMemoryMB: %v", err)
	}
	if got.UsedMemoryMB != -512 {
		t.Errorf("UsedMemoryMB = %d, want -512", got.UsedMemoryMB)
	}
}

// TestQuotaRepository_AddResidentSeconds (issue #484 / #485, third metered
// dimension): a LongRunning heartbeat accumulates into used_resident_seconds.
// Mirrors AddRequestCount — the addColumn routing path is identical so
// the test exercises the same whitelisted-column contract.
func TestQuotaRepository_AddResidentSeconds(t *testing.T) {
	repo, mock, cleanup := newQuotaMockRepo(t)
	defer cleanup()

	periodStart := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{
		"tenant_id", "max_deployments", "max_apps", "max_workers",
		"max_memory_mb", "max_outbound_mb", "max_requests_per_month",
		"max_resident_seconds_per_month", "used_outbound_bytes",
		"used_request_count", "used_memory_mb", "used_resident_seconds",
		"quota_period_start",
	}).AddRow("t_1", 10, 5, 3, 256, 1000, 100_000, 2_592_000, 0, 0, 0, 90, periodStart)

	mock.ExpectQuery(`UPDATE quotas SET`).
		WithArgs("t_1", int64(90)).
		WillReturnRows(rows)

	got, err := repo.AddResidentSeconds(context.Background(), "t_1", 90)
	if err != nil {
		t.Fatalf("AddResidentSeconds: %v", err)
	}
	if got.UsedResidentSeconds != 90 {
		t.Errorf("UsedResidentSeconds = %d, want 90", got.UsedResidentSeconds)
	}
}

// TestQuotaRepository_AddResidentSeconds_NotFound: a missing tenant
// returns (nil, nil) like the other addColumn wrappers. Edge-ingress
// fails open on missing tenants so this path is exercised silently in
// production.
func TestQuotaRepository_AddResidentSeconds_NotFound(t *testing.T) {
	repo, mock, cleanup := newQuotaMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`UPDATE quotas SET`).
		WithArgs("t_missing", int64(60)).
		WillReturnError(sql.ErrNoRows)

	got, err := repo.AddResidentSeconds(context.Background(), "t_missing", 60)
	if err != nil {
		t.Fatalf("expected nil error for sql.ErrNoRows, got %v", err)
	}
	if got != nil {
		t.Errorf("got %+v, want nil", got)
	}
}

// TestQuotaRepository_AddMemoryMB_NoRows: a missing tenant returns
// (nil, nil) like the other addColumn-based wrappers.
func TestQuotaRepository_AddMemoryMB_NoRows(t *testing.T) {
	repo, mock, cleanup := newQuotaMockMemoryRepo(t)
	defer cleanup()

	mock.ExpectQuery(`UPDATE quotas SET used_memory_mb`).
		WithArgs("t_missing", int64(256)).
		WillReturnError(sql.ErrNoRows)

	got, err := repo.AddMemoryMB(context.Background(), "t_missing", 256)
	if err != nil {
		t.Fatalf("expected nil error for sql.ErrNoRows, got %v", err)
	}
	if got != nil {
		t.Errorf("got %+v, want nil", got)
	}
}

// TestQuotaRepository_VerifyMemoryUnderCap_Accepts: cap not yet hit,
// the verifying UPDATE returns the tenant_id row (allowing the deploy).
func TestQuotaRepository_VerifyMemoryUnderCap_Accepts(t *testing.T) {
	repo, mock, cleanup := newQuotaMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"tenant_id"}).AddRow("t_1")

	mock.ExpectQuery(`UPDATE quotas\s+SET used_memory_mb = used_memory_mb \+ 0`).
		WithArgs("t_1", int64(256)).
		WillReturnRows(rows)

	ok, err := repo.VerifyMemoryUnderCap(context.Background(), "t_1", 256)
	if err != nil {
		t.Fatalf("VerifyMemoryUnderCap: %v", err)
	}
	if !ok {
		t.Errorf("got false, want true (within cap)")
	}
}

// TestQuotaRepository_VerifyMemoryUnderCap_Rejects: cap would be
// exceeded; the WHERE short-circuits to false and the query returns
// no rows, which sqlmock surfaces as ErrNoRows we translate to
// (false, nil).
func TestQuotaRepository_VerifyMemoryUnderCap_Rejects(t *testing.T) {
	repo, mock, cleanup := newQuotaMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`UPDATE quotas\s+SET used_memory_mb = used_memory_mb \+ 0`).
		WithArgs("t_1", int64(256)).
		WillReturnError(sql.ErrNoRows)

	ok, err := repo.VerifyMemoryUnderCap(context.Background(), "t_1", 256)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if ok {
		t.Errorf("got true, want false (over cap)")
	}
}

// TestQuotaRepository_VerifyMemoryUnderCap_Unlimited: max_memory_mb=-1
// is the enterprise sentinel — guard against a buggy < vs <= or
// against a future migration to a different sentinel breaking this.
func TestQuotaRepository_VerifyMemoryUnderCap_Unlimited(t *testing.T) {
	repo, mock, cleanup := newQuotaMockRepo(t)
	defer cleanup()

	// Even an absurd perAppMemoryMB must still pass; the SQL has
	// max_memory_mb = -1 OR used_memory_mb + $N <= max_memory_mb,
	// and -1 OR true short-circuits to true.
	mock.ExpectQuery(`UPDATE quotas`).
		WithArgs("t_ent", int64(10_000_000)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("t_ent"))

	ok, err := repo.VerifyMemoryUnderCap(context.Background(), "t_ent", 10_000_000)
	if err != nil {
		t.Fatalf("VerifyMemoryUnderCap: %v", err)
	}
	if !ok {
		t.Errorf("got false, want true (unlimited sentinel)")
	}
}
