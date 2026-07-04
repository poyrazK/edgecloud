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
		"used_outbound_bytes", "used_request_count", "quota_period_start",
	}).AddRow("t_1", 10, 5, 3, 256, 1000, 100_000, 0, 0, periodStart)

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
		"used_outbound_bytes", "used_request_count", "quota_period_start",
	}).AddRow("t_1", 10, 5, 3, 256, 1000, 100_000, 42, 0, periodStart)

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
		"used_outbound_bytes", "used_request_count", "quota_period_start",
	}).AddRow("t_1", 50, 20, 10, 512, 10_000, 5_000_000, 0, 17, periodStart)

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
