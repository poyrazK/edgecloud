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

	q := domain.DefaultQuota("t_1")

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO quotas`)).
		WithArgs(q.TenantID, q.MaxDeployments, q.MaxApps, q.MaxWorkers, q.MaxMemoryMB, q.MaxOutboundMB).
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
		"max_memory_mb", "max_outbound_mb", "used_outbound_bytes", "quota_period_start",
	}).AddRow("t_1", 10, 5, 3, 256, 1000, 0, periodStart)

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

	q := domain.DefaultQuota("t_1")
	q.MaxDeployments = 50
	q.MaxApps = 20

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE quotas SET`)).
		WithArgs(q.TenantID, q.MaxDeployments, q.MaxApps, q.MaxWorkers, q.MaxMemoryMB, q.MaxOutboundMB).
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
		"max_memory_mb", "max_outbound_mb", "used_outbound_bytes", "quota_period_start",
	}).AddRow("t_1", 10, 5, 3, 256, 1000, 42, periodStart)

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
