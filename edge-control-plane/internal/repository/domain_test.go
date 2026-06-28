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

func newDomainMockRepo(t *testing.T) (*DomainRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return NewDomainRepository(sqlxDB), mock, func() { _ = mockDB.Close() }
}

func TestDomainRepository_Create(t *testing.T) {
	repo, mock, cleanup := newDomainMockRepo(t)
	defer cleanup()

	now := time.Now()
	d := &domain.Domain{
		ID:        "dom_1",
		TenantID:  "t_1",
		AppName:   "hello",
		FQDN:      "hello.edgecloud.dev",
		Status:    domain.DomainStatusPending,
		CreatedAt: now,
	}

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO domains`)).
		WithArgs(d.ID, d.TenantID, d.AppName, d.FQDN, d.Status, d.CreatedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Create(context.Background(), d); err != nil {
		t.Fatalf("Create: %v", err)
	}
}

func TestDomainRepository_GetByID(t *testing.T) {
	repo, mock, cleanup := newDomainMockRepo(t)
	defer cleanup()

	now := time.Now()
	errStr := "DNS timeout"
	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "app_name", "fqdn", "status", "last_error", "created_at", "verified_at",
	}).AddRow("dom_1", "t_1", "hello", "hello.edgecloud.dev", domain.DomainStatusPending, &errStr, now, &now)

	mock.ExpectQuery(`SELECT id.*FROM domains WHERE`).
		WithArgs("dom_1").
		WillReturnRows(rows)

	got, err := repo.GetByID(context.Background(), "dom_1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.ID != "dom_1" {
		t.Errorf("ID = %q, want dom_1", got.ID)
	}
	if got.LastError == nil || *got.LastError != "DNS timeout" {
		t.Errorf("LastError = %v", got.LastError)
	}
	if got.VerifiedAt == nil {
		t.Error("VerifiedAt = nil, want non-nil")
	}
}

func TestDomainRepository_GetByID_NotFound(t *testing.T) {
	repo, mock, cleanup := newDomainMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT id.*FROM domains WHERE`).
		WithArgs("bad_id").
		WillReturnError(sql.ErrNoRows)

	got, err := repo.GetByID(context.Background(), "bad_id")
	if err != nil {
		t.Fatalf("expected nil error for sql.ErrNoRows, got %v", err)
	}
	if got != nil {
		t.Errorf("got %+v, want nil", got)
	}
}

func TestDomainRepository_GetByFQDN(t *testing.T) {
	repo, mock, cleanup := newDomainMockRepo(t)
	defer cleanup()

	now := time.Now()
	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "app_name", "fqdn", "status", "last_error", "created_at", "verified_at",
	}).AddRow("dom_1", "t_1", "hello", "hello.edgecloud.dev", domain.DomainStatusActive, nil, now, nil)

	mock.ExpectQuery(`SELECT id.*FROM domains WHERE fqdn`).
		WithArgs("hello.edgecloud.dev").
		WillReturnRows(rows)

	got, err := repo.GetByFQDN(context.Background(), "hello.edgecloud.dev")
	if err != nil {
		t.Fatalf("GetByFQDN: %v", err)
	}
	if got.FQDN != "hello.edgecloud.dev" {
		t.Errorf("FQDN = %q", got.FQDN)
	}
}

func TestDomainRepository_ListByApp(t *testing.T) {
	repo, mock, cleanup := newDomainMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "app_name", "fqdn", "status", "last_error", "created_at", "verified_at",
	}).AddRow("dom_1", "t_1", "hello", "a.edgecloud.dev", domain.DomainStatusPending, nil, time.Now(), nil).
		AddRow("dom_2", "t_1", "hello", "b.edgecloud.dev", domain.DomainStatusActive, nil, time.Now(), nil)

	mock.ExpectQuery(`SELECT id.*FROM domains WHERE.*ORDER BY created_at DESC`).
		WithArgs("t_1", "hello").
		WillReturnRows(rows)

	domains, err := repo.ListByApp(context.Background(), "t_1", "hello")
	if err != nil {
		t.Fatalf("ListByApp: %v", err)
	}
	if len(domains) != 2 {
		t.Errorf("len = %d, want 2", len(domains))
	}
}

func TestDomainRepository_CountByApp(t *testing.T) {
	repo, mock, cleanup := newDomainMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"count"}).AddRow(3)
	mock.ExpectQuery(`SELECT COUNT.*FROM domains`).
		WithArgs("t_1", "hello").
		WillReturnRows(rows)

	got, err := repo.CountByApp(context.Background(), "t_1", "hello")
	if err != nil {
		t.Fatalf("CountByApp: %v", err)
	}
	if got != 3 {
		t.Errorf("CountByApp = %d, want 3", got)
	}
}

func TestDomainRepository_ListAll(t *testing.T) {
	repo, mock, cleanup := newDomainMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "app_name", "fqdn", "status", "last_error", "created_at", "verified_at",
	}).AddRow("dom_1", "t_1", "hello", "h.edgecloud.dev", domain.DomainStatusPending, nil, time.Now(), nil)

	mock.ExpectQuery(`SELECT id.*FROM domains ORDER BY created_at`).
		WillReturnRows(rows)

	domains, err := repo.ListAll(context.Background())
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(domains) != 1 {
		t.Errorf("len = %d, want 1", len(domains))
	}
}

func TestDomainRepository_AtomicDelete_Found(t *testing.T) {
	repo, mock, cleanup := newDomainMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"bool"}).AddRow(true)
	mock.ExpectQuery(regexp.QuoteMeta(`DELETE FROM domains WHERE`)).
		WithArgs("t_1", "hello", "hello.edgecloud.dev").
		WillReturnRows(rows)

	got, err := repo.AtomicDelete(context.Background(), "t_1", "hello", "hello.edgecloud.dev")
	if err != nil {
		t.Fatalf("AtomicDelete: %v", err)
	}
	if !got {
		t.Errorf("AtomicDelete = false, want true")
	}
}

func TestDomainRepository_AtomicDelete_NotFound(t *testing.T) {
	repo, mock, cleanup := newDomainMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`DELETE FROM domains WHERE`)).
		WithArgs("t_1", "hello", "missing.edgecloud.dev").
		WillReturnError(sql.ErrNoRows)

	got, err := repo.AtomicDelete(context.Background(), "t_1", "hello", "missing.edgecloud.dev")
	if err != nil {
		t.Fatalf("AtomicDelete: %v", err)
	}
	if got {
		t.Errorf("AtomicDelete = true, want false for not-found")
	}
}

func TestDomainRepository_UpdateStatus_SetsFields(t *testing.T) {
	repo, mock, cleanup := newDomainMockRepo(t)
	defer cleanup()

	errStr := "cert expired"
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE domains SET status`)).
		WithArgs("dom_1", domain.DomainStatusFailed, &errStr).
		WillReturnResult(sqlmock.NewResult(0, 1)) // rows affected = 1

	got, err := repo.UpdateStatus(context.Background(), "dom_1", domain.DomainStatusFailed, &errStr)
	if err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}
	if !got {
		t.Error("UpdateStatus = false, want true (row found)")
	}
}

func TestDomainRepository_UpdateStatus_NotFound(t *testing.T) {
	repo, mock, cleanup := newDomainMockRepo(t)
	defer cleanup()

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE domains SET status`)).
		WithArgs("bad_id", domain.DomainStatusActive, (*string)(nil)).
		WillReturnResult(sqlmock.NewResult(0, 0)) // rows affected = 0

	got, err := repo.UpdateStatus(context.Background(), "bad_id", domain.DomainStatusActive, nil)
	if err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}
	if got {
		t.Error("UpdateStatus = true, want false (row not found)")
	}
}
