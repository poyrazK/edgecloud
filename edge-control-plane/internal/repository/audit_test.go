package repository

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

func newAuditMockRepo(t *testing.T) (*AuditRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return NewAuditRepository(sqlxDB), mock, func() { _ = mockDB.Close() }
}

func TestAuditRepository_Insert(t *testing.T) {
	repo, mock, cleanup := newAuditMockRepo(t)
	defer cleanup()

	ev := &domain.AuditEvent{
		TenantID:   "t_abc",
		APIKeyID:   "k_abc",
		Role:       "developer",
		Action:     "delete",
		Resource:   "app",
		ResourceID: "my-app",
		Details:    "app my-app deleted",
		Outcome:    "success",
		ErrorMsg:   "",
		RequestIP:  "10.0.0.1",
		CreatedAt:  time.Now(),
	}

	rows := sqlmock.NewRows([]string{"id"}).AddRow(int64(1))
	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO audit_logs`)).
		WithArgs(ev.TenantID, ev.APIKeyID, ev.Role, ev.Action, ev.Resource, ev.ResourceID,
			ev.Details, ev.Outcome, ev.ErrorMsg, ev.RequestIP).
		WillReturnRows(rows)

	got, err := repo.Insert(context.Background(), ev)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if got != 1 {
		t.Errorf("got id %d, want 1", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestAuditRepository_Insert_EmptyFields(t *testing.T) {
	repo, mock, cleanup := newAuditMockRepo(t)
	defer cleanup()

	ev := &domain.AuditEvent{
		TenantID:   "",
		APIKeyID:   "",
		Role:       "",
		Action:     "bootstrap",
		Resource:   "tenant",
		ResourceID: "t_new",
		Details:    "tenant t_new created via self-signup",
		Outcome:    "success",
		ErrorMsg:   "",
		RequestIP:  "",
	}

	rows := sqlmock.NewRows([]string{"id"}).AddRow(int64(2))
	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO audit_logs`)).
		WithArgs(ev.TenantID, ev.APIKeyID, ev.Role, ev.Action, ev.Resource, ev.ResourceID,
			ev.Details, ev.Outcome, ev.ErrorMsg, ev.RequestIP).
		WillReturnRows(rows)

	got, err := repo.Insert(context.Background(), ev)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if got != 2 {
		t.Errorf("got id %d, want 2", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}
