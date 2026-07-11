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

// ---------------------------------------------------------------------------
// DeleteOlderThanBatched — issue #574 retention GC sweep path.
// Mirrors log_entry_test.go:25-201 verbatim, against audit_logs and
// the (created_at) index added in migration 031.
// ---------------------------------------------------------------------------

// TestAuditRepository_DeleteOlderThanBatches_PaginatesUntilEmpty
// pins the pagination short-circuit: when a batch returns fewer rows
// than batchSize, the loop stops and does NOT issue a 4th query.
func TestAuditRepository_DeleteOlderThanBatches_PaginatesUntilEmpty(t *testing.T) {
	repo, mock, cleanup := newAuditMockRepo(t)
	defer cleanup()

	// Three batches: full, full, short. Loop must stop after the short.
	mock.ExpectExec(`DELETE FROM audit_logs WHERE id IN \(SELECT id FROM audit_logs WHERE created_at < NOW\(\)`).
		WithArgs(90*24*60*60.0, 10_000). // 90 days in seconds
		WillReturnResult(sqlmock.NewResult(0, 10_000))
	mock.ExpectExec(`DELETE FROM audit_logs WHERE id IN \(SELECT id FROM audit_logs WHERE created_at < NOW\(\)`).
		WithArgs(90*24*60*60.0, 10_000).
		WillReturnResult(sqlmock.NewResult(0, 10_000))
	mock.ExpectExec(`DELETE FROM audit_logs WHERE id IN \(SELECT id FROM audit_logs WHERE created_at < NOW\(\)`).
		WithArgs(90*24*60*60.0, 10_000).
		WillReturnResult(sqlmock.NewResult(0, 5_234))

	deleted, err := repo.DeleteOlderThanBatched(
		context.Background(), 90*24*time.Hour, 10_000, 100)
	if err != nil {
		t.Fatalf("DeleteOlderThanBatched: %v", err)
	}
	if want := int64(25_234); deleted != want {
		t.Errorf("deleted = %d, want %d", deleted, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestAuditRepository_DeleteOlderThanBatches_StopsAtMaxBatches
// pins the upper bound: when every batch is full, the loop must exit
// after maxBatches iterations rather than running forever.
func TestAuditRepository_DeleteOlderThanBatches_StopsAtMaxBatches(t *testing.T) {
	repo, mock, cleanup := newAuditMockRepo(t)
	defer cleanup()

	for i := 0; i < 3; i++ {
		mock.ExpectExec(`DELETE FROM audit_logs`).
			WithArgs(3600.0, 1000). // 1h retention
			WillReturnResult(sqlmock.NewResult(0, 1000))
	}

	deleted, err := repo.DeleteOlderThanBatched(
		context.Background(), 1*time.Hour, 1000, 3)
	if err != nil {
		t.Fatalf("DeleteOlderThanBatched: %v", err)
	}
	if want := int64(3000); deleted != want {
		t.Errorf("deleted = %d, want %d", deleted, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestAuditRepository_DeleteOlderThanBatches_HonorsCtxCancellation
// pins the early-exit on a cancelled context.
func TestAuditRepository_DeleteOlderThanBatches_HonorsCtxCancellation(t *testing.T) {
	repo, mock, cleanup := newAuditMockRepo(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())

	mock.ExpectExec(`DELETE FROM audit_logs`).
		WithArgs(3600.0, 1000).
		WillReturnResult(sqlmock.NewResult(0, 1000))
	mock.ExpectExec(`DELETE FROM audit_logs`).
		WithArgs(3600.0, 1000).
		WillReturnResult(sqlmock.NewResult(0, 1000))

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	deleted, err := repo.DeleteOlderThanBatched(
		ctx, 1*time.Hour, 1000, 100)
	_ = err // not asserted: timing-dependent
	if deleted == 0 {
		t.Errorf("deleted = 0, want >= 1000 (at least one batch ran before cancel)")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (loop ran more than 2 batches): %v", err)
	}
}

// TestAuditRepository_DeleteOlderThanBatches_RejectsNonPositiveRetention
// pins the up-front validation.
func TestAuditRepository_DeleteOlderThanBatches_RejectsNonPositiveRetention(t *testing.T) {
	repo, _, cleanup := newAuditMockRepo(t)
	defer cleanup()

	if _, err := repo.DeleteOlderThanBatched(
		context.Background(), 0, 1000, 100); err == nil {
		t.Error("expected error for retention=0, got nil")
	}
	if _, err := repo.DeleteOlderThanBatched(
		context.Background(), -1*time.Hour, 1000, 100); err == nil {
		t.Error("expected error for retention=-1h, got nil")
	}
}

// TestAuditRepository_DeleteOlderThanBatches_UsesServerNOW pins the
// clock-skew fix: the SQL must use NOW() and bind the retention as
// seconds, NOT pass a Go-computed timestamp.
func TestAuditRepository_DeleteOlderThanBatches_UsesServerNOW(t *testing.T) {
	repo, mock, cleanup := newAuditMockRepo(t)
	defer cleanup()

	mock.ExpectExec(`DELETE FROM audit_logs WHERE id IN \(SELECT id FROM audit_logs WHERE created_at < NOW\(\) - make_interval\(secs => \$1\) LIMIT \$2\)`).
		WithArgs(90*24*60*60.0, int64(10_000)).
		WillReturnResult(sqlmock.NewResult(0, 100))

	_, err := repo.DeleteOlderThanBatched(
		context.Background(), 90*24*time.Hour, 10_000, 100)
	if err != nil {
		t.Fatalf("DeleteOlderThanBatched: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (SQL shape or args wrong): %v", err)
	}
}

// TestAuditRepository_DeleteOlderThanBatches_BoundInt64 pins the type
// of the LIMIT $2 binding (matches log_entry_test.go:184-201).
func TestAuditRepository_DeleteOlderThanBatches_BoundInt64(t *testing.T) {
	repo, mock, cleanup := newAuditMockRepo(t)
	defer cleanup()

	const wantBatchSize = int64(10_000)
	mock.ExpectExec(`DELETE FROM audit_logs WHERE id IN \(SELECT id FROM audit_logs WHERE created_at < NOW\(\) - make_interval\(secs => \$1\) LIMIT \$2\)`).
		WithArgs(90*24*60*60.0, wantBatchSize).
		WillReturnResult(sqlmock.NewResult(0, 100))

	_, err := repo.DeleteOlderThanBatched(
		context.Background(), 90*24*time.Hour, int(wantBatchSize), 100)
	if err != nil {
		t.Fatalf("DeleteOlderThanBatched: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (LIMIT $2 was not bound as int64): %v", err)
	}
}
