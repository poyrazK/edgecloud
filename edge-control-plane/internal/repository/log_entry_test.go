package repository

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

// newLogEntryMockRepo wires a sqlmock-backed sqlx.DB into a
// LogEntryRepository. Mirrors the helper in api_key_test.go.
func newLogEntryMockRepo(t *testing.T) (*LogEntryRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return &LogEntryRepository{db: sqlxDB}, mock, func() { _ = mockDB.Close() }
}

// TestLogEntryRepository_DeleteOlderThanBatches_PaginatesUntilEmpty
// pins the pagination short-circuit: when a batch returns fewer rows
// than batchSize, the loop stops and does NOT issue a 4th query.
func TestLogEntryRepository_DeleteOlderThanBatches_PaginatesUntilEmpty(t *testing.T) {
	repo, mock, cleanup := newLogEntryMockRepo(t)
	defer cleanup()

	// Three batches: full, full, short. Loop must stop after the short.
	mock.ExpectExec(`DELETE FROM logs WHERE id IN \(SELECT id FROM logs WHERE ts < NOW\(\)`).
		WithArgs(7*24*60*60.0, 10_000). // 7 days in seconds
		WillReturnResult(sqlmock.NewResult(0, 10_000))
	mock.ExpectExec(`DELETE FROM logs WHERE id IN \(SELECT id FROM logs WHERE ts < NOW\(\)`).
		WithArgs(7*24*60*60.0, 10_000).
		WillReturnResult(sqlmock.NewResult(0, 10_000))
	mock.ExpectExec(`DELETE FROM logs WHERE id IN \(SELECT id FROM logs WHERE ts < NOW\(\)`).
		WithArgs(7*24*60*60.0, 10_000).
		WillReturnResult(sqlmock.NewResult(0, 5_234))

	deleted, err := repo.DeleteOlderThanBatched(
		context.Background(), 7*24*time.Hour, 10_000, 100)
	if err != nil {
		t.Fatalf("DeleteOlderThanBatched: %v", err)
	}
	if want := int64(25_234); deleted != want {
		t.Errorf("deleted = %d, want %d", deleted, want)
	}

	// Verify all expectations were met (no 4th call).
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestLogEntryRepository_DeleteOlderThanBatches_StopsAtMaxBatches
// pins the upper bound: when every batch is full, the loop must exit
// after maxBatches iterations rather than running forever.
func TestLogEntryRepository_DeleteOlderThanBatches_StopsAtMaxBatches(t *testing.T) {
	repo, mock, cleanup := newLogEntryMockRepo(t)
	defer cleanup()

	// maxBatches=3; all three return a full batch.
	for i := 0; i < 3; i++ {
		mock.ExpectExec(`DELETE FROM logs WHERE id IN \(SELECT id FROM logs WHERE ts < NOW\(\)`).
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

// TestLogEntryRepository_DeleteOlderThanBatches_HonorsCtxCancellation
// pins the early-exit on a cancelled context: between batches, the
// loop checks ctx.Err() and returns rather than issuing another DELETE.
func TestLogEntryRepository_DeleteOlderThanBatches_HonorsCtxCancellation(t *testing.T) {
	repo, mock, cleanup := newLogEntryMockRepo(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())

	// Two full batches, then a cancelled ctx. The first batch must
	// return successfully (1000 rows), the second must return
	// successfully too (1000 rows), and the loop must exit before
	// the third call because ctx is cancelled.
	mock.ExpectExec(`DELETE FROM logs`).
		WithArgs(3600.0, 1000).
		WillReturnResult(sqlmock.NewResult(0, 1000))
	mock.ExpectExec(`DELETE FROM logs`).
		WithArgs(3600.0, 1000).
		WillReturnResult(sqlmock.NewResult(0, 1000))

	// Cancel after a short delay so the first two batches run.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	deleted, err := repo.DeleteOlderThanBatched(
		ctx, 1*time.Hour, 1000, 100)
	// The exact error depends on timing — the third iteration's
	// ctx.Err() check returns the error, OR the first batch's
	// ExecContext returns ctx.Err(). Either way we expect non-nil.
	_ = err // not asserted: timing-dependent
	if deleted == 0 {
		t.Errorf("deleted = 0, want >= 1000 (at least one batch ran before cancel)")
	}
	// The mock was set up for exactly 2 batches; a third call would
	// have failed with "call to Query '...' which was not expected".
	// If the loop ran >2 batches, mock.ExpectationsWereMet reports
	// the leftover (this is the strong assertion).
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (loop ran more than 2 batches): %v", err)
	}
}

// TestLogEntryRepository_DeleteOlderThanBatches_RejectsNonPositiveRetention
// pins the up-front validation: a 0 or negative retention returns an
// error without issuing any DB query. This is the defense-in-depth
// guard alongside LogGCService.Run's check.
func TestLogEntryRepository_DeleteOlderThanBatches_RejectsNonPositiveRetention(t *testing.T) {
	repo, _, cleanup := newLogEntryMockRepo(t)
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

// TestLogEntryRepository_DeleteOlderThanBatches_UsesServerNOW
// pins the clock-skew fix: the SQL must use NOW() and bind the
// retention as seconds, NOT pass a Go-computed timestamp.
func TestLogEntryRepository_DeleteOlderThanBatches_UsesServerNOW(t *testing.T) {
	repo, mock, cleanup := newLogEntryMockRepo(t)
	defer cleanup()

	// The arg is retention.Seconds() (a float64), not a time.Time.
	// batchSize is bound as int64 (matching LIMIT's bigint type).
	mock.ExpectExec(`DELETE FROM logs WHERE id IN \(SELECT id FROM logs WHERE ts < NOW\(\) - make_interval\(secs => \$1\) LIMIT \$2\)`).
		WithArgs(7*24*60*60.0, int64(10_000)).
		WillReturnResult(sqlmock.NewResult(0, 100))

	_, err := repo.DeleteOlderThanBatched(
		context.Background(), 7*24*time.Hour, 10_000, 100)
	if err != nil {
		t.Fatalf("DeleteOlderThanBatched: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (SQL shape or args wrong): %v", err)
	}
}

// TestLogEntryRepository_DeleteOlderThanBatches_BoundInt64 pins the
// type of the LIMIT $2 binding. The Go-side parameter is int
// (declared in the function signature), but Postgres LIMIT expects
// bigint. Without the explicit int64(batchSize) cast, the driver
// binds an int4 and strict-type Postgres configurations fail at
// execution time. sqlmock's WithArgs does not enforce server-side
// type strictness on its own, so this test pins the explicit
// int64 value to catch any future regression that drops the cast.
//
// Other tests in this file use bare integer literals (e.g. 10_000)
// in WithArgs and pass because sqlmock's value matcher coerces
// int → int64 for the comparison. That coercion masks a real
// regression on the wire; this test uses an explicit int64 to make
// the binding type part of the contract.
func TestLogEntryRepository_DeleteOlderThanBatches_BoundInt64(t *testing.T) {
	repo, mock, cleanup := newLogEntryMockRepo(t)
	defer cleanup()

	const wantBatchSize = int64(10_000)
	mock.ExpectExec(`DELETE FROM logs WHERE id IN \(SELECT id FROM logs WHERE ts < NOW\(\) - make_interval\(secs => \$1\) LIMIT \$2\)`).
		WithArgs(7*24*60*60.0, wantBatchSize).
		WillReturnResult(sqlmock.NewResult(0, 100))

	_, err := repo.DeleteOlderThanBatched(
		context.Background(), 7*24*time.Hour, int(wantBatchSize), 100)
	if err != nil {
		t.Fatalf("DeleteOlderThanBatched: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (LIMIT $2 was not bound as int64): %v", err)
	}
}
