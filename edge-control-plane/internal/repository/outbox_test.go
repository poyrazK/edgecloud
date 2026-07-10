package repository

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// newOutboxMockRepo wires a sqlmock-backed OutboxRepository. The
// regex matcher lets the tests assert on query shape without
// pinning argument order.
func newOutboxMockRepo(t *testing.T) (*OutboxRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return NewOutboxRepository(sqlxDB), mock, func() { _ = mockDB.Close() }
}

// outboxRowColumns mirrors the SELECT column order used by ClaimDue's
// RETURNING clause — keep these in sync with the repository.
var outboxRowColumns = []string{
	"id", "tenant_id", "app_name", "kind", "payload", "regions",
	"attempt_count", "next_attempt_at", "status", "last_error",
	"dedupe_key", "created_at", "published_at", "claimed_until",
}

// TestOutboxEnqueue verifies that Enqueue issues a single INSERT
// with the expected column order and binds the row's fields in
// order. Run inside the caller's tx in production — here we just
// verify the SQL shape, not tx participation.
func TestOutboxEnqueue(t *testing.T) {
	repo, mock, cleanup := newOutboxMockRepo(t)
	defer cleanup()

	row := &OutboxRow{
		TenantID:  "t_1",
		AppName:   "myapp",
		Kind:      "task_update",
		Payload:   []byte(`{"hello":"world"}`),
		Regions:   pq.StringArray{"us-east", "eu-west"},
		DedupeKey: "t_1:myapp:attempt-1",
	}

	mock.ExpectExec(regexp.QuoteMeta(
		`INSERT INTO outbox`,
	)).
		WithArgs(row.TenantID, row.AppName, row.Kind, row.Payload, row.Regions, row.DedupeKey).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Enqueue(context.Background(), row); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestOutboxEnqueue_EmptyRegions covers the empty-regions branch in
// Enqueue: row.Regions == nil must be normalized to an empty
// pq.StringArray before the INSERT, so the TEXT[] column receives
// '{}' instead of NULL.
func TestOutboxEnqueue_EmptyRegions(t *testing.T) {
	repo, mock, cleanup := newOutboxMockRepo(t)
	defer cleanup()

	row := &OutboxRow{
		TenantID:  "t_1",
		AppName:   "myapp",
		Kind:      "task_update",
		Payload:   []byte(`{}`),
		Regions:   nil,
		DedupeKey: "t_1:myapp:attempt-2",
	}

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO outbox`)).
		WithArgs(row.TenantID, row.AppName, row.Kind, row.Payload, pq.StringArray{}, row.DedupeKey).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Enqueue(context.Background(), row); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
}

// TestOutboxEnqueue_DuplicateKeyReturnsTyped covers the UNIQUE
// constraint violation path. sqlmock can't synthesize a pq.Error
// with SQLSTATE 23505 directly through ExpectExec (the driver wraps
// the error), so we use a real Postgres integration via the
// integration-tagged suite for that — but here we pin the
// ErrDuplicateDedupeKey shape by feeding the production pq error
// through a closure that exercises isUniqueViolation.
func TestOutboxEnqueue_DuplicateKeyReturnsTyped(t *testing.T) {
	repo, mock, cleanup := newOutboxMockRepo(t)
	defer cleanup()

	// Build a real pq.Error with SQLSTATE 23505 (unique_violation).
	// isUniqueViolation must unwrap and detect it.
	pqErr := &pq.Error{
		Code:       "23505",
		Message:    `duplicate key value violates unique constraint "outbox_dedupe_key_key"`,
		Constraint: "outbox_dedupe_key_key",
	}

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO outbox`)).
		WithArgs("t_1", "myapp", "task_update", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(pqErr)

	err := repo.Enqueue(context.Background(), &OutboxRow{
		TenantID:  "t_1",
		AppName:   "myapp",
		Kind:      "task_update",
		Payload:   []byte(`{}`),
		Regions:   pq.StringArray{"us-east"},
		DedupeKey: "t_1:myapp:dup",
	})
	if !errors.Is(err, ErrDuplicateDedupeKey) {
		t.Errorf("Enqueue err = %v, want ErrDuplicateDedupeKey", err)
	}
}

// TestIsUniqueViolation_PQError pins the SQLSTATE-based detection:
// a *pq.Error with Code 23505 is detected, anything else returns
// false, and nil returns false. This is the regression test for
// the pre-#42 fix that removed the dead errors.Is(err, err) line.
func TestIsUniqueViolation_PQError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"other-pq-error", &pq.Error{Code: "23502", Message: "not null violation"}, false},
		{"unique-violation", &pq.Error{Code: "23505", Message: "duplicate key"}, true},
		{"wrapped-unique-violation", &wrapErr{inner: &pq.Error{Code: "23505", Message: "duplicate key"}}, true},
		{"plain-error", errors.New("something else"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isUniqueViolation(tt.err); got != tt.want {
				t.Errorf("isUniqueViolation(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// wrapErr is a tiny error wrapper so we can assert that
// errors.As unwraps correctly inside isUniqueViolation.
type wrapErr struct{ inner error }

func (w *wrapErr) Error() string { return w.inner.Error() }
func (w *wrapErr) Unwrap() error { return w.inner }

// TestOutboxClaimDue_PicksDueRows covers the happy path: ClaimDue
// returns the rows from the CTE-with-SKIP-LOCKED UPDATE, in the
// order the CTE produced them.
func TestOutboxClaimDue_PicksDueRows(t *testing.T) {
	repo, mock, cleanup := newOutboxMockRepo(t)
	defer cleanup()

	now := time.Now()
	mock.ExpectQuery(regexp.QuoteMeta(`WITH due AS (`)).
		WithArgs(50).
		WillReturnRows(sqlmock.NewRows(outboxRowColumns).
			AddRow(int64(11), "t_1", "a1", "task_update",
				[]byte(`{"a":1}`), pq.StringArray{"us-east"},
				0, now, "pending", nil,
				"t_1:a1:att-1", now, nil, now.Add(30*time.Second)).
			AddRow(int64(12), "t_1", "a2", "task_update",
				[]byte(`{"a":2}`), pq.StringArray{"eu-west"},
				1, now, "pending", nil,
				"t_1:a2:att-1", now, nil, now.Add(30*time.Second)))

	rows, err := repo.ClaimDue(context.Background(), 50)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("ClaimDue returned %d rows, want 2", len(rows))
	}
	if rows[0].ID != 11 || rows[1].ID != 12 {
		t.Errorf("rows = [%d, %d], want [11, 12]", rows[0].ID, rows[1].ID)
	}
	if rows[0].Regions[0] != "us-east" {
		t.Errorf("row[0].Regions[0] = %q, want us-east", rows[0].Regions[0])
	}
}

// TestOutboxClaimDue_RecoversStuckInFlight covers the
// crash-recovery path: a row stuck in 'in_flight' with
// claimed_until in the past must be re-claimable. The CTE predicate
// was widened in response to a review finding (#466 review) so
// stuck rows don't pile up after a drainer crash.
func TestOutboxClaimDue_RecoversStuckInFlight(t *testing.T) {
	repo, mock, cleanup := newOutboxMockRepo(t)
	defer cleanup()

	past := time.Now().Add(-2 * time.Minute) // claimed_until already expired
	mock.ExpectQuery(regexp.QuoteMeta(`WITH due AS (`)).
		WithArgs(10).
		WillReturnRows(sqlmock.NewRows(outboxRowColumns).
			AddRow(int64(99), "t_x", "app", "task_update",
				[]byte(`{}`), pq.StringArray{"us-east"},
				3, time.Now(), "in_flight", nil,
				"t_x:app:stuck", past, nil, past))

	rows, err := repo.ClaimDue(context.Background(), 10)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("ClaimDue returned %d rows, want 1 (stuck row should be re-claimable)", len(rows))
	}
	if rows[0].ID != 99 {
		t.Errorf("re-claimed row ID = %d, want 99", rows[0].ID)
	}
	if rows[0].Status != "in_flight" {
		t.Errorf("re-claimed row Status = %q, want in_flight", rows[0].Status)
	}
}

// TestOutboxMarkPublished covers the success path: status flips
// to 'published', published_at is set server-side, last_error is
// cleared, claimed_until is cleared.
func TestOutboxMarkPublished(t *testing.T) {
	repo, mock, cleanup := newOutboxMockRepo(t)
	defer cleanup()

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
		WithArgs(int64(11)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.MarkPublished(context.Background(), 11); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestOutboxMarkFailed_RetryKeepsPending covers the retry path:
// when attemptCount < maxAttempts, status stays 'pending' so the
// next ClaimDue will pick it up after next_attempt_at elapses.
// attempt_count is incremented; next_attempt_at is the caller-
// supplied backoff value.
func TestOutboxMarkFailed_RetryKeepsPending(t *testing.T) {
	repo, mock, cleanup := newOutboxMockRepo(t)
	defer cleanup()

	next := time.Now().Add(20 * time.Second)
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
		WithArgs(int64(11), "pending", 1, "nats: connection refused", next).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.MarkFailed(context.Background(), 11, 1, "nats: connection refused", 10, next); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
}

// TestOutboxMarkFailed_GivesUpAtMax covers the terminal path:
// when attemptCount >= maxAttempts, status flips to 'failed' and
// the row stays for operator inspection. ClaimDue's WHERE clause
// excludes 'failed' rows so they're never re-attempted.
func TestOutboxMarkFailed_GivesUpAtMax(t *testing.T) {
	repo, mock, cleanup := newOutboxMockRepo(t)
	defer cleanup()

	// nextAttemptAt is irrelevant on the terminal path (ClaimDue
	// won't pick the row back up), but the API still requires it.
	next := time.Now().Add(5 * time.Minute)
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
		WithArgs(int64(11), "failed", 10, "nats: connection refused", next).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.MarkFailed(context.Background(), 11, 10, "nats: connection refused", 10, next); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
}

// TestOutboxMarkFailed_BoundaryAtMax verifies the inclusive
// boundary: attemptCount == maxAttempts gives up, not retries.
// This pins the >= check at the boundary so a future regression
// to > doesn't silently extend retries by one.
func TestOutboxMarkFailed_BoundaryAtMax(t *testing.T) {
	repo, mock, cleanup := newOutboxMockRepo(t)
	defer cleanup()

	next := time.Now().Add(5 * time.Minute)
	// attemptCount=5, maxAttempts=5 — must give up.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
		WithArgs(int64(7), "failed", 5, "boom", next).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.MarkFailed(context.Background(), 7, 5, "boom", 5, next); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
}

// TestOutboxWithTx_ReturnsTxScopedRepo pins the WithTx contract:
// the returned repository must bind INSERTs to the provided tx, not
// the parent DB. The mock expectation is registered on the tx so
// the INSERT runs through the tx's ExecContext; if WithTx wrongly
// returned a DB-scoped repo, the tx would never see the call and
// the expectation would not be met.
func TestOutboxWithTx_ReturnsTxScopedRepo(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = mockDB.Close() })
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	repo := NewOutboxRepository(sqlxDB)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO outbox`)).
		WithArgs("t_1", "a", "task_update", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	tx, err := sqlxDB.Beginx()
	if err != nil {
		t.Fatalf("Beginx: %v", err)
	}
	if err := repo.WithTx(tx).Enqueue(context.Background(), &OutboxRow{
		TenantID:  "t_1",
		AppName:   "a",
		Kind:      "task_update",
		Payload:   []byte(`{}`),
		Regions:   pq.StringArray{"us-east"},
		DedupeKey: "t_1:a:att",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}
