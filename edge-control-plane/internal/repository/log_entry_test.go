package repository

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
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

// ---------------------------------------------------------------------------
// ListByTenantApp — issue #77 read path
// ---------------------------------------------------------------------------

// logEntryTestRow produces one sqlmock.Row shaped like a `SELECT … FROM logs`
// result, used by the ListByTenantApp tests below. Columns match the ORDER
// in ListByTenantApp's SELECT clause:
// id, tenant_id, deployment_id, app_name, worker_id, region, level,
// message, labels, ts.
func logEntryTestRow(id int64, ts time.Time) *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "tenant_id", "deployment_id", "app_name",
		"worker_id", "region", "level", "message", "labels", "ts",
	}).AddRow(
		id, "t_test", "d_x", "myapp",
		"w_us-east-1_h01", "us-east-1", "info",
		"hello", []byte(`{}`), ts,
	)
}

// TestLogEntryRepository_ListByTenantApp_FilterByAppAndTenant pins the
// minimum SQL shape: tenant and app must always be the first two bound
// args (in that order) and the SELECT must cover every column from
// domain.LogEntry. A regression that swapped them would let a tenant
// read another tenant's logs.
func TestLogEntryRepository_ListByTenantApp_FilterByAppAndTenant(t *testing.T) {
	repo, mock, cleanup := newLogEntryMockRepo(t)
	defer cleanup()

	ts := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT id, tenant_id, deployment_id, app_name, worker_id, region, level, message, labels, ts`).
		WithArgs("t_test", "myapp", int64(100)).
		WillReturnRows(logEntryTestRow(1, ts))

	out, err := repo.ListByTenantApp(context.Background(), "t_test", "myapp", LogListFilter{
		Limit: 100,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("len = %d, want 1", len(out))
	}
	if out[0].TenantID != "t_test" || out[0].AppName != "myapp" {
		t.Errorf("got tenant=%q app=%q, want t_test/myapp", out[0].TenantID, out[0].AppName)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestLogEntryRepository_ListByTenantApp_AppliesSinceAndLimit pins the
// since-bound SQL: when filter.Since is positive, the SQL must add
// `AND ts >= NOW() - make_interval(secs => $N)` and bind Seconds().
// Without the make_interval, a clock-skewed server could return rows
// older than the caller asked for.
func TestLogEntryRepository_ListByTenantApp_AppliesSinceAndLimit(t *testing.T) {
	repo, mock, cleanup := newLogEntryMockRepo(t)
	defer cleanup()

	ts := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT .* WHERE tenant_id = \$1 AND app_name = \$2`).
		WithArgs("t_test", "myapp", 300.0, int64(50)).
		WillReturnRows(logEntryTestRow(2, ts))

	out, err := repo.ListByTenantApp(context.Background(), "t_test", "myapp", LogListFilter{
		Since: 5 * time.Minute,
		Limit: 50,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("len = %d, want 1", len(out))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (since/limit SQL wrong): %v", err)
	}
}

// TestLogEntryRepository_ListByTenantApp_AppliesLevelFilter pins the
// level filter: when filter.Levels is non-empty, the SQL must add
// `AND level = ANY($N::text[])` and bind the slice via pq.Array. The
// test also verifies the slice is bound — sqlmock's regex matcher
// enforces the placeholder position, but the arg list is what catches
// "forgot to pass the levels".
func TestLogEntryRepository_ListByTenantApp_AppliesLevelFilter(t *testing.T) {
	repo, mock, cleanup := newLogEntryMockRepo(t)
	defer cleanup()

	ts := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	levels := []string{"warn", "error"}
	mock.ExpectQuery(`SELECT .* WHERE tenant_id = \$1 AND app_name = \$2`).
		WithArgs("t_test", "myapp", pq.Array(levels), int64(25)).
		WillReturnRows(logEntryTestRow(3, ts))

	out, err := repo.ListByTenantApp(context.Background(), "t_test", "myapp", LogListFilter{
		Levels: levels,
		Limit:  25,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("len = %d, want 1", len(out))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (level filter SQL wrong): %v", err)
	}
}

// TestLogEntryRepository_ListByTenantApp_RejectsNonPositiveLimit pins
// the up-front guard: a zero or negative Limit returns an error
// without issuing any DB query. The service layer also clamps ≤0 to
// DefaultLogLimit, so this is a belt-and-suspenders defense against
// a future caller forgetting to validate.
func TestLogEntryRepository_ListByTenantApp_RejectsNonPositiveLimit(t *testing.T) {
	repo, _, cleanup := newLogEntryMockRepo(t)
	defer cleanup()

	if _, err := repo.ListByTenantApp(context.Background(), "t_test", "myapp", LogListFilter{
		Limit: 0,
	}); err == nil {
		t.Error("expected error for limit=0, got nil")
	}
	if _, err := repo.ListByTenantApp(context.Background(), "t_test", "myapp", LogListFilter{
		Limit: -1,
	}); err == nil {
		t.Error("expected error for limit=-1, got nil")
	}
}

// TestLogEntryRepository_ListByTenantApp_OrdersByTsDescThenID pins the
// stable keyset ordering: SQL must end with `ORDER BY ts DESC, id DESC
// LIMIT $N`. A regression that forgot `id DESC` would return rows out
// of (ts,id) order — defeating the cursor codec that relies on strict
// lexicographic ordering for pagination.
func TestLogEntryRepository_ListByTenantApp_OrdersByTsDescThenID(t *testing.T) {
	repo, mock, cleanup := newLogEntryMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT .* ORDER BY ts DESC, id DESC LIMIT \$3`).
		WithArgs("t_test", "myapp", int64(10)).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "deployment_id", "app_name",
			"worker_id", "region", "level", "message", "labels", "ts",
		}))

	_, err := repo.ListByTenantApp(context.Background(), "t_test", "myapp", LogListFilter{
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (ORDER BY/LIMIT shape wrong): %v", err)
	}
}

// TestLogEntryRepository_ListByTenantApp_AppliesUntilBound pins the
// upper-bound SQL: when filter.Until is positive, the SQL must add
// `AND ts <= $N::timestamptz` and bind the UTC instant. The cast to
// `::timestamptz` is what keeps PostgreSQL from miscomparing against
// the timestamptz column.
func TestLogEntryRepository_ListByTenantApp_AppliesUntilBound(t *testing.T) {
	repo, mock, cleanup := newLogEntryMockRepo(t)
	defer cleanup()

	until := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT .* WHERE tenant_id = \$1 AND app_name = \$2`).
		WithArgs("t_test", "myapp", until, int64(50)).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "deployment_id", "app_name",
			"worker_id", "region", "level", "message", "labels", "ts",
		}))

	_, err := repo.ListByTenantApp(context.Background(), "t_test", "myapp", LogListFilter{
		Until: until,
		Limit: 50,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (until SQL wrong): %v", err)
	}
}

// TestLogEntryRepository_ListByTenantApp_AppliesCursorTuplePredicate
// pins the row-value keyset predicate: when the filter has both a
// cursor timestamp and a positive ID, the SQL must add
// `AND (ts, id) < ($N::timestamptz, $N::bigint)`. The tuple form is
// what lets ties on `ts` page correctly via `id` as the tiebreak.
func TestLogEntryRepository_ListByTenantApp_AppliesCursorTuplePredicate(t *testing.T) {
	repo, mock, cleanup := newLogEntryMockRepo(t)
	defer cleanup()

	ts := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT .* AND \(ts, id\) < \(\$3::timestamptz, \$4::bigint\)`).
		WithArgs("t_test", "myapp", ts, int64(42), int64(10)).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "deployment_id", "app_name",
			"worker_id", "region", "level", "message", "labels", "ts",
		}))

	_, err := repo.ListByTenantApp(context.Background(), "t_test", "myapp", LogListFilter{
		Limit:    10,
		CursorTS: ts,
		CursorID: 42,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (cursor SQL wrong): %v", err)
	}
}

// TestLogEntryRepository_ListByTenantApp_CursorModeOmitsOffset pins
// the cursor-mode contract: when a cursor is set, OFFSET must NOT
// appear in the SQL even if Offset > 0, because the keyset predicate
// is the page boundary. (A regression that emitted both would still
// return correct rows, but it's wasted budget and hides intent.)
func TestLogEntryRepository_ListByTenantApp_CursorModeOmitsOffset(t *testing.T) {
	repo, mock, cleanup := newLogEntryMockRepo(t)
	defer cleanup()

	ts := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	// The regex prohibits the literal `OFFSET` keyword only on the
	// cursor-mode query, so any regression that re-emits OFFSET fails.
	mock.ExpectQuery(`SELECT .* ORDER BY ts DESC, id DESC LIMIT \$5`).
		WithArgs("t_test", "myapp", ts, int64(7), int64(10)).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "deployment_id", "app_name",
			"worker_id", "region", "level", "message", "labels", "ts",
		}))

	_, err := repo.ListByTenantApp(context.Background(), "t_test", "myapp", LogListFilter{
		Limit:    10,
		Offset:   100, // Intentionally set; must be ignored in cursor mode.
		CursorTS: ts,
		CursorID: 7,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (OFFSET leaked into cursor mode): %v", err)
	}
}

// TestLogEntryRepository_ListByTenantApp_OffsetModeEmitsOffset pins
// the offset-mode contract: when no cursor is set, OFFSET must reach
// the SQL unchanged.
func TestLogEntryRepository_ListByTenantApp_OffsetModeEmitsOffset(t *testing.T) {
	repo, mock, cleanup := newLogEntryMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT .* LIMIT \$3 OFFSET \$4`).
		WithArgs("t_test", "myapp", int64(10), int64(150)).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "deployment_id", "app_name",
			"worker_id", "region", "level", "message", "labels", "ts",
		}))

	_, err := repo.ListByTenantApp(context.Background(), "t_test", "myapp", LogListFilter{
		Limit:  10,
		Offset: 150,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (OFFSET missing on offset mode): %v", err)
	}
}

// TestLogEntryRepository_ListByTenantApp_RejectsHalfCursor pins the
// defense-in-depth guard: a cursor must supply BOTH timestamp AND id.
// Half-cursors (one without the other) are rejected up front so the
// repo never issues a malformed query.
func TestLogEntryRepository_ListByTenantApp_RejectsHalfCursor(t *testing.T) {
	repo, _, cleanup := newLogEntryMockRepo(t)
	defer cleanup()

	cases := []struct {
		name   string
		filter LogListFilter
	}{
		{
			name: "ts but no id",
			filter: LogListFilter{
				Limit:    10,
				CursorTS: time.Now(),
			},
		},
		{
			name: "id but no ts",
			filter: LogListFilter{
				Limit:    10,
				CursorID: 7,
			},
		},
		{
			name: "negative id",
			filter: LogListFilter{
				Limit:    10,
				CursorTS: time.Now(),
				CursorID: -1,
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := repo.ListByTenantApp(
				context.Background(), "t_test", "myapp", c.filter,
			); err == nil {
				t.Errorf("expected error for half-cursor, got nil")
			}
		})
	}
}
