package service

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// stubLister is the minimum implementation of LogEntryLister needed by
// the LogService tests. It records the filter it was called with so
// tests can assert that defaults / clamps propagate to the repo, and
// returns whatever the test sets.
type stubLister struct {
	entries    []domain.LogEntry
	err        error
	called     bool
	lastFilter repository.LogListFilter
}

func (s *stubLister) ListByTenantApp(
	_ context.Context, _, _ string, filter repository.LogListFilter,
) ([]domain.LogEntry, error) {
	s.called = true
	s.lastFilter = filter
	return s.entries, s.err
}

// makeLogEntries builds `n` entries with positive IDs (1..n) and a
// per-call base timestamp. Each successive entry uses a fresh
// microsecond so the cursor codec always sees a unique (ts, id) pair
// — necessary because the equality-tiebreak test in repository/handler
// tests depends on `id` breaking ties.
func makeLogEntries(n int, baseTime string) []domain.LogEntry {
	base, err := time.Parse(time.RFC3339, baseTime)
	if err != nil {
		panic("invalid baseTime in test fixture: " + err.Error())
	}
	out := make([]domain.LogEntry, n)
	for i := 0; i < n; i++ {
		out[i] = domain.LogEntry{
			ID:       int64(i + 1),
			TenantID: "t_test",
			AppName:  "myapp",
			Level:    "info",
			Message:  "hello",
			TS:       base.Add(time.Duration(i) * time.Microsecond).UTC(),
		}
	}
	return out
}

// TestLogService_LogLevelOrdinal_MapsCanonicalLevels pins the level
// rank table. Adding a new level to canonicalLogLevels requires updating
// this map in lockstep; this test is the canary.
func TestLogService_LogLevelOrdinal_MapsCanonicalLevels(t *testing.T) {
	cases := []struct {
		level string
		want  int
	}{
		{"trace", 0},
		{"debug", 1},
		{"info", 2},
		{"warn", 3},
		{"error", 4},
		{"", -1},
		{"critical", -1}, // not in our schema
		{"INFO", -1},     // case-sensitive on purpose
	}
	for _, c := range cases {
		if got := LogLevelOrdinal(c.level); got != c.want {
			t.Errorf("LogLevelOrdinal(%q) = %d, want %d", c.level, got, c.want)
		}
	}
}

// TestLogService_LevelsAtOrAbove pins the level-set expansion. The
// ordering matters: a tenant asking for "warn" should see warn + error,
// not warn + trace.
func TestLogService_LevelsAtOrAbove(t *testing.T) {
	cases := []struct {
		min  string
		want []string
	}{
		{"trace", []string{"trace", "debug", "info", "warn", "error"}},
		{"debug", []string{"debug", "info", "warn", "error"}},
		{"info", []string{"info", "warn", "error"}},
		{"warn", []string{"warn", "error"}},
		{"error", []string{"error"}},
	}
	for _, c := range cases {
		got := LevelsAtOrAbove(c.min)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("LevelsAtOrAbove(%q) = %v, want %v", c.min, got, c.want)
		}
	}
}

// TestLogService_ClampsLimit pins the limit policy: ≤0 → default,
// >MaxLogLimit → max, in-between → unchanged. The service's clamp is
// the only defense against a hostile caller asking for the whole logs
// table.
func TestLogService_ClampsLimit(t *testing.T) {
	cases := []struct {
		name string
		in   int
		want int
	}{
		{"zero -> default", 0, DefaultLogLimit},
		{"negative -> default", -5, DefaultLogLimit},
		{"in-range unchanged", 250, 250},
		{"over max -> max", 2000, MaxLogLimit},
		{"exactly max", MaxLogLimit, MaxLogLimit},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			repo := &stubLister{}
			svc := NewLogService(repo)
			result, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
				Limit: c.in,
			})
			if err != nil {
				t.Fatalf("ListByTenantApp: %v", err)
			}
			// Service requests one probe row above the visible limit.
			if repo.lastFilter.Limit != c.want+1 {
				t.Errorf("repo limit = %d, want %d (visible + probe)", repo.lastFilter.Limit, c.want+1)
			}
			if result.Limit != c.want {
				t.Errorf("effective limit = %d, want %d", result.Limit, c.want)
			}
		})
	}
}

// TestService_ResolveLimit pins the exported helper's contract directly.
func TestService_ResolveLimit(t *testing.T) {
	cases := []struct {
		in   int
		want int
	}{
		{-1, DefaultLogLimit},
		{0, DefaultLogLimit},
		{1, 1},
		{DefaultLogLimit, DefaultLogLimit},
		{MaxLogLimit, MaxLogLimit},
		{MaxLogLimit + 1, MaxLogLimit},
	}
	for _, c := range cases {
		if got := ResolveLimit(c.in); got != c.want {
			t.Errorf("ResolveLimit(%d) = %d, want %d", c.in, c.want, got)
		}
	}
}

// TestLogService_AppliesDefaultSince pins the since-default policy.
func TestLogService_AppliesDefaultSince(t *testing.T) {
	repo := &stubLister{}
	svc := NewLogService(repo)
	_, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
		Since: 0,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	if repo.lastFilter.Since != DefaultLogSince {
		t.Errorf("repo since = %s, want %s (default)", repo.lastFilter.Since, DefaultLogSince)
	}
}

// TestLogService_RejectsUnknownLevel pins the error path.
func TestLogService_RejectsUnknownLevel(t *testing.T) {
	repo := &stubLister{}
	svc := NewLogService(repo)
	_, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
		MinLvl: "critical",
		Limit:  10,
	})
	if !errors.Is(err, ErrInvalidLevel) {
		t.Errorf("err = %v, want ErrInvalidLevel", err)
	}
}

// TestLogService_TranslatesLevelToLevelSet pins the query translation.
func TestLogService_TranslatesLevelToLevelSet(t *testing.T) {
	repo := &stubLister{}
	svc := NewLogService(repo)
	_, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
		MinLvl: "warn",
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	want := []string{"warn", "error"}
	if !reflect.DeepEqual(repo.lastFilter.Levels, want) {
		t.Errorf("repo levels = %v, want %v", repo.lastFilter.Levels, want)
	}
}

// TestLogService_NoLevelFilterOmitsLevels pins the empty-MinLvl case.
func TestLogService_NoLevelFilterOmitsLevels(t *testing.T) {
	repo := &stubLister{}
	svc := NewLogService(repo)
	_, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	if repo.lastFilter.Levels != nil {
		t.Errorf("repo levels = %v, want nil", repo.lastFilter.Levels)
	}
}

// TestLogService_PropagatesRepoError pins the error pass-through.
func TestLogService_PropagatesRepoError(t *testing.T) {
	wantErr := errors.New("db unreachable")
	repo := &stubLister{err: wantErr}
	svc := NewLogService(repo)
	_, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
		Limit: 10,
	})
	if !errors.Is(err, wantErr) {
		t.Errorf("err = %v, want %v", err, wantErr)
	}
}

// TestLogService_FirstPageWithProbeRowReturnsBothHints pins the
// first/offset cursor compatibility: a probe row (limit+1 entries)
// yields BOTH next_cursor and next_offset so clients on a current
// server can use either. The cursor is built from the LAST VISIBLE
// entry, not from the probe row, because the probe row is trimmed.
func TestLogService_FirstPageWithProbeRowReturnsBothHints(t *testing.T) {
	const visible = 50
	repo := &stubLister{
		entries: makeLogEntries(visible+1, "2026-07-14T12:00:00Z"),
	}
	svc := NewLogService(repo)
	result, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
		Limit: visible,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	if len(result.Entries) != visible {
		t.Fatalf("len(Entries) = %d, want %d (trimmed to limit)", len(result.Entries), visible)
	}
	if result.NextCursor == nil {
		t.Fatal("expected NextCursor set when probe row detected another page")
	}
	if result.NextOffset == nil {
		t.Fatal("expected NextOffset set on first/offset path when probe row detected")
	} else if *result.NextOffset != visible {
		t.Errorf("NextOffset = %d, want %d", *result.NextOffset, visible)
	}
	// Cursor should encode the LAST visible row's (ts, id) — not the
	// probe row, which was trimmed.
	last := result.Entries[visible-1]
	wantTS, wantID := last.TS, last.ID
	encTS, encID, err := decodeLogCursor(*result.NextCursor)
	if err != nil {
		t.Fatalf("decode NextCursor: %v", err)
	}
	if !encTS.Equal(wantTS) || encID != wantID {
		t.Errorf("cursor encodes (%v, %d), want (%v, %d)", encTS, encID, wantTS, wantID)
	}
}

// TestLogService_NoNextHintWhenFinalPageExactlyFull fixes the
// phantom-next-page regression: a final page of EXACTLY limit rows
// must NOT emit next_cursor or next_offset. The previous len==limit
// assertion conflated "full" with "another page exists".
func TestLogService_NoNextHintWhenFinalPageExactlyFull(t *testing.T) {
	const visible = 100
	repo := &stubLister{
		entries: makeLogEntries(visible, "2026-07-14T12:00:00Z"),
	}
	svc := NewLogService(repo)
	result, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
		Limit: visible,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	if len(result.Entries) != visible {
		t.Fatalf("len = %d, want %d", len(result.Entries), visible)
	}
	if result.NextCursor != nil {
		t.Errorf("NextCursor = %q, want nil (final page)", *result.NextCursor)
	}
	if result.NextOffset != nil {
		t.Errorf("NextOffset = %d, want nil (final page)", *result.NextOffset)
	}
}

// TestLogService_NoNextOffsetWhenPartialPage omits both hints when
// the result has fewer than limit entries (last page).
func TestLogService_NoNextOffsetWhenPartialPage(t *testing.T) {
	repo := &stubLister{
		entries: makeLogEntries(3, "2026-07-14T12:00:00Z"),
	}
	svc := NewLogService(repo)
	result, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
		Limit: 100,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	if result.NextCursor != nil {
		t.Errorf("NextCursor = %q, want nil (partial page)", *result.NextCursor)
	}
	if result.NextOffset != nil {
		t.Errorf("NextOffset = %d, want nil (partial page)", *result.NextOffset)
	}
}

// TestLogService_CursorModeSuppressesNextOffset pins the cursor-only
// contract: when the caller supplies a cursor, the service emits only
// next_cursor; next_offset stays null even if another page exists.
func TestLogService_CursorModeSuppressesNextOffset(t *testing.T) {
	// First request populates a real cursor from the probe row.
	probe := &stubLister{
		entries: makeLogEntries(11, "2026-07-14T12:00:00Z"),
	}
	first, err := NewLogService(probe).ListByTenantApp(
		context.Background(), "t_test", "myapp", LogQuery{Limit: 10},
	)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.NextCursor == nil {
		t.Fatal("first page must yield a cursor to continue the test")
	}

	// Second page receives the cursor; the (limit+1) probe row shows
	// another page is available. The contract says next_offset must
	// stay nil in cursor mode.
	probe2 := &stubLister{
		entries: makeLogEntries(11, "2026-07-14T12:00:00Z"),
	}
	result, err := NewLogService(probe2).ListByTenantApp(
		context.Background(), "t_test", "myapp", LogQuery{
			Limit:  10,
			Cursor: *first.NextCursor,
		},
	)
	if err != nil {
		t.Fatalf("ListByTenantApp with cursor: %v", err)
	}
	if result.NextOffset != nil {
		t.Errorf("NextOffset = %d, want nil in cursor mode", *result.NextOffset)
	}
	if result.NextCursor == nil {
		t.Fatal("NextCursor must be set when probe row detects another page")
	}
	// Repository must see the cursor, not an offset.
	if probe2.lastFilter.CursorTS.IsZero() {
		t.Error("expected repository CursorTS to be populated")
	}
	if probe2.lastFilter.CursorID == 0 {
		t.Error("expected repository CursorID to be populated")
	}
	if probe2.lastFilter.Offset != 0 {
		t.Errorf("expected repository Offset = 0 in cursor mode, got %d", probe2.lastFilter.Offset)
	}
}

// TestLogService_CursorModeRejectsMalformedCursor pins the error path
// when the caller hands us a cursor that doesn't decode.
func TestLogService_CursorModeRejectsMalformedCursor(t *testing.T) {
	repo := &stubLister{}
	svc := NewLogService(repo)
	_, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
		Limit:  10,
		Cursor: "not-a-cursor",
	})
	if !errors.Is(err, ErrInvalidLogCursor) {
		t.Errorf("err = %v, want ErrInvalidLogCursor", err)
	}
}

// TestLogService_OffsetPropagatesToRepo pins that offset reaches the
// repository filter unchanged (only on offset mode, not cursor mode).
func TestLogService_OffsetPropagatesToRepo(t *testing.T) {
	repo := &stubLister{}
	svc := NewLogService(repo)
	_, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
		Limit:  50,
		Offset: 150,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	if repo.lastFilter.Offset != 150 {
		t.Errorf("repo offset = %d, want 150", repo.lastFilter.Offset)
	}
}

// TestLogService_UntilPropagatesToRepo pins that an absolute upper
// bound (Until) reaches the repository as a Time value untouched by
// the service.
func TestLogService_UntilPropagatesToRepo(t *testing.T) {
	repo := &stubLister{}
	svc := NewLogService(repo)
	until := time.Date(2026, 7, 14, 13, 0, 0, 0, time.UTC)
	_, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
		Limit: 50,
		Until: until,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	if !repo.lastFilter.Until.Equal(until) {
		t.Errorf("repo until = %s, want %s", repo.lastFilter.Until, until)
	}
}

// TestLogService_CursorAndLevelPropagateTogether pins that the
// service correctly composes a cursor with a minimum-severity
// level filter. The repository integration test
// (TestLogEntryRepoIntegration_CombinedFiltersCompose) covers the
// SQL contract; this test pins the in-Go combination — that the
// service decodes the cursor into (ts, id), translates the level
// into the set of accepted levels, and hands BOTH to the repo
// (rather than silently dropping one when the other is present).
func TestLogService_CursorAndLevelPropagateTogether(t *testing.T) {
	repo := &stubLister{}
	svc := NewLogService(repo)
	cursor, err := encodeLogCursor(time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC), 99)
	if err != nil {
		t.Fatalf("encodeLogCursor: %v", err)
	}
	_, err = svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
		Limit:  50,
		MinLvl: "warn",
		Cursor: cursor,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	wantLevels := LevelsAtOrAbove("warn")
	if !reflect.DeepEqual(repo.lastFilter.Levels, wantLevels) {
		t.Errorf("repo levels = %v, want %v", repo.lastFilter.Levels, wantLevels)
	}
	if repo.lastFilter.CursorID != 99 {
		t.Errorf("repo cursor id = %d, want 99", repo.lastFilter.CursorID)
	}
	wantTS := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	if !repo.lastFilter.CursorTS.Equal(wantTS) {
		t.Errorf("repo cursor ts = %s, want %s", repo.lastFilter.CursorTS, wantTS)
	}
}

// TestLogService_RejectsCursorAfterUntil pins that the service
// rejects a request whose cursor ts is strictly after the supplied
// `until`. Without this check, the strict-tuple predicate and the
// `ts <= until` clause would silently produce an empty page — the
// client would think "no more rows" when they really meant "rows
// up to a different time bound". The handler maps the returned
// ErrInvalidLogCursor sentinel to a 400 (see
// TestLogsList_RejectsCursorAndOffsetTogether for the existing
// handler-level cursor rejection path).
func TestLogService_RejectsCursorAfterUntil(t *testing.T) {
	repo := &stubLister{}
	svc := NewLogService(repo)
	// Cursor encodes (ts=13:00:00Z, id=42). `until` is 12:00:00Z,
	// so cursor.ts is strictly after until.
	cursor, err := encodeLogCursor(time.Date(2026, 7, 14, 13, 0, 0, 0, time.UTC), 42)
	if err != nil {
		t.Fatalf("encodeLogCursor: %v", err)
	}
	until := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	_, err = svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
		Limit:  50,
		Until:  until,
		Cursor: cursor,
	})
	if !errors.Is(err, ErrInvalidLogCursor) {
		t.Fatalf("err = %v, want ErrInvalidLogCursor", err)
	}
	// Repo must NOT be called when the request is invalid.
	if repo.called {
		t.Errorf("repo called despite cursor-after-until rejection")
	}
}

// TestLogService_DefaultsSinceIsConstant pins the value of the
// exported default so a future refactor that accidentally changes the
// number trips this test before the CLI starts sending wrong bounds.
func TestLogService_DefaultsSinceIsConstant(t *testing.T) {
	if DefaultLogSince != 5*time.Minute {
		t.Errorf("DefaultLogSince = %s, want 5m", DefaultLogSince)
	}
	if MaxLogLimit != 1000 {
		t.Errorf("MaxLogLimit = %d, want 1000", MaxLogLimit)
	}
	if DefaultLogLimit != 100 {
		t.Errorf("DefaultLogLimit = %d, want 100", DefaultLogLimit)
	}
}
