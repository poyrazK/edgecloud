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
	lastFilter repository.LogListFilter
}

func (s *stubLister) ListByTenantApp(
	_ context.Context, _, _ string, filter repository.LogListFilter,
) ([]domain.LogEntry, error) {
	s.lastFilter = filter
	return s.entries, s.err
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
			if repo.lastFilter.Limit != c.want {
				t.Errorf("repo limit = %d, want %d", repo.lastFilter.Limit, c.want)
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
			t.Errorf("ResolveLimit(%d) = %d, want %d", c.in, got, c.want)
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

// TestLogService_NextOffsetWhenFullPage returns a NextOffset when the
// result has exactly limit entries (more rows may exist).
func TestLogService_NextOffsetWhenFullPage(t *testing.T) {
	repo := &stubLister{
		entries: make([]domain.LogEntry, MaxLogLimit),
	}
	svc := NewLogService(repo)
	result, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
		Limit:  MaxLogLimit,
		Offset: 500,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	if result.NextOffset == nil {
		t.Fatal("expected NextOffset to be set when result is full")
	}
	want := 500 + MaxLogLimit
	if *result.NextOffset != want {
		t.Errorf("NextOffset = %d, want %d", *result.NextOffset, want)
	}
}

// TestLogService_NoNextOffsetWhenPartialPage omits NextOffset when the
// result has fewer than limit entries (last page).
func TestLogService_NoNextOffsetWhenPartialPage(t *testing.T) {
	repo := &stubLister{
		entries: make([]domain.LogEntry, 3),
	}
	svc := NewLogService(repo)
	result, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
		Limit: 100,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	if result.NextOffset != nil {
		t.Errorf("NextOffset = %d, want nil (partial page)", *result.NextOffset)
	}
}

// TestLogService_OffsetPropagatesToRepo pins that offset reaches the
// repository filter unchanged.
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
