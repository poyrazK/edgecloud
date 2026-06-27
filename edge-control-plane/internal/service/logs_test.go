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
// table. The effective-limit second return value MUST match what the
// repo got (handler echoes it back without re-clamping).
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
			_, got, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
				Limit: c.in,
			})
			if err != nil {
				t.Fatalf("ListByTenantApp: %v", err)
			}
			if repo.lastFilter.Limit != c.want {
				t.Errorf("repo limit = %d, want %d", repo.lastFilter.Limit, c.want)
			}
			if got != c.want {
				t.Errorf("effective limit = %d, want %d", got, c.want)
			}
		})
	}
}

// TestService_ResolveLimit pins the exported helper's contract directly
// (the handler calls it... no, the handler echoes the value from
// ListByTenantApp; ResolveLimit is the function the service itself
// delegates to). Pinning it separately means a refactor that swaps the
// implementation can keep the contract.
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

// TestLogService_AppliesDefaultSince pins the since-default policy:
// zero or negative Since → DefaultLogSince. A negative value comes
// from a future-dated RFC3339 (handler clamps to 0 before calling) or
// a clock-skewed client; either way the service should not propagate
// a negative into the SQL.
func TestLogService_AppliesDefaultSince(t *testing.T) {
	repo := &stubLister{}
	svc := NewLogService(repo)
	_, _, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
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

// TestLogService_RejectsUnknownLevel pins the error path: an unknown
// level must surface as ErrInvalidLevel (not a generic error) so the
// handler can map it to 400.
func TestLogService_RejectsUnknownLevel(t *testing.T) {
	repo := &stubLister{}
	svc := NewLogService(repo)
	_, _, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
		MinLvl: "critical",
		Limit:  10,
	})
	if !errors.Is(err, ErrInvalidLevel) {
		t.Errorf("err = %v, want ErrInvalidLevel", err)
	}
}

// TestLogService_TranslatesLevelToLevelSet pins the query translation:
// a non-empty MinLvl produces a Levels slice with every level at or
// above it. The repo uses this slice for the level = ANY(...) filter.
func TestLogService_TranslatesLevelToLevelSet(t *testing.T) {
	repo := &stubLister{}
	svc := NewLogService(repo)
	_, _, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
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

// TestLogService_NoLevelFilterOmitsLevels pins the empty-MinLvl case:
// when no level is requested, the repo must see a nil Levels slice
// (not the full canonical set, which would still match everything
// but cost a recheck).
func TestLogService_NoLevelFilterOmitsLevels(t *testing.T) {
	repo := &stubLister{}
	svc := NewLogService(repo)
	_, _, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListByTenantApp: %v", err)
	}
	if repo.lastFilter.Levels != nil {
		t.Errorf("repo levels = %v, want nil", repo.lastFilter.Levels)
	}
}

// TestLogService_PropagatesRepoError pins the error pass-through:
// any non-ErrInvalidLevel error from the repo must reach the handler
// unchanged (the handler maps it to 500).
func TestLogService_PropagatesRepoError(t *testing.T) {
	wantErr := errors.New("db unreachable")
	repo := &stubLister{err: wantErr}
	svc := NewLogService(repo)
	_, _, err := svc.ListByTenantApp(context.Background(), "t_test", "myapp", LogQuery{
		Limit: 10,
	})
	if !errors.Is(err, wantErr) {
		t.Errorf("err = %v, want %v", err, wantErr)
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
