package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// mockAuditGCRepo is the mock the AuditGCService tests run against.
// Mirrors mockLogGCRepo (log_gc_test.go:15-52) — same shape, different
// interface.
type mockAuditGCRepo struct {
	mu    sync.Mutex
	calls []time.Duration
	err   error
	delay time.Duration
}

func (m *mockAuditGCRepo) DeleteOlderThanBatched(ctx context.Context, retention time.Duration, batchSize, maxBatches int) (int64, error) {
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, retention)
	if m.err != nil {
		return 0, m.err
	}
	return 0, nil
}

func (m *mockAuditGCRepo) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *mockAuditGCRepo) lastRetention() (time.Duration, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.calls) == 0 {
		return 0, false
	}
	return m.calls[len(m.calls)-1], true
}

// recordingAuditSink mirrors recordingLogSink (log_gc_test.go:285-335)
// — same shape, typed to AuditGCSink.
type recordingAuditSink struct {
	mu    sync.Mutex
	calls []logSinkCall
}

func (r *recordingAuditSink) record(rowsDeleted int64, hadError bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, logSinkCall{rowsDeleted, hadError})
}

func (r *recordingAuditSink) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func makeRecordingAuditSink() (AuditGCSink, *recordingAuditSink) {
	r := &recordingAuditSink{}
	var sink AuditGCSink = r.record
	return sink, r
}

func TestAuditGC_DeletesOldRows(t *testing.T) {
	repo := &mockAuditGCRepo{}
	svc := NewAuditGCService(repo, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		interval  = 10 * time.Second
		retention = 90 * 24 * time.Hour
	)

	done := make(chan struct{})
	go func() {
		svc.Run(ctx, interval, retention)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)

	gotRetention, ok := repo.lastRetention()
	if !ok {
		t.Fatal("DeleteOlderThanBatched was not called on first sweep")
	}
	if got := repo.callCount(); got != 1 {
		t.Errorf("DeleteOlderThanBatched call count = %d, want 1", got)
	}
	if gotRetention != retention {
		t.Errorf("retention = %s, want %s", gotRetention, retention)
	}
	cancel()
	<-done
}

func TestAuditGC_RetentionFromConfig(t *testing.T) {
	repo := &mockAuditGCRepo{}
	svc := NewAuditGCService(repo, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.Run(ctx, 10*time.Second, 1*time.Hour)
	time.Sleep(20 * time.Millisecond)
	got1, _ := repo.lastRetention()
	cancel()
	time.Sleep(20 * time.Millisecond)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go svc.Run(ctx2, 10*time.Second, 90*24*time.Hour)
	time.Sleep(20 * time.Millisecond)
	got2, _ := repo.lastRetention()
	cancel2()

	if got1 != 1*time.Hour {
		t.Errorf("first retention = %s, want 1h", got1)
	}
	if got2 != 90*24*time.Hour {
		t.Errorf("second retention = %s, want 90d", got2)
	}
}

func TestAuditGC_TickerFiresAtInterval(t *testing.T) {
	repo := &mockAuditGCRepo{}
	svc := NewAuditGCService(repo, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 30*time.Millisecond, 1*time.Hour)
		close(done)
	}()
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	if got := repo.callCount(); got < 3 {
		t.Errorf("DeleteOlderThan call count = %d, want at least 3", got)
	}
}

func TestAuditGC_RepoErrorDoesNotStopLoop(t *testing.T) {
	repo := &mockAuditGCRepo{err: errors.New("simulated DB outage")}
	svc := NewAuditGCService(repo, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 30*time.Millisecond, 1*time.Hour)
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	if got := repo.callCount(); got < 2 {
		t.Errorf("DeleteOlderThan call count = %d, want >= 2 (loop must continue after errors)", got)
	}
}

func TestAuditGC_ZeroIntervalRefusesToRun(t *testing.T) {
	repo := &mockAuditGCRepo{}
	svc := NewAuditGCService(repo, nil)

	done := make(chan struct{})
	go func() {
		svc.Run(context.Background(), 0, 1*time.Hour)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return for interval=0")
	}
	if got := repo.callCount(); got != 0 {
		t.Errorf("DeleteOlderThan call count = %d, want 0", got)
	}
}

func TestAuditGC_NegativeRetentionRefusesToRun(t *testing.T) {
	repo := &mockAuditGCRepo{}
	svc := NewAuditGCService(repo, nil)

	done := make(chan struct{})
	go func() {
		svc.Run(context.Background(), time.Hour, -1*time.Hour)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return for retention=-1h")
	}
	if got := repo.callCount(); got != 0 {
		t.Errorf("DeleteOlderThan call count = %d, want 0", got)
	}
}

// Timing note: this test depends on `Run` reading `ctx.Err()`
// synchronously before the first tick — the pre-cancelled ctx is
// observed by `runOnce` (or by the loopHealth boundary above it)
// immediately, so the goroutine returns before `DeleteOlderThanBatched`
// is ever invoked. If `Run` ever grows a pre-loop blocking call
// (e.g. a warm-up query) this test will start to deadlock instead of
// short-circuiting, and the assertion below would need to learn to
// tolerate it.
func TestAuditGC_PreemptsOnCancelledContext(t *testing.T) {
	repo := &mockAuditGCRepo{}
	svc := NewAuditGCService(repo, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 10*time.Millisecond, 1*time.Hour)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit; should short-circuit on pre-cancelled ctx")
	}
	if got := repo.callCount(); got != 0 {
		t.Errorf("DeleteOlderThan call count = %d, want 0", got)
	}
}

func TestAuditGC_RecordsMetrics(t *testing.T) {
	sink, rec := makeRecordingAuditSink()
	repo := &mockAuditGCRepo{}
	svc := NewAuditGCService(repo, sink)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.Run(ctx, 30*time.Millisecond, 1*time.Hour)
	time.Sleep(90 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)

	if got := rec.callCount(); got < 2 {
		t.Errorf("sink call count = %d, want >= 2 (immediate + at least 1 tick)", got)
	}
}

func TestAuditGC_NilSink_NoPanic(t *testing.T) {
	repo := &mockAuditGCRepo{}
	svc := NewAuditGCService(repo, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 30*time.Millisecond, 1*time.Hour)
		close(done)
	}()
	time.Sleep(60 * time.Millisecond)
	cancel()
	<-done
}

// mockAuditGCRepoWithCounting returns a stateful repo that simulates
// (firstN rows, ok) → (zeroN rows, ok) across successive calls. Mirrors
// mockLogGCRepoWithCounting (log_gc_test.go:337-368) — same shape,
// different interface.
type mockAuditGCRepoWithCounting struct {
	mu     sync.Mutex
	calls  int
	firstN int64
	zeroN  int64
}

func (m *mockAuditGCRepoWithCounting) DeleteOlderThanBatched(_ context.Context, _ time.Duration, _, _ int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.calls == 1 {
		return m.firstN, nil
	}
	return m.zeroN, nil
}

func (m *mockAuditGCRepoWithCounting) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// TestAuditGC_RecordsMetrics_TickCountEqualsSweepCount: a healthy repo
// (0 rows, no errors) records one sink call per sweep — guards against a
// regression where the sink fires twice (or zero times) per sweep tick.
func TestAuditGC_RecordsMetrics_TickCountEqualsSweepCount(t *testing.T) {
	repo := &mockAuditGCRepoWithCounting{firstN: 0, zeroN: 0}
	sink, rec := makeRecordingAuditSink()
	svc := NewAuditGCService(repo, sink)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		svc.Run(ctx, 30*time.Millisecond, 1*time.Hour)
	}()
	time.Sleep(120 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)

	sweeps := repo.callCount()
	if sweeps < 3 {
		t.Errorf("repo.callCount = %d, want >= 3", sweeps)
	}
	if got := rec.callCount(); got != sweeps {
		t.Errorf("sink call count = %d, want %d (one sink call per sweep)", got, sweeps)
	}
	for i, c := range rec.calls {
		if c.rowsDeleted != 0 {
			t.Errorf("sink call[%d] rowsDeleted = %d, want 0", i, c.rowsDeleted)
		}
		if c.hadError {
			t.Errorf("sink call[%d] hadError = true, want false on healthy repo", i)
		}
	}
}
