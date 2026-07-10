// Package loophealth tracks the liveness of long-running background
// goroutines in the control plane.
//
// The control plane spawns six top-level loops from (*app.App).RunBackground
// (heartbeat, log_gc, reconcile, worker_gc, deployment_gc, autoscale) plus
// two inner goroutines inside WorkerService.SubscribeHeartbeats. Without
// `defer recover()`, a panic in any of these kills the goroutine silently
// while /health keeps reporting "ok". This package provides:
//
//   - A Tracker that owns per-loop state (start time, last beat, panic
//     count, running flag).
//   - Two thin wrappers, Run and RunErr, that recover panics, bump the
//     counters, and log with a caller-supplied log function.
//   - A Snapshot() method that returns JSON-marshalable per-loop state
//     sorted by name, including a computed "stale" flag.
//
// Logging is injected via the LogFn callback so the package stays logger
// agnostic: callers pass log.Printf for stdlib log or a small adapter for
// log/slog (used by the autoscale package).
//
// The package does not implement graceful shutdown drain (issue #443's
// "L5" follow-up) or loop restart logic. Both are explicitly deferred.
package loophealth

import (
	"context"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultStaleAfter is how long a loop may go without bumping lastBeatAt
// before Snapshot() flags it as stale. Two minutes is generous: all six
// loops in RunBackground have intervals <= 1h, and the heartbeat drain is
// event-driven (its running flag is the primary liveness signal, not the
// timestamp).
const DefaultStaleAfter = 2 * time.Minute

// LogFn is the function signature used by Run / RunErr to log recovered
// panics. The wrapper formats with fmt.Sprintf semantics so callers can
// pass log.Printf, fmt.Printf, or a slog adapter unchanged.
type LogFn func(format string, args ...any)

// Loop holds the per-name state for one background goroutine.
type Loop struct {
	name       string
	startedAt  atomic.Int64 // unix nanos; written once at first entry
	lastBeatAt atomic.Int64 // unix nanos; bumped on entry and via Beat()
	panics     atomic.Int64 // count of recovered panics
	running    atomic.Bool  // true while inside the wrapper body

	// done is closed when the loop's wrapper body has fully returned —
	// including any panic-recovery logging via the caller-supplied
	// LogFn. waitForExited blocks on this channel so callers can
	// observe the wrapper's post-state (e.g. log output, panics
	// counter) without racing the goroutine on memory visibility.
	//
	// doneInit guards the lazy initialization of done. The wrapper
	// body is expected to run at most once per loop name (per
	// (Tracker).Run / RunErr convention in app.RunBackground), so a
	// single sync.Once is sufficient — a second invocation would need
	// a new channel, which is the caller's responsibility, not ours.
	doneOnce sync.Once
	done     chan struct{}
}

// Done returns a channel that is closed when the wrapper body has
// fully exited (including panic-recovery logging). Safe for
// concurrent use; returns the same channel across calls because
// Loop is single-shot per name.
func (l *Loop) Done() <-chan struct{} {
	ch := l.initDone()
	return ch
}

// initDone lazily creates the done channel and returns it (send-end)
// so the wrapper body can close it via defer. Callers outside the
// package should use Done() instead. The wrapper body is expected
// to run at most once per loop name (per (*Tracker).Run / RunErr
// convention in app.RunBackground); a second invocation would need
// a fresh channel, which is the caller's responsibility.
func (l *Loop) initDone() chan struct{} {
	l.doneOnce.Do(func() {
		l.done = make(chan struct{})
	})
	return l.done
}

// Name returns the loop's registered name.
func (l *Loop) Name() string { return l.name }

// StartedAt returns the time at which the wrapper body first entered.
// Zero if the loop has never been started.
func (l *Loop) StartedAt() time.Time {
	v := l.startedAt.Load()
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(0, v)
}

// LastBeatAt returns the time of the most recent Beat() call (including
// the implicit beat on entry to the wrapper body). Zero if no beat has
// happened yet.
func (l *Loop) LastBeatAt() time.Time {
	v := l.lastBeatAt.Load()
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(0, v)
}

// Panics returns the number of recovered panics.
func (l *Loop) Panics() int64 { return l.panics.Load() }

// RecordPanic increments the panic counter by one. Exposed for tests
// that want to simulate a recovered panic without running the
// recover path; production code never calls this — the wrapper
// itself bumps the counter on a real recovered panic.
func (l *Loop) RecordPanic() { l.panics.Add(1) }

// Running reports whether the loop body is currently executing.
func (l *Loop) Running() bool { return l.running.Load() }

// Beat records that the loop made progress. Useful for loops that want to
// bump liveness from inside their own tick handlers.
func (l *Loop) Beat() { l.lastBeatAt.Store(time.Now().UnixNano()) }

// State is the JSON-marshalable view of a loop, suitable for inclusion in
// the /health response body.
type State struct {
	Name       string `json:"name"`
	StartedAt  string `json:"started_at,omitempty"`   // RFC3339
	LastBeatAt string `json:"last_beat_at,omitempty"` // RFC3339
	Panics     int64  `json:"panics"`
	Running    bool   `json:"running"`
	Stale      bool   `json:"stale"`
}

// Tracker is the shared registry of loops. Construct one per process and
// pass it to Run / RunErr / Get. Snapshot is safe for concurrent reads.
type Tracker struct {
	mu         sync.RWMutex
	loops      map[string]*Loop
	staleAfter atomic.Int64 // nanoseconds
}

// NewTracker constructs an empty Tracker with DefaultStaleAfter.
func NewTracker() *Tracker {
	t := &Tracker{loops: make(map[string]*Loop)}
	t.staleAfter.Store(int64(DefaultStaleAfter))
	return t
}

// SetStaleAfter overrides the staleness threshold. Zero resets to the
// default. Safe for concurrent use; the field is read at Snapshot() time.
func (t *Tracker) SetStaleAfter(d time.Duration) {
	if d <= 0 {
		d = DefaultStaleAfter
	}
	t.staleAfter.Store(int64(d))
}

// StaleAfter returns the current staleness threshold.
func (t *Tracker) StaleAfter() time.Duration {
	return time.Duration(t.staleAfter.Load())
}

// Get returns the Loop registered under name, lazily creating it on first
// access. Safe for concurrent use.
func (t *Tracker) Get(name string) *Loop {
	t.mu.RLock()
	l, ok := t.loops[name]
	t.mu.RUnlock()
	if ok {
		return l
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if l, ok = t.loops[name]; ok {
		return l
	}
	l = &Loop{name: name}
	t.loops[name] = l
	return l
}

// Snapshot returns the JSON-marshalable state of every registered loop,
// sorted by name for stable output. The Stale field is computed against
// the current time and StaleAfter threshold.
func (t *Tracker) Snapshot() []State {
	t.mu.RLock()
	loops := make([]*Loop, 0, len(t.loops))
	for _, l := range t.loops {
		loops = append(loops, l)
	}
	t.mu.RUnlock()

	sort.Slice(loops, func(i, j int) bool { return loops[i].name < loops[j].name })

	now := time.Now().UnixNano()
	threshold := t.StaleAfter()
	out := make([]State, 0, len(loops))
	for _, l := range loops {
		s := State{
			Name:    l.name,
			Panics:  l.panics.Load(),
			Running: l.running.Load(),
		}
		if ts := l.startedAt.Load(); ts != 0 {
			s.StartedAt = time.Unix(0, ts).UTC().Format(time.RFC3339)
		}
		if ts := l.lastBeatAt.Load(); ts != 0 {
			s.LastBeatAt = time.Unix(0, ts).UTC().Format(time.RFC3339)
			s.Stale = now-ts > int64(threshold)
		}
		out = append(out, s)
	}
	return out
}

// Run wraps a no-return ctx loop in its own goroutine, so callers can't
// forget the `go` keyword (issue #443 review finding #1 — the
// pre-fix call sites in (*app.App).RunBackground did). Recovers panics,
// logs them via logFn with prefix, and bumps the per-loop panic counter.
// The wrapper writes startedAt once (on first entry), bumps lastBeatAt
// on every entry, and toggles running across the body.
//
// logFn receives fmt.Sprintf-style arguments; pass log.Printf for the
// stdlib log package or a small adapter for log/slog.
func (t *Tracker) Run(ctx context.Context, name, prefix string, logFn LogFn, fn func(context.Context)) {
	go t.runInner(ctx, name, prefix, logFn, func(context.Context) error {
		fn(ctx)
		return nil
	})
}

// RunErr is like Run but for loops that return an error. A non-nil error
// return is logged via logFn with prefix (same shape as the existing
// `log.Printf("%ssubscription error: %v", ...)` wrappers in app.go). The
// loop still exits — only ctx cancellation or a panic changes lifecycle.
//
// As with Run, the goroutine is spawned inside the wrapper; callers do
// not (and must not) prefix the call with `go`.
func (t *Tracker) RunErr(ctx context.Context, name, prefix string, logFn LogFn, fn func(context.Context) error) {
	go t.runInner(ctx, name, prefix, logFn, fn)
}

func (t *Tracker) runInner(ctx context.Context, name, prefix string, logFn LogFn, fn func(context.Context) error) {
	loop := t.Get(name)
	// Initialize the done channel before marking the loop as running
	// so a caller polling Done() + Running() can't observe Running=true
	// with a nil done channel (which would block forever).
	doneCh := loop.initDone()
	now := time.Now().UnixNano()
	loop.startedAt.CompareAndSwap(0, now)
	loop.lastBeatAt.Store(now)
	loop.running.Store(true)
	defer func() {
		loop.running.Store(false)
		if r := recover(); r != nil {
			loop.panics.Add(1)
			logFn("%spanic recovered: %v\n%s", prefix, r, debug.Stack())
		}
		// Close done last so anything the deferred logFn writes
		// happens-before a receiver on Done(). Channel close is a
		// release fence; without this, waitForExited-style callers
		// can observe Running()==false (atomic) while the panic
		// log is still pending in the goroutine — go test -race
		// flags this as a memory-ordering race even though the
		// log buffer itself is mutex-protected.
		close(doneCh)
	}()
	if err := fn(ctx); err != nil {
		logFn("%sloop returned error: %v", prefix, err)
	}
}
