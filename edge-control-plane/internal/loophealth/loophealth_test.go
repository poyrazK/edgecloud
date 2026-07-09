package loophealth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// discardLog returns a LogFn that captures formatted output to a buffer
// and a closure to read what was captured. Tests use this to assert that
// the wrapper formatted messages with the configured prefix.
func discardLog() (LogFn, func() string) {
	var mu sync.Mutex
	var buf bytes.Buffer
	fn := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		fmt.Fprintf(&buf, format, args...)
		buf.WriteByte('\n')
	}
	return fn, func() string {
		mu.Lock()
		defer mu.Unlock()
		return buf.String()
	}
}

func TestTracker_GetLazyCreates(t *testing.T) {
	tr := NewTracker()
	l := tr.Get("heartbeat")
	if l == nil {
		t.Fatal("Get returned nil")
	}
	if l.Name() != "heartbeat" {
		t.Errorf("Name = %q, want %q", l.Name(), "heartbeat")
	}
	if l.Panics() != 0 || l.Running() {
		t.Errorf("fresh loop: panics=%d running=%v, want 0/false", l.Panics(), l.Running())
	}
	if !l.StartedAt().IsZero() || !l.LastBeatAt().IsZero() {
		t.Errorf("fresh loop has non-zero timestamps: started=%v beat=%v", l.StartedAt(), l.LastBeatAt())
	}
	// Second Get returns the same instance.
	if got := tr.Get("heartbeat"); got != l {
		t.Errorf("Get returned a different instance on second call")
	}
}

func TestRun_BumpsRunningAndPanics(t *testing.T) {
	tr := NewTracker()
	logFn, _ := discardLog()
	done := make(chan struct{})
	tr.Run(context.Background(), "heartbeat", "heartbeat: ", logFn, func(ctx context.Context) {
		close(done)
		// Inside the body, running should be true.
		if !tr.Get("heartbeat").Running() {
			t.Errorf("expected running=true inside body")
		}
		panic("forced")
	})
	<-done
	// After return, running=false and panics==1.
	l := tr.Get("heartbeat")
	if l.Running() {
		t.Errorf("expected running=false after return")
	}
	if l.Panics() != 1 {
		t.Errorf("Panics = %d, want 1", l.Panics())
	}
	if l.StartedAt().IsZero() {
		t.Errorf("StartedAt is zero after first entry")
	}
	if l.LastBeatAt().IsZero() {
		t.Errorf("LastBeatAt is zero after first entry")
	}
}

func TestRun_NoPanic_KeepsRunningFalse(t *testing.T) {
	tr := NewTracker()
	logFn, read := discardLog()
	tr.Run(context.Background(), "log_gc", "log_gc: ", logFn, func(ctx context.Context) {
		// No panic, no error.
	})
	l := tr.Get("log_gc")
	if l.Running() {
		t.Errorf("expected running=false after return")
	}
	if l.Panics() != 0 {
		t.Errorf("Panics = %d, want 0", l.Panics())
	}
	if l.StartedAt().IsZero() {
		t.Errorf("StartedAt is zero after entry")
	}
	if read() != "" {
		t.Errorf("expected no log output on clean exit, got %q", read())
	}
}

func TestRun_LogsViaPrefix(t *testing.T) {
	tr := NewTracker()
	// Capture stdlib log output too — Run/RunErr use the LogFn we pass,
	// so the prefix is applied through the LogFn (which appends '\n').
	logFn, read := discardLog()
	tr.Run(context.Background(), "log_gc", "log_gc: ", logFn, func(ctx context.Context) {
		panic("forced")
	})
	got := read()
	if !strings.Contains(got, "log_gc: ") {
		t.Errorf("captured log missing prefix 'log_gc: ': %q", got)
	}
	if !strings.Contains(got, "panic recovered") {
		t.Errorf("captured log missing 'panic recovered': %q", got)
	}
	if !strings.Contains(got, "forced") {
		t.Errorf("captured log missing panic value 'forced': %q", got)
	}
	// Stack trace should mention this test function.
	if !strings.Contains(got, "loophealth_test.go") {
		t.Errorf("captured log missing stack trace from this file: %q", got)
	}
}

func TestRunErr_RecoversAndSwallowsError(t *testing.T) {
	tr := NewTracker()
	logFn, read := discardLog()
	sentinel := errors.New("subscription failed")
	tr.RunErr(context.Background(), "heartbeat", "heartbeat: ", logFn, func(ctx context.Context) error {
		panic("inner panic")
	})
	// RunErr with a panicking fn: panic should be recovered and counted.
	if tr.Get("heartbeat").Panics() != 1 {
		t.Errorf("Panics = %d, want 1", tr.Get("heartbeat").Panics())
	}
	if !strings.Contains(read(), "panic recovered") {
		t.Errorf("expected 'panic recovered' in log, got %q", read())
	}

	// Now an error-returning fn: the wrapper should log it and exit cleanly.
	read2, _ := discardLog()
	tr.RunErr(context.Background(), "heartbeat2", "heartbeat: ", read2, func(ctx context.Context) error {
		return sentinel
	})
	if tr.Get("heartbeat2").Panics() != 0 {
		t.Errorf("Panics = %d, want 0 (error return is not a panic)", tr.Get("heartbeat2").Panics())
	}
	if tr.Get("heartbeat2").Running() {
		t.Errorf("expected running=false after fn returns")
	}
}

func TestRunErr_WithLogFn_ForwardsFormatting(t *testing.T) {
	tr := NewTracker()
	logFn, read := discardLog()
	tr.RunErrWithLog(context.Background(), "autoscale", "autoscale: ", logFn, func(ctx context.Context) error {
		panic("kaboom")
	})
	got := read()
	if !strings.Contains(got, "autoscale: ") {
		t.Errorf("expected 'autoscale: ' prefix, got %q", got)
	}
	if !strings.Contains(got, "kaboom") {
		t.Errorf("expected panic value in log, got %q", got)
	}
}

func TestSnapshot_SortedByName(t *testing.T) {
	tr := NewTracker()
	tr.Get("zebra")
	tr.Get("alpha")
	tr.Get("mango")
	snap := tr.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("Snapshot len = %d, want 3", len(snap))
	}
	want := []string{"alpha", "mango", "zebra"}
	for i, w := range want {
		if snap[i].Name != w {
			t.Errorf("snap[%d].Name = %q, want %q", i, snap[i].Name, w)
		}
	}
}

func TestSnapshot_StaleComputed(t *testing.T) {
	tr := NewTracker()
	tr.SetStaleAfter(50 * time.Millisecond)
	logFn, _ := discardLog()
	tr.Run(context.Background(), "log_gc", "log_gc: ", logFn, func(ctx context.Context) {
		// entry bumps lastBeatAt to now
	})
	l := tr.Get("log_gc")
	// Immediately after return, snapshot should NOT be stale.
	for _, s := range tr.Snapshot() {
		if s.Name == "log_gc" && s.Stale {
			t.Errorf("expected stale=false immediately after exit, got true")
		}
	}
	// Manually backdate lastBeatAt to 1 hour ago via Beat + time warp.
	// We can't directly write the atomic, but we can sleep past the
	// threshold and verify the next snapshot flips stale.
	time.Sleep(80 * time.Millisecond)
	for _, s := range tr.Snapshot() {
		if s.Name == "log_gc" && !s.Stale {
			t.Errorf("expected stale=true after %v, got false (lastBeatAt=%s now=%s)",
				80*time.Millisecond, s.LastBeatAt, time.Now().UTC().Format(time.RFC3339))
		}
	}
	_ = l
}

func TestSnapshot_RunningFalseAfterExit(t *testing.T) {
	tr := NewTracker()
	logFn, _ := discardLog()
	tr.Run(context.Background(), "log_gc", "log_gc: ", logFn, func(ctx context.Context) {})
	for _, s := range tr.Snapshot() {
		if s.Name == "log_gc" && s.Running {
			t.Errorf("expected running=false in snapshot after exit")
		}
	}
}

func TestState_RFC3339(t *testing.T) {
	tr := NewTracker()
	logFn, _ := discardLog()
	tr.Run(context.Background(), "log_gc", "log_gc: ", logFn, func(ctx context.Context) {
		tr.Get("log_gc").Beat()
	})
	for _, s := range tr.Snapshot() {
		if s.Name != "log_gc" {
			continue
		}
		if s.StartedAt != "" {
			if _, err := time.Parse(time.RFC3339, s.StartedAt); err != nil {
				t.Errorf("StartedAt not RFC3339: %q (%v)", s.StartedAt, err)
			}
		}
		if s.LastBeatAt != "" {
			if _, err := time.Parse(time.RFC3339, s.LastBeatAt); err != nil {
				t.Errorf("LastBeatAt not RFC3339: %q (%v)", s.LastBeatAt, err)
			}
		}
		// Round-trip JSON marshalling to ensure no fields break encoding.
		b, err := json.Marshal(s)
		if err != nil {
			t.Fatalf("json.Marshal: %v", err)
		}
		// Name should appear in the marshalled output.
		if !regexp.MustCompile(`"name":"log_gc"`).Match(b) {
			t.Errorf("marshalled State missing name=log_gc: %s", b)
		}
	}
}

func TestBeat_BumpsLastBeatAt(t *testing.T) {
	tr := NewTracker()
	l := tr.Get("foo")
	if !l.LastBeatAt().IsZero() {
		t.Fatalf("LastBeatAt already non-zero: %v", l.LastBeatAt())
	}
	before := time.Now()
	l.Beat()
	after := time.Now()
	ts := l.LastBeatAt()
	if ts.Before(before.Truncate(time.Millisecond)) || ts.After(after.Add(time.Millisecond)) {
		t.Errorf("Beat timestamp %v not between %v and %v", ts, before, after)
	}
}

func TestNewTracker_DefaultStaleAfter(t *testing.T) {
	tr := NewTracker()
	if got := tr.StaleAfter(); got != DefaultStaleAfter {
		t.Errorf("StaleAfter = %v, want %v", got, DefaultStaleAfter)
	}
	tr.SetStaleAfter(0)
	if got := tr.StaleAfter(); got != DefaultStaleAfter {
		t.Errorf("SetStaleAfter(0) reset to %v, want %v", got, DefaultStaleAfter)
	}
	tr.SetStaleAfter(7 * time.Second)
	if got := tr.StaleAfter(); got != 7*time.Second {
		t.Errorf("SetStaleAfter(7s) = %v, want 7s", got)
	}
}

// TestRun_DoesNotInterfereWithStdlibLog verifies that the wrapper's LogFn
// is invoked rather than the stdlib log package directly. We swap
// log.Default() output to a buffer and assert nothing is written there
// when only LogFn is used.
func TestRun_DoesNotInterfereWithStdlibLog(t *testing.T) {
	var buf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(orig)

	tr := NewTracker()
	logFn, _ := discardLog()
	tr.Run(context.Background(), "log_gc", "log_gc: ", logFn, func(ctx context.Context) {
		panic("forced")
	})
	if buf.Len() != 0 {
		t.Errorf("stdlib log captured output (expected none): %q", buf.String())
	}
}
