package service

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/billing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// fakeMeteringProvider records every RecordUsage call and lets the
// test choose whether to return an error.
//
// Thread-safety: calls is an atomic counter so a test can run
// concurrently if needed (current tests don't, but the seam should
// be safe for the drainer's loop body to call from a goroutine).
type fakeMeteringProvider struct {
	name       domain.BillingProvider
	calls      int64
	returnErr  error
	lastCalled billing.MeterUsage
}

func (f *fakeMeteringProvider) Name() domain.BillingProvider { return f.name }
func (f *fakeMeteringProvider) RecordUsage(_ context.Context, in billing.MeterUsage) error {
	atomic.AddInt64(&f.calls, 1)
	f.lastCalled = in
	return f.returnErr
}

// TestMeteringDrainer_zero_rate_short_circuits confirms the
// billing-neutral default: when Rates[kind] is 0 or missing the
// drainer calls MarkProcessed WITHOUT calling the provider. This is
// the fresh-install behavior — operators opt INTO billing by
// setting METERING_RATE_<KIND>.
//
// We drive a full Tick because the rate-check lives inside
// processRow and we'd rather exercise the real code path than a
// hand-rolled stub. The fakeRepo returns one row, the provider
// records the call count — and asserts it stays at 0.
func TestMeteringDrainer_zero_rate_short_circuits(t *testing.T) {
	provider := &fakeMeteringProvider{name: domain.ProviderNoop}
	repo := &fakeRepo{rows: []repository.BillingUsageEventWithID{{
		ID:             1,
		TenantID:       "t_demo",
		Kind:           domain.MeterKindResidentSeconds,
		Quantity:       30,
		IdempotencyKey: "t_demo:resident_seconds:abc",
		RecordedAt:     time.Now(),
	}}}
	d := NewMeteringDrainer(repo, provider, time.Second, 50, 10, map[string]float64{
		// intentionally empty — proves the "entry missing" branch
		// also short-circuits to MarkProcessed.
	})

	d.Tick(context.Background())

	if got := atomic.LoadInt64(&provider.calls); got != 0 {
		t.Fatalf("provider.RecordUsage calls = %d, want 0 (rate card empty)", got)
	}
	if repo.markedProcessed != 1 {
		t.Errorf("MarkProcessed calls = %d, want 1 (zero-rate rows are marked processed)",
			repo.markedProcessed)
	}
}

// TestMeteringDrainer_zero_rate_short_circuits_compute_ms pins the
// zero-rate contract for the fourth billing dimension (issue #555).
// The drainer's rate lookup is shared across all kinds — when
// rates["compute_ms"] is 0 (or the entry is absent, the
// METERING_RATE_COMPUTE_MS=0 default), processRow short-circuits to
// MarkProcessed without touching the provider. Without this test a
// future refactor that filters kinds inside processRow could silently
// start dropping FaaS duration rows.
//
// We drive the explicit "rate=0" branch (not the entry-missing
// branch) to document the operator opt-in: setting
// METERING_RATE_COMPUTE_MS=0 produces the same billing-neutral
// behavior as a fresh install. Idempotency-key shape mirrors what
// worker_test.go's applyTenantDelta fixture produces for the new
// dimension ("<tenant>:compute_ms:<dedupe_id>"), so the test catches
// any drift on either side.
func TestMeteringDrainer_zero_rate_short_circuits_compute_ms(t *testing.T) {
	provider := &fakeMeteringProvider{name: domain.ProviderNoop}
	repo := &fakeRepo{rows: []repository.BillingUsageEventWithID{{
		ID:             7,
		TenantID:       "t_x",
		Kind:           domain.MeterKindComputeMs,
		Quantity:       150,
		IdempotencyKey: "t_x:compute_ms:w:d:1",
		RecordedAt:     time.Unix(1_700_000_000, 0),
	}}}
	d := NewMeteringDrainer(repo, provider, time.Second, 50, 10, map[string]float64{
		// Explicit zero rate for compute_ms — the METERING_RATE_COMPUTE_MS=0
		// default. request_count / resident_seconds / outbound_bytes stay
		// off the rate card so the test confirms the compute_ms entry alone
		// governs dispatch.
		"compute_ms": 0,
	})

	d.Tick(context.Background())

	if got := atomic.LoadInt64(&provider.calls); got != 0 {
		t.Fatalf("provider.RecordUsage calls = %d, want 0 (METERING_RATE_COMPUTE_MS=0 short-circuits)", got)
	}
	if repo.markedProcessed != 1 {
		t.Errorf("MarkProcessed calls = %d, want 1 (zero-rate row marked processed)",
			repo.markedProcessed)
	}
}

// TestMeteringDrainer_calls_provider_when_rate_set is the happy path:
// rate > 0 → RecordUsage is called with the row's fields. We use a
// stub repo to avoid spinning up sqlmock for a single-call test; the
// repo interface is the contract that matters.
func TestMeteringDrainer_calls_provider_when_rate_set(t *testing.T) {
	provider := &fakeMeteringProvider{name: domain.ProviderStripe}
	repo := &fakeRepo{rows: []repository.BillingUsageEventWithID{{
		ID:             42,
		TenantID:       "t_demo",
		Kind:           domain.MeterKindRequestCount,
		Quantity:       7,
		IdempotencyKey: "t_demo:request_count:dedupe",
		RecordedAt:     time.Unix(1_700_000_000, 0),
	}}}
	d := NewMeteringDrainer(repo, provider, time.Second, 50, 10, map[string]float64{
		"request_count": 0.001,
	})

	d.Tick(context.Background())

	if got := atomic.LoadInt64(&provider.calls); got != 1 {
		t.Fatalf("provider.RecordUsage calls = %d, want 1", got)
	}
	if got, want := provider.lastCalled.TenantID, "t_demo"; got != want {
		t.Errorf("TenantID = %q, want %q", got, want)
	}
	if got, want := provider.lastCalled.Kind, domain.MeterKindRequestCount; got != want {
		t.Errorf("Kind = %q, want %q", got, want)
	}
	if got, want := provider.lastCalled.Quantity, uint64(7); got != want {
		t.Errorf("Quantity = %d, want %d", got, want)
	}
	if got, want := provider.lastCalled.IdempotencyKey, "t_demo:request_count:dedupe"; got != want {
		t.Errorf("IdempotencyKey = %q, want %q", got, want)
	}
}

// TestMeteringDrainer_routes_transient_to_retry covers the 5xx case:
// RecordUsage returns a transient error, the drainer logs and
// leaves the row processed_at IS NULL for the next tick. We assert
// by checking that MarkProcessed was NOT called.
func TestMeteringDrainer_routes_transient_to_retry(t *testing.T) {
	provider := &fakeMeteringProvider{
		name:      domain.ProviderStripe,
		returnErr: errTransient, // wraps nothing → transient
	}
	repo := &fakeRepo{rows: []repository.BillingUsageEventWithID{{
		ID:             1,
		TenantID:       "t_demo",
		Kind:           domain.MeterKindRequestCount,
		Quantity:       1,
		IdempotencyKey: "x",
	}}}
	d := NewMeteringDrainer(repo, provider, time.Second, 50, 10, map[string]float64{
		"request_count": 0.001,
	})

	d.Tick(context.Background())

	if got := atomic.LoadInt64(&provider.calls); got != 1 {
		t.Fatalf("provider.RecordUsage calls = %d, want 1", got)
	}
	if repo.markedProcessed != 0 {
		t.Errorf("MarkProcessed called %d times on transient error; want 0 (next-tick retry)",
			repo.markedProcessed)
	}
}

// TestMeteringDrainer_marks_processed_on_terminal covers the 4xx
// case: provider returns a billing.ErrTerminal-wrapped error →
// drainer routes to MarkProcessed so the row stops cycling.
func TestMeteringDrainer_marks_processed_on_terminal(t *testing.T) {
	provider := &fakeMeteringProvider{
		name:      domain.ProviderStripe,
		returnErr: billingTerminalErr,
	}
	repo := &fakeRepo{rows: []repository.BillingUsageEventWithID{{
		ID:             1,
		TenantID:       "t_demo",
		Kind:           domain.MeterKindRequestCount,
		Quantity:       1,
		IdempotencyKey: "x",
	}}}
	d := NewMeteringDrainer(repo, provider, time.Second, 50, 10, map[string]float64{
		"request_count": 0.001,
	})

	d.Tick(context.Background())

	if got := atomic.LoadInt64(&provider.calls); got != 1 {
		t.Fatalf("provider.RecordUsage calls = %d, want 1", got)
	}
	if repo.markedProcessed != 1 {
		t.Errorf("MarkProcessed called %d times on terminal error; want 1 (row stops cycling)",
			repo.markedProcessed)
	}
}

// TestBackoffForMetering_caps_at_5_minutes is the contract test for
// the backoff schedule (kept identical to outbox_drainer.backoffFor
// for dashboard consistency).
func TestBackoffForMetering_caps_at_5_minutes(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 5 * time.Second},
		{1, 10 * time.Second},
		{5, 160 * time.Second},
		{6, 5 * time.Minute}, // 2^6 * 5s = 320s, capped at 300s
		{30, 5 * time.Minute},
	}
	for _, tc := range cases {
		got := backoffForMetering(tc.attempt)
		if got != tc.want {
			t.Errorf("backoffForMetering(%d) = %s, want %s", tc.attempt, got, tc.want)
		}
	}
}

// fakeRepo is a minimal BillingUsageRepository stand-in that returns
// canned rows from ClaimDue and counts MarkProcessed calls. The
// real repo's MarkProcessed / ClaimDue are tested in
// repository/billing_usage_test.go against sqlmock; the drainer
// tests only need to verify the drainer's control flow.
type fakeRepo struct {
	rows            []repository.BillingUsageEventWithID
	markedProcessed int64
}

func (f *fakeRepo) ClaimDue(_ context.Context, _ int) ([]repository.BillingUsageEventWithID, error) {
	// Return the canned rows once, then empty on subsequent calls.
	out := f.rows
	f.rows = nil
	return out, nil
}

func (f *fakeRepo) MarkProcessed(_ context.Context, _ int64) error {
	atomic.AddInt64(&f.markedProcessed, 1)
	return nil
}

// errTransient is a plain error (no terminal wrap) so the drainer's
// errors.Is(err, billing.ErrTerminal) check returns false and the
// transient path runs.
var errTransient = &transientErr{}

type transientErr struct{}

func (*transientErr) Error() string { return "transient stripe 500" }

// billingTerminalErr wraps billing.ErrTerminal so errors.Is finds it.
var billingTerminalErr = wrappedTerminal{}

type wrappedTerminal struct{}

func (wrappedTerminal) Error() string { return "terminal stripe 400" }
func (wrappedTerminal) Unwrap() error { return billing.ErrTerminal }
