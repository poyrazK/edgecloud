//go:build integration
// +build integration

// Package autoscale regression test — issue #85 acceptance criterion:
//
//	"100 RPS spike → new worker requested within 90s."
//
// This file is build-tag-gated so it does NOT run under the default
// `go test ./...` CI job. To run it locally with Docker:
//
//	go test -tags=integration ./internal/autoscale/... -run TestRegression -v
//
// The CI job `go-test-integration` runs it on every PR using a
// docker:dind service so the headline acceptance is exercised
// end-to-end, not just skipped.
//
// The test does not simulate 100 RPS — it constructs the fleet state
// that a 100 RPS spike produces in production: a worker reporting
// `app_slots=0` while DeployRepo.Count returns a high value. With
// TargetHeadroomPct=20 this triggers ComputeDecision's "free slots
// shortage" branch and emits scale_up. The 90s upper bound is the
// acceptance criterion, not a perf target — DecisionIntervalS=1s in
// the test config gives ~90 ticks within that budget.

package autoscale

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	natsio "github.com/nats-io/nats.go"
	tc "github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// shouldSkipRegression mirrors edge-worker/tests/integration_tests.rs:50.
// Returns (reason, true) when the test should t.Skip — Docker unavailable,
// CI without Docker, or explicit SKIP_INTEGRATION_TESTS=1.
//
// CI runs the test with `docker:dind` so /var/run/docker.sock is present
// and the guard does not fire; this is purely for local-dev convenience.
func shouldSkipRegression() (string, bool) {
	if _, ok := os.LookupEnv("SKIP_INTEGRATION_TESTS"); ok {
		return "SKIP_INTEGRATION_TESTS set", true
	}
	if _, err := os.Stat("/var/run/docker.sock"); err != nil {
		return "/var/run/docker.sock not present (set SKIP_INTEGRATION_TESTS=1 to skip)", true
	}
	return "", false
}

// newTestNATS boots a NATS testcontainer and returns a connected
// *natsio.Conn. The container is terminated via t.Cleanup so each
// test gets its own server — no shared state between tests.
//
// We use the lightweight nats:2.10-alpine image. No JetStream
// required — the autoscaler subscribes via plain ChanSubscribe, not
// js.Subscribe, so a basic NATS server is enough.
func newTestNATS(t *testing.T) *natsio.Conn {
	t.Helper()
	ctx := context.Background()
	natsC, err := tcnats.RunContainer(ctx,
		tc.WithImage("nats:2.10-alpine"),
	)
	if err != nil {
		t.Fatalf("start nats testcontainer: %v", err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = natsC.Terminate(cctx)
	})

	url, err := natsC.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("nats connection string: %v", err)
	}
	nc, err := natsio.Connect(url,
		natsio.Name("autoscale-regression-test"),
		natsio.RetryOnFailedConnect(true),
		natsio.MaxReconnects(-1),
		natsio.ReconnectWait(250*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc
}

// newServiceForRegression builds a Service wired for regression tests.
// DecisionIntervalS=1s so the 90s budget has plenty of ticks.
// ScaleUpCooldownS=5s so the cooldown test's 2.5s observation window
// sits comfortably inside the cooldown — without this margin, the
// cooldown can expire mid-observation and the second tick would
// (correctly) re-fire Provision. 5s is the smallest value that
// keeps the test stable across CI runners that schedule ticks
// slightly late.
//
// The optional `overrides` Config is shallow-merged into the defaults
// (only non-zero fields replace). Same pattern works for scale-down
// tests that need a shorter ScaleDownCooldownS.
//
// Returns the service, an event recorder (so the test can assert on what
// was inserted), and a MockCloudProvider whose Provision counter
// (cloud.ProvisionCalls()) and Deprovision counter (cloud.DeprovisionCalls())
// are bumped automatically by the mock — no caller-side accounting needed.
func newServiceForRegression(t *testing.T, nc *natsio.Conn, overrides ...Config) (
	*Service, *mockEventRepo, *MockCloudProvider,
) {
	t.Helper()
	events := &mockEventRepo{}
	cloud := &MockCloudProvider{
		KindFunc:      func() string { return "mock" },
		ProvisionFunc: func(_ context.Context, region string) (string, error) { return "w_" + region + "_new", nil },
	}
	cfg := Config{
		Enabled:            true,
		MinWorkers:         1,
		MaxWorkers:         10,
		TargetHeadroomPct:  20,
		ScaleUpCooldownS:   5,
		ScaleDownCooldownS: 60,
		DecisionIntervalS:  1,
	}
	if len(overrides) > 0 {
		o := overrides[0]
		if o.MinWorkers != 0 {
			cfg.MinWorkers = o.MinWorkers
		}
		if o.MaxWorkers != 0 {
			cfg.MaxWorkers = o.MaxWorkers
		}
		if o.TargetHeadroomPct != 0 {
			cfg.TargetHeadroomPct = o.TargetHeadroomPct
		}
		if o.ScaleUpCooldownS != 0 {
			cfg.ScaleUpCooldownS = o.ScaleUpCooldownS
		}
		if o.ScaleDownCooldownS != 0 {
			cfg.ScaleDownCooldownS = o.ScaleDownCooldownS
		}
		if o.DecisionIntervalS != 0 {
			cfg.DecisionIntervalS = o.DecisionIntervalS
		}
	}
	s := NewService(Deps{
		Cfg:        cfg,
		NC:         nc,
		DeployRepo: &mockDeployRepo{},
		EventRepo:  events,
		Cloud:      cloud,
		Log:        discardLogger(),
	})
	return s, events, cloud
}

// publishHeartbeat sends a synthetic HeartbeatMessage to
// edgecloud.heartbeats.<region>. The shape mirrors the Rust
// HeartbeatMessage struct (edge-worker/src/messages.rs) and the Go
// nats.HeartbeatMessage — autoscaler only reads worker_id, region,
// and cluster_headroom.app_slots, but we send the full shape so the
// test fails loudly if a future change tightens parsing.
func publishHeartbeat(t *testing.T, nc *natsio.Conn, region string, appSlots uint32) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"type":      "heartbeat",
		"worker_id": "w_" + region + "_test",
		"region":    region,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"apps":      map[string]any{},
		"cluster_headroom": map[string]any{
			"app_slots": appSlots,
		},
	})
	if err != nil {
		t.Fatalf("marshal heartbeat: %v", err)
	}
	if err := nc.Publish("edgecloud.heartbeats."+region, body); err != nil {
		t.Fatalf("publish heartbeat: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("nats flush: %v", err)
	}
}

// waitForProvision polls until at least one Provision call has been
// recorded on `cloud`, or `deadline` elapses. Uses a 50ms poll
// interval so the test reacts within ~one tick (DecisionIntervalS=1s
// in test config).
//
// We poll because the autoscaler fires Provision asynchronously on
// the decision goroutine, and the test cannot observe it without
// yielding to the runtime.
func waitForProvision(t *testing.T, cloud *MockCloudProvider, deadline time.Duration) {
	t.Helper()
	timeout := time.NewTimer(deadline)
	defer timeout.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-timeout.C:
			t.Fatalf("waitForProvision: no Provision call within %v", deadline)
		case <-tick.C:
			if cloud.ProvisionCalls() >= 1 {
				return
			}
		}
	}
}

// waitForDeprovision polls until at least `n` Deprovision calls
// have been recorded on `cloud`, or `deadline` elapses. Mirrors
// waitForProvision but for the Deprovision counter.
func waitForDeprovision(t *testing.T, cloud *MockCloudProvider, n int64, deadline time.Duration) {
	t.Helper()
	timeout := time.NewTimer(deadline)
	defer timeout.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-timeout.C:
			t.Fatalf("waitForDeprovision(%d): only %d calls within %v", n, cloud.DeprovisionCalls(), deadline)
		case <-tick.C:
			if cloud.DeprovisionCalls() >= n {
				return
			}
		}
	}
}

// publishHeartbeatCustom sends a heartbeat with a custom worker ID.
// Needed for multi-worker scale-down tests where each heartbeat must
// come from a distinct worker to build the fleet view.
func publishHeartbeatCustom(t *testing.T, nc *natsio.Conn, workerID, region string, appSlots uint32) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"type":      "heartbeat",
		"worker_id": workerID,
		"region":    region,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"apps":      map[string]any{},
		"cluster_headroom": map[string]any{
			"app_slots": appSlots,
		},
	})
	if err != nil {
		t.Fatalf("marshal heartbeat: %v", err)
	}
	if err := nc.Publish("edgecloud.heartbeats."+region, body); err != nil {
		t.Fatalf("publish heartbeat: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("nats flush: %v", err)
	}
}

// runSubscribeWithCleanup wires the autoscaler's Subscribe goroutine
// with a sync.WaitGroup so the test can wait for clean exit. Service.Subscribe
// (service.go:116) owns its own goroutine and we can't intercept it,
// so we manually re-create what it does: ChanSubscribe + spawn run()
// with the WaitGroup tracked.
//
// t.Cleanup runs LIFO, so:
//
//	nc.Close() (registered in newTestNATS, first)
//	cancel(); wg.Wait() (registered here, second — runs FIRST)
//	natsC.Terminate() (registered in newTestNATS, third — runs LAST)
//
// That ordering ensures the goroutine exits before the NATS connection
// it reads from closes.
func runSubscribeWithCleanup(t *testing.T, s *Service, nc *natsio.Conn, ctx context.Context) {
	t.Helper()
	ch := make(chan *natsio.Msg, 100)
	sub, err := nc.ChanSubscribe("edgecloud.heartbeats.>", ch)
	if err != nil {
		t.Fatalf("ChanSubscribe: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.run(ctx, sub, ch)
	}()
	t.Cleanup(func() {
		// ctx is canceled by the test's defer cancel() before cleanup
		// runs, so s.run will already have exited via ctx.Done() and
		// closed sub.Unsubscribe() in its own defer. wg.Wait() just
		// synchronizes — no drain goroutine needed.
		wg.Wait()
	})
}

// TestRegression_ScaleUpFiresUnderSpike is the headline acceptance
// criterion from issue #85: under a spike (high desiredApps + zero
// free slots), the autoscaler must request a new worker within 90s.
//
// The test simulates the spike by publishing a single heartbeat
// reporting app_slots=0 and configuring DeployRepo.Count to return
// 100. With TargetHeadroomPct=20, ComputeDecision computes:
//
//	needed = 100 + 20 = 120  >  totalFreeSlots=0  →  scale_up (1 → 2)
func TestRegression_ScaleUpFiresUnderSpike(t *testing.T) {
	if reason, ok := shouldSkipRegression(); ok {
		t.Skipf("regression: %s", reason)
	}

	nc := newTestNATS(t)
	s, events, cloud := newServiceForRegression(t, nc)
	// Wire Count to return 100 (the "spike aftermath"): the autoscaler
	// will treat this as DesiredApps=100 and demand more workers.
	s.deployRepo = &mockDeployRepo{
		countFunc: func(_ context.Context) (int, error) { return 100, nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runSubscribeWithCleanup(t, s, nc, ctx)

	// 1 worker reporting 0 free slots — this is the spike's footprint.
	publishHeartbeat(t, nc, "fra", 0)

	// Wait ≤ 90s for cloud.Provision to fire. The autoscaler ticks
	// at 1s and reacts within one tick of the heartbeat landing.
	waitForProvision(t, cloud, 90*time.Second)

	// Give the Insert a moment to land after Provision returns.
	time.Sleep(100 * time.Millisecond)
	if len(events.events) < 1 {
		t.Fatalf("Provision called but no event recorded")
	}
	ev := events.events[0]
	if ev.Action != domain.AutoscaleUp {
		t.Errorf("event.Action = %q, want scale_up", ev.Action)
	}
	if !ev.Succeeded {
		t.Errorf("event.Succeeded = false, want true")
	}
	if ev.Region != "fra" {
		t.Errorf("event.Region = %q, want fra", ev.Region)
	}
	if ev.FromCount != 1 || ev.ToCount != 2 {
		t.Errorf("event.FromCount=%d ToCount=%d, want 1→2", ev.FromCount, ev.ToCount)
	}
}

// TestRegression_CooldownSuppressesSecondScaleUp pins the cooldown
// contract: after a successful scale_up, a second tick within
// ScaleUpCooldownS must convert to a noop event and NOT call Provision
// again. With ScaleUpCooldownS=5s in test config, the cooldown window
// comfortably covers the 2.5s observation period so a CI runner
// scheduling ticks slightly late still observes suppression.
func TestRegression_CooldownSuppressesSecondScaleUp(t *testing.T) {
	if reason, ok := shouldSkipRegression(); ok {
		t.Skipf("regression: %s", reason)
	}

	nc := newTestNATS(t)
	s, events, cloud := newServiceForRegression(t, nc)
	s.deployRepo = &mockDeployRepo{
		countFunc: func(_ context.Context) (int, error) { return 100, nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runSubscribeWithCleanup(t, s, nc, ctx)

	// Trigger the first scale_up.
	publishHeartbeat(t, nc, "fra", 0)
	waitForProvision(t, cloud, 90*time.Second)
	firstCount := cloud.ProvisionCalls()

	// Burst more heartbeats well within the cooldown window. The
	// next tick (within ~1s) must convert the resulting scale_up
	// decision into a noop event and NOT call Provision again.
	for i := 0; i < 5; i++ {
		publishHeartbeat(t, nc, "fra", 0)
		time.Sleep(150 * time.Millisecond)
	}

	// Observe for 2.5s — long enough for ~2 decision ticks to fire
	// after the first scale_up. With ScaleUpCooldownS=5s, all of
	// those ticks must be noops and Provision must NOT fire again.
	time.Sleep(2500 * time.Millisecond)

	secondCount := cloud.ProvisionCalls()
	if secondCount > firstCount {
		t.Errorf("Provision called again after first scale_up: first=%d second=%d (cooldown should suppress)", firstCount, secondCount)
	}

	// At least one cooldown-suppressed noop event must have been recorded.
	foundNoop := false
	for _, ev := range events.events {
		if ev.Action == domain.AutoscaleNoop {
			foundNoop = true
			break
		}
	}
	if !foundNoop {
		t.Errorf("no noop event recorded after cooldown-suppressed scale_up; events=%d", len(events.events))
	}
}

// TestRegression_MultiRegionIndependent pins per-region fan-out:
// heartbeats in two regions must each produce their own Provision
// call. The autoscaler fans out per-region in evaluateAll
// (service.go:175) — this test pins that wiring.
func TestRegression_MultiRegionIndependent(t *testing.T) {
	if reason, ok := shouldSkipRegression(); ok {
		t.Skipf("regression: %s", reason)
	}

	nc := newTestNATS(t)
	s, _, cloud := newServiceForRegression(t, nc)
	s.deployRepo = &mockDeployRepo{
		countFunc: func(_ context.Context) (int, error) { return 100, nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runSubscribeWithCleanup(t, s, nc, ctx)

	publishHeartbeat(t, nc, "fra", 0)
	publishHeartbeat(t, nc, "iad", 0)

	// Wait for both regions to Provision. The 90s budget is generous
	// (each tick fires at 1s). We poll until ≥ 2 calls land.
	deadline := time.After(90 * time.Second)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("both regions did not Provision within 90s; calls=%d", cloud.ProvisionCalls())
		case <-tick.C:
			if cloud.ProvisionCalls() >= 2 {
				return
			}
		}
	}
}

// TestRegression_ScaleDownFiresOnExcess exercises the Deprovision
// path end-to-end: 3 workers in region fra each reporting 500 free
// slots while desiredApps=1. With TargetHeadroomPct=20, needed=1,
// free=1500 > 2×1 → scale_down by 1.
//
// This is the only test that proves the full Deprovision pipeline
// works (pickVictim → CloudProvider.Deprovision → event recorded).
func TestRegression_ScaleDownFiresOnExcess(t *testing.T) {
	if reason, ok := shouldSkipRegression(); ok {
		t.Skipf("regression: %s", reason)
	}

	nc := newTestNATS(t)
	// ScaleDownCooldownS=5 so we don't wait 60s to observe the second
	// tick, but the default 60s would still work for the first fire.
	s, events, cloud := newServiceForRegression(t, nc, Config{
		ScaleDownCooldownS: 5,
	})
	s.deployRepo = &mockDeployRepo{
		// Count=1 so needed=1 (1 + 20% = 1.2 → 1, floor at 1).
		// 3 workers × 500 free slots = 1500, far above 2×1 → scale_down.
		countFunc: func(_ context.Context) (int, error) { return 1, nil },
	}
	// Wire Deprovision so it succeeds and increments the counter.
	cloud.DeprovisionFunc = func(_ context.Context, _, _ string) error { return nil }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runSubscribeWithCleanup(t, s, nc, ctx)

	// 3 workers, all with massive excess capacity.
	for i := 0; i < 3; i++ {
		publishHeartbeatCustom(t, nc, fmt.Sprintf("w_fra_%d", i), "fra", 500)
	}

	// Wait for the first Deprovision to fire.
	waitForDeprovision(t, cloud, 1, 90*time.Second)

	// Give the Insert a moment to land.
	time.Sleep(100 * time.Millisecond)

	if len(events.events) < 1 {
		t.Fatal("Provision called but no event recorded")
	}
	ev := events.events[0]
	if ev.Action != domain.AutoscaleDown {
		t.Errorf("event.Action = %q, want scale_down", ev.Action)
	}
	if ev.Region != "fra" {
		t.Errorf("event.Region = %q, want fra", ev.Region)
	}
	if ev.FromCount != 3 || ev.ToCount != 2 {
		t.Errorf("event.FromCount=%d ToCount=%d, want 3→2", ev.FromCount, ev.ToCount)
	}
	if !ev.Succeeded {
		t.Errorf("event.Succeeded = false, want true")
	}
}

// TestRegression_ScaleDownCooldownSuppressesSecond pins the
// scale-down cooldown contract: after a successful Deprovision, a
// subsequent tick within ScaleDownCooldownS must suppress the next
// Deprovision and record a cooldown-noop event instead.
func TestRegression_ScaleDownCooldownSuppressesSecond(t *testing.T) {
	if reason, ok := shouldSkipRegression(); ok {
		t.Skipf("regression: %s", reason)
	}

	nc := newTestNATS(t)
	// Short cooldown so the observation window fits inside it.
	// ScaleDownCooldownS=2s means the test waits ~2.5s after the
	// first Deprovision and must observe NO second Deprovision.
	s, events, cloud := newServiceForRegression(t, nc, Config{
		ScaleUpCooldownS:   5,
		ScaleDownCooldownS: 2,
	})
	s.deployRepo = &mockDeployRepo{
		countFunc: func(_ context.Context) (int, error) { return 1, nil },
	}
	cloud.DeprovisionFunc = func(_ context.Context, _, _ string) error { return nil }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runSubscribeWithCleanup(t, s, nc, ctx)

	// 3 workers with excess → triggers first scale_down.
	for i := 0; i < 3; i++ {
		publishHeartbeatCustom(t, nc, fmt.Sprintf("w_fra_%d", i), "fra", 500)
	}
	waitForDeprovision(t, cloud, 1, 90*time.Second)
	firstCount := cloud.DeprovisionCalls()

	// Keep heartbeats coming (still 3 workers worth, but one has been
	// deprovisioned — the fleet view still sees 3 until the heartbeat
	// service removes it, which doesn't happen because we keep publishing
	// all 3). The key: excess still exists.
	for i := 0; i < 5; i++ {
		for j := 0; j < 3; j++ {
			publishHeartbeatCustom(t, nc, fmt.Sprintf("w_fra_%d", j), "fra", 500)
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Observe for 1.5s — with ScaleDownCooldownS=2s, this sits
	// comfortably inside the cooldown window and must NOT fire
	// another Deprovision.
	time.Sleep(1500 * time.Millisecond)

	secondCount := cloud.DeprovisionCalls()
	if secondCount > firstCount {
		t.Errorf("Deprovision called again: first=%d second=%d (cooldown should suppress)", firstCount, secondCount)
	}

	// At least one cooldown-suppressed noop event must exist.
	foundNoop := false
	for _, ev := range events.events {
		if ev.Action == domain.AutoscaleNoop {
			foundNoop = true
			break
		}
	}
	if !foundNoop {
		t.Errorf("no noop event recorded after cooldown-suppressed scale_down; events=%d", len(events.events))
	}
}

// errSkipped removed: the t.Skip / t.Skipf calls themselves gate the
// tests; a sentinel adds noise without value.
