package autoscale

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	natsio "github.com/nats-io/nats.go"
)

// mockDeployRepo records Count calls and returns a configured value.
type mockDeployRepo struct {
	countFunc func(ctx context.Context) (int, error)
}

func (m *mockDeployRepo) Count(ctx context.Context) (int, error) {
	if m.countFunc == nil {
		return 0, nil
	}
	return m.countFunc(ctx)
}

// mockEventRepo records Insert calls so tests can assert the wire
// shape of recorded events. Default returns id=1, nil.
type mockEventRepo struct {
	mu     sync.Mutex
	events []*domain.AutoscaleEvent
}

func (m *mockEventRepo) Insert(_ context.Context, e *domain.AutoscaleEvent) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
	return int64(len(m.events)), nil
}

// discardLogger returns a *slog.Logger that drops everything — tests
// don't assert on log output, and the default slog writes to stderr.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestHandleHeartbeat_PopulatesFleet is the simplest wiring pin:
// a single heartbeat JSON must populate the per-region fleet view.
func TestHandleHeartbeat_PopulatesFleet(t *testing.T) {
	s := NewService(Deps{
		Cfg:        Config{Enabled: true, MinWorkers: 1, MaxWorkers: 10, DecisionIntervalS: 30},
		DeployRepo: &mockDeployRepo{},
		EventRepo:  &mockEventRepo{},
		Cloud:      &MockCloudProvider{},
		Log:        discardLogger(),
	})

	body, _ := json.Marshal(map[string]any{
		"type":      "heartbeat",
		"worker_id": "w_fra_abc",
		"region":    "fra",
		"timestamp": time.Now().Format(time.RFC3339),
		"apps":      map[string]any{},
		"cluster_headroom": map[string]any{
			"app_slots": 42,
		},
	})
	msg := natsMsg(body)
	s.handleHeartbeat(msg)

	fleet := s.SnapshotFleet("fra")
	if len(fleet) != 1 {
		t.Fatalf("len(fleet) = %d, want 1", len(fleet))
	}
	if fleet[0].WorkerID != "w_fra_abc" {
		t.Errorf("WorkerID = %q, want w_fra_abc", fleet[0].WorkerID)
	}
	if fleet[0].Headroom == nil {
		t.Fatalf("Headroom = nil, want populated")
	}
	if fleet[0].Headroom.AppSlots != 42 {
		t.Errorf("AppSlots = %d, want 42", fleet[0].Headroom.AppSlots)
	}
}

// TestHandleHeartbeat_LegacyNoHeadroom pins backward-compat: a
// pre-#85 worker that doesn't send cluster_headroom should still
// appear in the fleet view, just with Headroom=nil (which the
// decision function handles via legacyAssumedAppSlots).
func TestHandleHeartbeat_LegacyNoHeadroom(t *testing.T) {
	s := NewService(Deps{
		Cfg:        Config{Enabled: true, MinWorkers: 1, MaxWorkers: 10, DecisionIntervalS: 30},
		DeployRepo: &mockDeployRepo{},
		EventRepo:  &mockEventRepo{},
		Cloud:      &MockCloudProvider{},
		Log:        discardLogger(),
	})

	body, _ := json.Marshal(map[string]any{
		"worker_id": "w_legacy_1",
		"region":    "fra",
	})
	msg := natsMsg(body)
	s.handleHeartbeat(msg)

	fleet := s.SnapshotFleet("fra")
	if len(fleet) != 1 {
		t.Fatalf("len(fleet) = %d, want 1", len(fleet))
	}
	if fleet[0].Headroom != nil {
		t.Errorf("Headroom = %+v, want nil for legacy worker", fleet[0].Headroom)
	}
}

// TestHandleHeartbeat_MalformedSkipped pins the "don't crash on
// garbage" invariant — a malformed JSON message must NOT poison the
// fleet view (no panic, no worker added).
func TestHandleHeartbeat_MalformedSkipped(t *testing.T) {
	s := NewService(Deps{
		Cfg:        Config{Enabled: true, MinWorkers: 1, MaxWorkers: 10, DecisionIntervalS: 30},
		DeployRepo: &mockDeployRepo{},
		EventRepo:  &mockEventRepo{},
		Cloud:      &MockCloudProvider{},
		Log:        discardLogger(),
	})

	s.handleHeartbeat(natsMsg([]byte("not json")))
	if fleet := s.SnapshotFleet("fra"); fleet != nil {
		t.Errorf("fleet = %+v, want nil after malformed heartbeat", fleet)
	}
}

// TestApplyCooldown_NoHistoryReturnsDecision pins the trivial case:
// a nil lastEvent means no cooldown has been recorded yet, so any
// Decision is passed through unchanged.
func TestApplyCooldown_NoHistoryReturnsDecision(t *testing.T) {
	s := NewService(Deps{Cfg: Config{ScaleUpCooldownS: 60, ScaleDownCooldownS: 300}, Cloud: &MockCloudProvider{}, Log: discardLogger()})
	d := Decision{Action: domain.AutoscaleUp, FromCount: 0, ToCount: 1, Reason: "below min"}
	got := s.applyCooldown(d, nil, time.Now())
	if got.Action != domain.AutoscaleUp {
		t.Errorf("Action = %q, want scale_up", got.Action)
	}
}

// TestApplyCooldown_SuppressesWithinWindow pins the core gate: a
// scale_up fired 10s after the last scale_up with a 60s cooldown
// must be suppressed and converted to noop.
func TestApplyCooldown_SuppressesWithinWindow(t *testing.T) {
	s := NewService(Deps{Cfg: Config{ScaleUpCooldownS: 60, ScaleDownCooldownS: 300}, Cloud: &MockCloudProvider{}, Log: discardLogger()})

	last := &domain.AutoscaleEvent{
		Action:    domain.AutoscaleUp,
		CreatedAt: time.Now().Add(-10 * time.Second),
	}
	d := Decision{Action: domain.AutoscaleUp, FromCount: 1, ToCount: 2, Reason: "free_slots=0"}
	got := s.applyCooldown(d, last, time.Now())
	if got.Action != domain.AutoscaleNoop {
		t.Fatalf("Action = %q, want noop (cooldown)", got.Action)
	}
	if !strings.Contains(got.Reason, "scale_up cooldown") {
		t.Errorf("Reason = %q, want substring 'scale_up cooldown'", got.Reason)
	}
}

// TestApplyCooldown_AllowsAfterWindow pins the inverse: a scale_up
// fired 70s after the last scale_up with a 60s cooldown must be
// allowed through.
func TestApplyCooldown_AllowsAfterWindow(t *testing.T) {
	s := NewService(Deps{Cfg: Config{ScaleUpCooldownS: 60, ScaleDownCooldownS: 300}, Cloud: &MockCloudProvider{}, Log: discardLogger()})

	last := &domain.AutoscaleEvent{
		Action:    domain.AutoscaleUp,
		CreatedAt: time.Now().Add(-70 * time.Second),
	}
	d := Decision{Action: domain.AutoscaleUp, FromCount: 1, ToCount: 2, Reason: "free_slots=0"}
	got := s.applyCooldown(d, last, time.Now())
	if got.Action != domain.AutoscaleUp {
		t.Errorf("Action = %q, want scale_up (past cooldown)", got.Action)
	}
}

// TestApplyCooldown_CrossActionIndependent pins that cooldowns are
// per-action-class: a scale_down right after a scale_up is NOT
// suppressed. Critical for the issue #85 spec which calls out
// independent scale-up vs scale-down cooldown semantics.
func TestApplyCooldown_CrossActionIndependent(t *testing.T) {
	s := NewService(Deps{Cfg: Config{ScaleUpCooldownS: 60, ScaleDownCooldownS: 300}, Cloud: &MockCloudProvider{}, Log: discardLogger()})

	last := &domain.AutoscaleEvent{
		Action:    domain.AutoscaleUp,
		CreatedAt: time.Now().Add(-10 * time.Second),
	}
	d := Decision{Action: domain.AutoscaleDown, FromCount: 5, ToCount: 4, Reason: "free_slots=1000 excess"}
	got := s.applyCooldown(d, last, time.Now())
	if got.Action != domain.AutoscaleDown {
		t.Errorf("Action = %q, want scale_down (cross-action cooldown is independent)", got.Action)
	}
}

// TestExecute_ScaleUp_Success pins the success path: a CloudProvider
// returning ("", nil) (the Noop semantic) records a scale_up event
// with succeeded=true and an empty provider_kind.
func TestExecute_ScaleUp_Success(t *testing.T) {
	var provCalls int
	cloud := &MockCloudProvider{
		ProvisionFunc: func(_ context.Context, region string) (string, error) {
			provCalls++
			if region != "fra" {
				t.Errorf("Provision region = %q, want fra", region)
			}
			return "", nil // noop semantic
		},
	}
	repo := &mockEventRepo{}
	s := NewService(Deps{Cfg: Config{}, Cloud: cloud, EventRepo: repo, Log: discardLogger()})

	d := Decision{Action: domain.AutoscaleUp, FromCount: 1, ToCount: 2, Reason: "free_slots=0"}
	s.execute(context.Background(), "fra", []WorkerHeadroom{
		{WorkerID: "w1", Region: "fra"},
	}, d)

	if provCalls != 1 {
		t.Errorf("Provision calls = %d, want 1", provCalls)
	}
	if len(repo.events) != 1 {
		t.Fatalf("events = %d, want 1", len(repo.events))
	}
	ev := repo.events[0]
	if ev.Action != domain.AutoscaleUp {
		t.Errorf("Action = %q, want scale_up", ev.Action)
	}
	if !ev.Succeeded {
		t.Errorf("Succeeded = false, want true")
	}
	if ev.ErrorMessage != nil {
		t.Errorf("ErrorMessage = %v, want nil", *ev.ErrorMessage)
	}
	if ev.ProviderKind != "mock" {
		t.Errorf("ProviderKind = %q, want mock", ev.ProviderKind)
	}
	// CreatedAt must be set to roughly now() — applyCooldown reads it
	// back from lastEventByRegion, and a zero value makes `now.Sub(zero)`
	// a huge positive number that defeats the cooldown gate. Pin the
	// upper-bound window so the test catches a regression where the
	// field is left default-zero again.
	if ev.CreatedAt.IsZero() {
		t.Errorf("CreatedAt = zero time, want ~now() (applyCooldown depends on this)")
	}
	if delta := time.Since(ev.CreatedAt); delta < 0 || delta > 5*time.Second {
		t.Errorf("CreatedAt off by %v, want within 5s of now()", delta)
	}
}

// TestExecute_ScaleUp_Failure pins the error path: a CloudProvider
// returning an error records a scale_up event with succeeded=false
// and a non-nil error_message. The autoscaler continues running.
func TestExecute_ScaleUp_Failure(t *testing.T) {
	cloud := &MockCloudProvider{
		ProvisionFunc: func(_ context.Context, _ string) (string, error) {
			return "", errors.New("hetzner: rate limited")
		},
	}
	repo := &mockEventRepo{}
	s := NewService(Deps{Cfg: Config{}, Cloud: cloud, EventRepo: repo, Log: discardLogger()})

	d := Decision{Action: domain.AutoscaleUp, FromCount: 1, ToCount: 2, Reason: "free_slots=0"}
	s.execute(context.Background(), "fra", []WorkerHeadroom{{WorkerID: "w1", Region: "fra"}}, d)

	if len(repo.events) != 1 {
		t.Fatalf("events = %d, want 1", len(repo.events))
	}
	ev := repo.events[0]
	if ev.Succeeded {
		t.Errorf("Succeeded = true, want false")
	}
	if ev.ErrorMessage == nil {
		t.Fatalf("ErrorMessage = nil, want 'hetzner: rate limited'")
	}
	if *ev.ErrorMessage != "hetzner: rate limited" {
		t.Errorf("ErrorMessage = %q, want 'hetzner: rate limited'", *ev.ErrorMessage)
	}
}

// TestExecute_ScaleDown_PicksOldest pins pickVictim's semantics:
// the scale_down path must call Deprovision with the worker that
// has the oldest LastSeen — the safest candidate for removal.
func TestExecute_ScaleDown_PicksOldest(t *testing.T) {
	var gotWorker string
	now := time.Now()
	cloud := &MockCloudProvider{
		DeprovisionFunc: func(_ context.Context, _, workerID string) error {
			gotWorker = workerID
			return nil
		},
	}
	repo := &mockEventRepo{}
	s := NewService(Deps{Cfg: Config{}, Cloud: cloud, EventRepo: repo, Log: discardLogger()})

	workers := []WorkerHeadroom{
		{WorkerID: "w_new", Region: "fra", LastSeen: now},
		{WorkerID: "w_old", Region: "fra", LastSeen: now.Add(-10 * time.Minute)},
		{WorkerID: "w_mid", Region: "fra", LastSeen: now.Add(-1 * time.Minute)},
	}
	d := Decision{Action: domain.AutoscaleDown, FromCount: 3, ToCount: 2, Reason: "free_slots=600 excess"}
	s.execute(context.Background(), "fra", workers, d)

	if gotWorker != "w_old" {
		t.Errorf("Deprovision target = %q, want w_old (oldest LastSeen)", gotWorker)
	}
}

// TestPickVictim_Empty pins the empty-fleet defensive case: pickVictim
// must not panic on an empty slice.
func TestPickVictim_Empty(t *testing.T) {
	if got := pickVictim(nil); got != "" {
		t.Errorf("pickVictim(nil) = %q, want \"\"", got)
	}
}

// TestNoopCloudProvider_KindReturnsNoop pins the provider_kind
// column for the dev default — admin endpoint shows operators
// which provider ran.
func TestNoopCloudProvider_KindReturnsNoop(t *testing.T) {
	n := NewNoopCloudProvider(discardLogger())
	if n.Kind() != "noop" {
		t.Errorf("Kind = %q, want noop", n.Kind())
	}
}

// TestNewCloudProvider_RejectsUnknown pins the typo-guard: a config
// with provider_kind=hetzner must fail to start with a clear error
// rather than silently running as Noop.
func TestNewCloudProvider_RejectsUnknown(t *testing.T) {
	_, err := NewCloudProvider("hetzner", discardLogger())
	if err == nil {
		t.Fatal("err = nil, want error for unknown provider_kind")
	}
	if !strings.Contains(err.Error(), "hetzner") {
		t.Errorf("err = %q, want substring 'hetzner'", err.Error())
	}
}

// TestExecute_NoopDoesNotOverwriteLastEvent pins the cooldown-gate
// invariant: a noop event recorded by execute (whether from "within
// target" or from a prior cooldown suppression) must NOT overwrite
// lastEventByRegion[region]. If it did, a sequence like
//
//	tick 1: scale_up → lastEvent=Up   (CreatedAt=T0)
//	tick 2: noop     → lastEvent=Noop (CreatedAt=T1)  <-- BUG would overwrite here
//	tick 3: scale_up → applyCooldown sees Noop vs Up → "different
//	                       action class" → scale_up proceeds → Provision fires again
//
// would re-fire Provision inside the cooldown window. The
// regression test TestRegression_CooldownSuppressesSecondScaleUp
// exercises the same scenario end-to-end with NATS; this test pins
// the in-memory contract directly so the failure mode is caught by
// a unit test that doesn't need Docker.
func TestExecute_NoopDoesNotOverwriteLastEvent(t *testing.T) {
	cloud := &MockCloudProvider{
		ProvisionFunc: func(_ context.Context, _ string) (string, error) { return "", nil },
	}
	repo := &mockEventRepo{}
	s := NewService(Deps{
		Cfg:       Config{ScaleUpCooldownS: 60},
		Cloud:     cloud,
		EventRepo: repo,
		Log:       discardLogger(),
	})
	workers := []WorkerHeadroom{{WorkerID: "w1", Region: "fra"}}

	// Tick 1: real scale_up — must update lastEventByRegion.
	s.execute(context.Background(), "fra", workers,
		Decision{Action: domain.AutoscaleUp, FromCount: 1, ToCount: 2, Reason: "free_slots=0"})
	if got := s.lastEventByRegion["fra"]; got == nil || got.Action != domain.AutoscaleUp {
		t.Fatalf("after scale_up, lastEventByRegion[fra] = %+v, want scale_up", got)
	}

	// Tick 2: noop (cooldown suppressed) — must NOT overwrite.
	s.execute(context.Background(), "fra", workers,
		Decision{Action: domain.AutoscaleNoop, FromCount: 1, ToCount: 1, Reason: "scale_up cooldown"})
	got := s.lastEventByRegion["fra"]
	if got == nil {
		t.Fatalf("after noop, lastEventByRegion[fra] = nil, want the prior scale_up to remain")
	}
	if got.Action != domain.AutoscaleUp {
		t.Errorf("after noop, lastEventByRegion[fra].Action = %q, want scale_up (cooldown gate depends on this)", got.Action)
	}
	if got.Reason != "free_slots=0" {
		t.Errorf("after noop, lastEventByRegion[fra].Reason = %q, want 'free_slots=0' (no-op must not overwrite the scale event)", got.Reason)
	}

	// Tick 3: applyCooldown against the now-stable lastEvent must
	// still suppress (it sees scale_up, not noop).
	now := time.Now()
	last := s.lastEventByRegion["fra"]
	// Force the elapsed to 1s to land squarely inside the 60s cooldown.
	last.CreatedAt = now.Add(-1 * time.Second)
	d := Decision{Action: domain.AutoscaleUp, FromCount: 1, ToCount: 2, Reason: "free_slots=0"}
	if got := s.applyCooldown(d, last, now); got.Action != domain.AutoscaleNoop {
		t.Errorf("tick-3 applyCooldown.Action = %q, want noop (cooldown gate must survive across noop tick)", got.Action)
	}
}

// TestNewCloudProvider_AcceptsKnown pins the happy paths: empty
// (defaults to noop) and "noop" must succeed. "mock" is rejected
// here — tests construct MockCloudProvider directly rather than
// routing through the factory.
func TestNewCloudProvider_AcceptsKnown(t *testing.T) {
	for _, kind := range []string{"", "noop"} {
		p, err := NewCloudProvider(kind, discardLogger())
		if err != nil {
			t.Errorf("NewCloudProvider(%q) err = %v", kind, err)
			continue
		}
		if p == nil {
			t.Errorf("NewCloudProvider(%q) = nil", kind)
		}
	}
}

// TestSubscribe_NoNATSConnectionSkips pins the dev/test fallback:
// when nc is nil, Subscribe returns nil without panicking. Mirrors
// WorkerService.SubscribeHeartbeats' nc=nil branch.
func TestSubscribe_NoNATSConnectionSkips(t *testing.T) {
	s := NewService(Deps{
		Cfg:        Config{Enabled: true},
		DeployRepo: &mockDeployRepo{},
		EventRepo:  &mockEventRepo{},
		Cloud:      &MockCloudProvider{},
		Log:        discardLogger(),
	})
	if err := s.Subscribe(context.Background()); err != nil {
		t.Errorf("Subscribe(nc=nil) err = %v, want nil", err)
	}
}

// TestSubscribe_DisabledSkips pins the prod-fallback: when
// Enabled=false, Subscribe returns nil without subscribing.
// This is the path cmd/api/main.go takes when the operator has
// not opted in.
func TestSubscribe_DisabledSkips(t *testing.T) {
	s := NewService(Deps{
		Cfg:        Config{Enabled: false},
		DeployRepo: &mockDeployRepo{},
		EventRepo:  &mockEventRepo{},
		Cloud:      &MockCloudProvider{},
		Log:        discardLogger(),
	})
	if err := s.Subscribe(context.Background()); err != nil {
		t.Errorf("Subscribe(enabled=false) err = %v, want nil", err)
	}
}

// TestDesiredApps_FallbackOnDBError pins the safe default when the
// deployment repository is unreachable: desiredApps must return 0
// (not panic, not return a stale count) so the autoscaler sees
// needed=1 and noops rather than scaling down based on bad data.
func TestDesiredApps_FallbackOnDBError(t *testing.T) {
	s := NewService(Deps{
		DeployRepo: &mockDeployRepo{
			countFunc: func(_ context.Context) (int, error) {
				return 0, errors.New("connection refused")
			},
		},
		EventRepo: &mockEventRepo{},
		Cloud:     &MockCloudProvider{},
		Log:       discardLogger(),
	})
	if n := s.desiredApps(context.Background()); n != 0 {
		t.Errorf("desiredApps = %d, want 0 on DB error", n)
	}
}

// TestDesiredApps_ReturnsCount pins the happy path: the deploy repo
// returns a count and desiredApps passes it through.
func TestDesiredApps_ReturnsCount(t *testing.T) {
	s := NewService(Deps{
		DeployRepo: &mockDeployRepo{
			countFunc: func(_ context.Context) (int, error) { return 42, nil },
		},
		EventRepo: &mockEventRepo{},
		Cloud:     &MockCloudProvider{},
		Log:       discardLogger(),
	})
	if n := s.desiredApps(context.Background()); n != 42 {
		t.Errorf("desiredApps = %d, want 42", n)
	}
}

// TestFormatSeconds_LessThanOne pins the sub-second branch: a value
// like 0.5 must render as "<1s" so cooldown log messages are
// readable even when the decision tick fires almost immediately
// after the last event.
func TestFormatSeconds_LessThanOne(t *testing.T) {
	if got := formatSeconds(0.5); got != "<1s" {
		t.Errorf("formatSeconds(0.5) = %q, want \"<1s\"", got)
	}
}

// TestEvaluateAll_ScaleUpOnShortage exercises the full decision
// pipeline end-to-end inside a single tick:
//   - Seed a fleet with 1 worker reporting 10 free slots
//   - desiredApps returns 20 → needed = 24 → shortage → scale_up
//
// This test validates that the snapshot → ComputeDecision → execute
// chain works correctly without requiring a NATS server.
func TestEvaluateAll_ScaleUpOnShortage(t *testing.T) {
	events := &mockEventRepo{}
	s := NewService(Deps{
		Cfg: Config{
			Enabled: true, MinWorkers: 1, MaxWorkers: 10,
			TargetHeadroomPct: 20, DecisionIntervalS: 30,
		},
		DeployRepo: &mockDeployRepo{
			countFunc: func(_ context.Context) (int, error) { return 20, nil },
		},
		EventRepo: events,
		Cloud: &MockCloudProvider{
			ProvisionFunc: func(_ context.Context, _ string) (string, error) { return "", nil },
		},
		Log: discardLogger(),
	})

	body, _ := json.Marshal(map[string]any{
		"worker_id": "w_fra_abc",
		"region":    "fra",
		"cluster_headroom": map[string]any{"app_slots": 10},
	})
	s.handleHeartbeat(natsMsg(body))

	s.evaluateAll(context.Background())

	if len(events.events) == 0 {
		t.Fatal("no events recorded after evaluateAll")
	}
	ev := events.events[0]
	if ev.Action != domain.AutoscaleUp {
		t.Errorf("Action = %q, want scale_up", ev.Action)
	}
	if ev.Region != "fra" {
		t.Errorf("Region = %q, want fra", ev.Region)
	}
	if ev.FromCount != 1 || ev.ToCount != 2 {
		t.Errorf("FromCount=%d ToCount=%d, want 1→2", ev.FromCount, ev.ToCount)
	}
	if !ev.Succeeded {
		t.Errorf("Succeeded = false, want true")
	}
	if ev.ProviderKind != "mock" {
		t.Errorf("ProviderKind = %q, want mock", ev.ProviderKind)
	}
}

// TestEvaluateAll_MultiRegionIndependent verifies that the per-region
// fan-out in evaluateAll produces independent decisions for each
// region. fra needs a scale_up (1 worker, 10 slots, desiredApps=20),
// iad needs a scale_down (5 workers, 2500 free slots, desiredApps=20).
func TestEvaluateAll_MultiRegionIndependent(t *testing.T) {
	events := &mockEventRepo{}
	s := NewService(Deps{
		Cfg: Config{
			Enabled: true, MinWorkers: 1, MaxWorkers: 10,
			TargetHeadroomPct: 20, DecisionIntervalS: 30,
		},
		DeployRepo: &mockDeployRepo{
			countFunc: func(_ context.Context) (int, error) { return 20, nil },
		},
		EventRepo: events,
		Cloud: &MockCloudProvider{
			ProvisionFunc:   func(_ context.Context, _ string) (string, error) { return "", nil },
			DeprovisionFunc: func(_ context.Context, _, _ string) error { return nil },
		},
		Log: discardLogger(),
	})

	// fra: 1 worker with 10 slots → shortage → scale_up
	s.handleHeartbeat(natsMsg(jsonBody(map[string]any{
		"worker_id": "w_fra_abc", "region": "fra",
		"cluster_headroom": map[string]any{"app_slots": 10},
	})))
	// iad: 5 workers with 500 slots each → excess → scale_down
	for i := 0; i < 5; i++ {
		s.handleHeartbeat(natsMsg(jsonBody(map[string]any{
			"worker_id": fmt.Sprintf("w_iad_%d", i), "region": "iad",
			"cluster_headroom": map[string]any{"app_slots": 500},
		})))
	}

	s.evaluateAll(context.Background())

	if len(events.events) != 2 {
		t.Fatalf("events = %d, want 2 (fra scale_up, iad scale_down)", len(events.events))
	}

	// Events may arrive in any order (map iteration in evaluateAll).
	// Look up by region instead of relying on positional indexing.
	var evFra, evIad *domain.AutoscaleEvent
	for i := range events.events {
		switch events.events[i].Region {
		case "fra":
			evFra = events.events[i]
		case "iad":
			evIad = events.events[i]
		}
	}
	if evFra == nil {
		t.Fatal("no event for region fra")
	}
	if evIad == nil {
		t.Fatal("no event for region iad")
	}
	if evFra.Action != domain.AutoscaleUp {
		t.Errorf("fra Action = %q, want scale_up", evFra.Action)
	}
	if evFra.Region != "fra" {
		t.Errorf("fra Region = %q, want fra", evFra.Region)
	}
	if evIad.Action != domain.AutoscaleDown {
		t.Errorf("iad Action = %q, want scale_down", evIad.Action)
	}
	if evIad.Region != "iad" {
		t.Errorf("iad Region = %q, want iad", evIad.Region)
	}
}

// TestEvaluateAll_NoopOnTarget verifies that when the fleet is within
// the target headroom band, evaluateAll records a noop event.
func TestEvaluateAll_NoopOnTarget(t *testing.T) {
	events := &mockEventRepo{}
	s := NewService(Deps{
		Cfg: Config{
			Enabled: true, MinWorkers: 1, MaxWorkers: 10,
			TargetHeadroomPct: 20, DecisionIntervalS: 30,
		},
		DeployRepo: &mockDeployRepo{
			countFunc: func(_ context.Context) (int, error) { return 10, nil },
		},
		EventRepo: events,
		Cloud:     &MockCloudProvider{},
		Log:       discardLogger(),
	})

	// 1 worker with 50 slots, desiredApps=10 → needed=12, free=50 → within target
	s.handleHeartbeat(natsMsg(jsonBody(map[string]any{
		"worker_id": "w_fra_abc", "region": "fra",
		"cluster_headroom": map[string]any{"app_slots": 50},
	})))

	s.evaluateAll(context.Background())

	if len(events.events) != 1 {
		t.Fatalf("events = %d, want 1 (noop)", len(events.events))
	}
	if events.events[0].Action != domain.AutoscaleNoop {
		t.Errorf("Action = %q, want noop", events.events[0].Action)
	}
}

// TestEvaluateAll_EvictsStaleWorker pins the staleness eviction
// contract: a worker whose LastSeen is older than StaleAfter must be
// removed from the fleet map before the decision snapshot.
//
// When the last worker in a region is evicted, the region is removed
// from the fleet map entirely and no decision tick fires for it —
// the autoscaler only evaluates regions that have at least one active
// worker. A new heartbeat will re-add the worker.
func TestEvaluateAll_EvictsStaleWorker(t *testing.T) {
	events := &mockEventRepo{}
	s := NewService(Deps{
		Cfg: Config{
			Enabled: true, MinWorkers: 1, MaxWorkers: 10,
			TargetHeadroomPct: 20, StaleAfter: 5 * time.Minute,
		},
		DeployRepo: &mockDeployRepo{
			countFunc: func(_ context.Context) (int, error) { return 0, nil },
		},
		EventRepo: events,
		Cloud:     &MockCloudProvider{},
		Log:       discardLogger(),
	})

	// Inject a stale worker directly into the fleet map.
	s.mu.Lock()
	s.fleets["fra"] = map[string]WorkerHeadroom{
		"w_stale": {
			WorkerID: "w_stale", Region: "fra",
			Headroom: &nats.ClusterHeadroom{AppSlots: 50},
			LastSeen: time.Now().Add(-10 * time.Minute),
		},
	}
	s.mu.Unlock()

	s.evaluateAll(context.Background())

	// Stale worker must be evicted from the fleet map and the region
	// removed (no active workers in that region).
	s.mu.Lock()
	_, exists := s.fleets["fra"]
	s.mu.Unlock()
	if exists {
		t.Error("stale worker was not evicted from fleets")
	}

	// No events — the region was empty after eviction, so no decision
	// tick fired for it. (When a region has 0 workers, evaluateAll
	// has nothing to snapshot and skips it.)
	if len(events.events) != 0 {
		t.Errorf("events = %d, want 0 (no decision fires for an empty region)", len(events.events))
	}

	// A fresh heartbeat must re-add the worker (not blocked by
	// staleness — eviction happens in evaluateAll, not in
	// handleHeartbeat).
	body, _ := json.Marshal(map[string]any{
		"worker_id": "w_fra_new", "region": "fra",
		"cluster_headroom": map[string]any{"app_slots": 50},
	})
	s.handleHeartbeat(natsMsg(body))
	s.mu.Lock()
	fleet, ok := s.fleets["fra"]
	s.mu.Unlock()
	if !ok || len(fleet) != 1 {
		t.Error("fresh heartbeat must re-add worker after eviction")
	}
}

// jsonBody is a convenience wrapper for json.Marshal in tests.
func jsonBody(v map[string]any) []byte {
	b, _ := json.Marshal(v)
	return b
}

// natsMsg wraps a byte payload in a *natsio.Msg. We don't need a
// real NATS message — the service only reads msg.Data. Subject is
// hardcoded because the autoscaler doesn't filter by subject.
func natsMsg(data []byte) *natsio.Msg {
	return &natsio.Msg{Subject: "edgecloud.heartbeats.fra", Data: data}
}
