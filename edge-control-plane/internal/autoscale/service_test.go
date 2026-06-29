package autoscale

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
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

// natsMsg wraps a byte payload in a *natsio.Msg. We don't need a
// real NATS message — the service only reads msg.Data. Subject is
// hardcoded because the autoscaler doesn't filter by subject.
func natsMsg(data []byte) *natsio.Msg {
	return &natsio.Msg{Subject: "edgecloud.heartbeats.fra", Data: data}
}
