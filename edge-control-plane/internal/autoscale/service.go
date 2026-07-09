package autoscale

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/loophealth"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	natsio "github.com/nats-io/nats.go"
)

// Config is the autoscaler-side mirror of internal/config.AutoscaleConfig.
// Kept local so the package has no upward dependency on internal/config
// (which would prevent it from being unit-tested with a stand-alone
// Config struct). cmd/api/main.go copies fields across.
type Config struct {
	Enabled            bool
	MinWorkers         int
	MaxWorkers         int
	TargetHeadroomPct  int
	ScaleUpCooldownS   int
	ScaleDownCooldownS int
	DecisionIntervalS  int
	// StaleAfter is the duration after which a worker that hasn't
	// heartbeated is evicted from the fleet view. Zero means no
	// staleness eviction (backward compatible). A reasonable value
	// is 3× the heartbeat interval (default 90s).
	StaleAfter time.Duration
}

// deployRepoInterface exposes only Count, which the autoscaler uses
// to compute DesiredApps fleet-wide. (Region partitioning of the
// deployment table is a follow-up — see Count's doc comment.)
type deployRepoInterface interface {
	Count(ctx context.Context) (int, error)
}

// eventRepoInterface is the autoscale_events writer the service uses.
// The narrower surface keeps MockCloudProvider tests focused.
type eventRepoInterface interface {
	Insert(ctx context.Context, e *domain.AutoscaleEvent) (int64, error)
}

// Deps is the constructor's parameter bag. All fields are required
// except Log (defaults to slog.Default()), Tracker (nil disables
// liveness reporting — used in tests that build a Service without
// the control-plane tracker), and Enabled (read from Cfg).
type Deps struct {
	Cfg        Config
	NC         *natsio.Conn
	DeployRepo deployRepoInterface
	EventRepo  eventRepoInterface
	Cloud      CloudProvider
	Log        *slog.Logger
	Tracker    *loophealth.Tracker
}

// Service subscribes to NATS heartbeats, maintains a per-region fleet
// view, and on each decision tick either:
//   - calls CloudProvider.Provision / Deprovision and records an
//     `autoscale_events` row (scale_up / scale_down).
//   - records a noop row when the fleet is in-band or a cooldown gate
//     suppressed an otherwise-valid scale event.
//
// Concurrency model:
//   - A single goroutine owns `fleets` and `lastEventByRegion`. The
//     Subscribe goroutine updates fleets from heartbeats; the decision
//     ticker reads from it. No locks are needed because only one
//     goroutine touches either.
//   - CloudProvider calls run synchronously on the decision goroutine
//     — a slow provision API extends the next tick but does not block
//     the heartbeat path.
type Service struct {
	cfg        Config
	nc         *natsio.Conn
	deployRepo deployRepoInterface
	eventRepo  eventRepoInterface
	cloud      CloudProvider
	log        *slog.Logger
	// tracker optionally receives liveness updates from the run loop.
	// A nil tracker is permitted (existing tests build Service without
	// one); the run loop skips Beat() calls when it's nil. Wired by
	// app.New so /health can surface autoscale freshness (review
	// finding #3).
	tracker *loophealth.Tracker

	// mu guards fleets + lastEventByRegion. Uses RWMutex so the admin
	// endpoint (SnapshotFleet) can read without blocking heartbeat writes.
	// Writers (handleHeartbeat, evaluateAll, execute) take the write lock;
	// readers (SnapshotFleet) take the read lock.
	//
	// lastEventByRegion is keyed by region so each region's cooldown
	// clock is independent. A scale_up in fra does not block iad's
	// next scale_up, and vice versa. (Pre-#85 this was a single
	// `lastEvent` shared cluster-wide, which contradicted the
	// per-action-class docstring on applyCooldown.)
	mu                sync.RWMutex
	fleets            map[string]map[string]WorkerHeadroom // region → workerID → headroom
	lastEventByRegion map[string]*domain.AutoscaleEvent
}

// NewService constructs an autoscaler Service. Returns nil when cfg.Enabled
// is false — callers should check for nil and skip Subscribe. Mirrors
// the convention of `service.NewWorkerService`, which returns a
// non-nil struct even when the connection is absent.
func NewService(d Deps) *Service {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.Cfg.DecisionIntervalS < 1 {
		d.Cfg.DecisionIntervalS = 30
	}
	return &Service{
		cfg:               d.Cfg,
		nc:                d.NC,
		deployRepo:        d.DeployRepo,
		eventRepo:         d.EventRepo,
		cloud:             d.Cloud,
		log:               d.Log,
		tracker:           d.Tracker,
		fleets:            make(map[string]map[string]WorkerHeadroom),
		lastEventByRegion: make(map[string]*domain.AutoscaleEvent),
	}
}

// Subscribe attaches to edgecloud.heartbeats.> and starts the
// decision loop. Idempotent: returns nil without subscribing when
// the autoscaler is disabled or NATS is unconfigured (matches
// WorkerService.SubscribeHeartbeats at internal/service/worker.go:155).
//
// Returns immediately on success; the goroutine exits when ctx is
// canceled.
func (s *Service) Subscribe(ctx context.Context) error {
	if !s.cfg.Enabled {
		s.log.Info("autoscale: disabled (cfg.enabled=false), skipping Subscribe")
		return nil
	}
	if s.nc == nil {
		s.log.Info("autoscale: no NATS connection, skipping Subscribe (e.g. test mode)")
		return nil
	}
	ch := make(chan *natsio.Msg, 100)
	sub, err := s.nc.ChanSubscribe("edgecloud.heartbeats.>", ch)
	if err != nil {
		return fmt.Errorf("autoscale: subscribe heartbeats: %w", err)
	}

	go s.run(ctx, sub, ch)
	s.log.Info("autoscale: subscribed to edgecloud.heartbeats.>", "decision_interval_s", s.cfg.DecisionIntervalS)
	return nil
}

func (s *Service) run(ctx context.Context, sub *natsio.Subscription, ch <-chan *natsio.Msg) {
	defer func() { _ = sub.Unsubscribe() }()

	ticker := time.NewTicker(time.Duration(s.cfg.DecisionIntervalS) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			s.handleHeartbeat(msg)
			if s.tracker != nil {
				s.tracker.Get("autoscale").Beat()
			}
		case <-ticker.C:
			s.evaluateAll(ctx)
			if s.tracker != nil {
				s.tracker.Get("autoscale").Beat()
			}
		}
	}
}

// evaluateAll snapshots every region's fleet, computes a decision,
// and dispatches it through applyCooldown + execute. Per-region
// processing keeps the failure blast radius small: a panic or DB
// error in one region's decision doesn't poison the others.
//
// Noop decisions (whether from "within target" in ComputeDecision or
// from cooldown suppression in applyCooldown) flow through execute
// so eventRepo.Insert records them — operators reading the
// autoscale_events table can see both "we wanted to scale but were
// suppressed" and "fleet is healthy, no action needed". Without
// this, a cooldown-suppressed tick would be invisible to operators.
func (s *Service) evaluateAll(ctx context.Context) {
	s.mu.Lock()

	// Evict stale workers whose last heartbeat exceeds StaleAfter.
	// Must happen before the snapshot so stale capacity doesn't
	// pollute the decision input. A worker that was evicted will be
	// re-added on its next heartbeat (handleHeartbeat), which is
	// correct — the worker isn't gone, just unresponsive.
	if s.cfg.StaleAfter > 0 {
		now := time.Now()
		for region, workers := range s.fleets {
			for wid, w := range workers {
				if now.Sub(w.LastSeen) > s.cfg.StaleAfter {
					delete(s.fleets[region], wid)
					s.log.Info("autoscale: evicted stale worker",
						"worker_id", wid, "region", region,
						"last_seen", w.LastSeen, "age", now.Sub(w.LastSeen).Round(time.Second))
				}
			}
			if len(s.fleets[region]) == 0 {
				delete(s.fleets, region)
			}
		}
	}

	snapshot := make(map[string][]WorkerHeadroom, len(s.fleets))
	for region, workers := range s.fleets {
		list := make([]WorkerHeadroom, 0, len(workers))
		for _, w := range workers {
			list = append(list, w)
		}
		snapshot[region] = list
	}
	lastEvents := make(map[string]*domain.AutoscaleEvent, len(s.lastEventByRegion))
	for region, ev := range s.lastEventByRegion {
		lastEvents[region] = ev
	}
	s.mu.Unlock()

	for region, workers := range snapshot {
		state := FleetState{Workers: workers, DesiredApps: s.desiredApps(ctx)}
		d := ComputeDecision(state, s.cfg)
		d = s.applyCooldown(d, lastEvents[region], time.Now())
		s.execute(ctx, region, workers, d)
	}
}

// desiredApps returns the fleet-wide active deployment count.
// The same value is used for every region — region partitioning of the
// deployment table is a follow-up (the data model has region only
// on workers today). Falls back to 0 on DB error — the autoscaler would
// then see `needed=1` (the floor) and likely noop, which is the
// safer default than scale_down based on stale data.
func (s *Service) desiredApps(ctx context.Context) int {
	if s.deployRepo == nil {
		return 0
	}
	n, err := s.deployRepo.Count(ctx)
	if err != nil {
		s.log.Warn("autoscale: Count(active_deployments) failed; defaulting DesiredApps=0", "err", err)
		return 0
	}
	return n
}

// applyCooldown converts a Decision into a noop when the same action
// class fired within the configured cooldown window. Kept separate
// from ComputeDecision so the pure decision logic can be tested
// without any clock stub.
//
// Cooldown is per-action-class: a scale_up only gates the next
// scale_up, and a scale_down only gates the next scale_down.
// A scale_down 30s after a scale_up is NOT suppressed — the two
// action classes have independent cooldown timers.
//
// lastEvent is the most-recent autoscale_events row from the DB.
// nil means no cooldown history.
func (s *Service) applyCooldown(d Decision, lastEvent *domain.AutoscaleEvent, now time.Time) Decision {
	if lastEvent == nil || d.Action == domain.AutoscaleNoop {
		return d
	}
	if lastEvent.Action != d.Action {
		// Different action class — cooldowns are independent.
		return d
	}
	elapsed := now.Sub(lastEvent.CreatedAt).Seconds()
	var cooldown int
	switch d.Action {
	case domain.AutoscaleUp:
		cooldown = s.cfg.ScaleUpCooldownS
	case domain.AutoscaleDown:
		cooldown = s.cfg.ScaleDownCooldownS
	default:
		return d
	}
	if cooldown > 0 && elapsed < float64(cooldown) {
		return Decision{
			Action:    domain.AutoscaleNoop,
			FromCount: d.FromCount,
			ToCount:   d.FromCount,
			Reason:    fmt.Sprintf("%s cooldown (last %s ago, cooldown=%ds)", d.Action, formatSeconds(elapsed), cooldown),
		}
	}
	return d
}

// execute calls the CloudProvider and records the result as an
// autoscale_events row. Always logs at info (or warn on failure).
// Mutates lastEventByRegion[region] (only for scale_up / scale_down —
// see below) so the next tick's cooldown check (per-region) sees
// this row.
func (s *Service) execute(ctx context.Context, region string, workers []WorkerHeadroom, d Decision) {
	// CreatedAt is set here (in addition to the DB default) so that
	// lastEventByRegion carries the wall-clock time the decision was
	// made. applyCooldown reads lastEvent.CreatedAt to compute the
	// elapsed window; if we leave it zero-valued, `now.Sub(zero)` is
	// a huge positive number and the cooldown gate never fires
	// (every tick looks like it happened "ages ago").
	ev := &domain.AutoscaleEvent{
		Region:       region,
		Action:       d.Action,
		FromCount:    d.FromCount,
		ToCount:      d.ToCount,
		Reason:       d.Reason,
		ProviderKind: s.cloud.Kind(),
		Succeeded:    true,
		CreatedAt:    time.Now(),
	}
	switch d.Action {
	case domain.AutoscaleUp:
		_, err := s.cloud.Provision(ctx, region)
		if err != nil {
			msg := err.Error()
			ev.Succeeded = false
			ev.ErrorMessage = &msg
		}
	case domain.AutoscaleDown:
		wid := pickVictim(workers)
		if wid == "" {
			s.log.Warn("autoscale: scale_down requested but no victim found", "region", region)
			return
		}
		if err := s.cloud.Deprovision(ctx, region, wid); err != nil {
			msg := err.Error()
			ev.Succeeded = false
			ev.ErrorMessage = &msg
		}
	}

	if _, err := s.eventRepo.Insert(ctx, ev); err != nil {
		s.log.Error("autoscale: insert event", "region", region, "err", err)
	}
	// Only update lastEventByRegion for actual scale events. If we
	// overwrote it on noop events too, a sequence of cooldown-
	// suppressed ticks would reset the cooldown clock to time.Now()
	// each tick — and the NEXT tick would see "different action
	// class" (Noop vs Up) in applyCooldown, let the scale_up
	// through, and re-fire Provision. Pinning lastEventByRegion to
	// the most recent *scale* event preserves the cooldown gate
	// across the noop ticks that exist between two valid scale
	// windows. (Regression test: TestRegression_CooldownSuppressesSecondScaleUp.)
	if d.Action != domain.AutoscaleNoop {
		s.mu.Lock()
		s.lastEventByRegion[region] = ev
		s.mu.Unlock()
	}

	if ev.Succeeded {
		s.log.Info("autoscale: decision executed",
			"region", region, "action", d.Action, "from", d.FromCount, "to", d.ToCount, "reason", d.Reason)
	} else {
		s.log.Warn("autoscale: decision failed",
			"region", region, "action", d.Action, "from", d.FromCount, "to", d.ToCount, "reason", d.Reason, "err", *ev.ErrorMessage)
	}
}

// pickVictim returns the worker ID with the oldest LastSeen — the
// least-recently-heartbeated worker is the safest to remove (least
// live traffic). Returns "" if `workers` is empty.
func pickVictim(workers []WorkerHeadroom) string {
	if len(workers) == 0 {
		return ""
	}
	oldest := workers[0]
	for _, w := range workers[1:] {
		if w.LastSeen.Before(oldest.LastSeen) {
			oldest = w
		}
	}
	return oldest.WorkerID
}

// handleHeartbeat decodes a heartbeat message and refreshes the
// fleet view for the worker's region. Failures are logged and
// swallowed — a malformed heartbeat must not stop the decision loop.
func (s *Service) handleHeartbeat(msg *natsio.Msg) {
	var hb struct {
		WorkerID        string          `json:"worker_id"`
		Region          string          `json:"region"`
		ClusterHeadroom json.RawMessage `json:"cluster_headroom"`
	}
	if err := json.Unmarshal(msg.Data, &hb); err != nil {
		s.log.Warn("autoscale: malformed heartbeat", "err", err)
		return
	}
	if hb.WorkerID == "" || hb.Region == "" {
		return
	}
	var headroom *nats.ClusterHeadroom
	if len(hb.ClusterHeadroom) > 0 {
		var h nats.ClusterHeadroom
		if err := json.Unmarshal(hb.ClusterHeadroom, &h); err != nil {
			s.log.Warn("autoscale: malformed cluster_headroom", "worker_id", hb.WorkerID, "err", err)
		} else {
			headroom = &h
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	region, ok := s.fleets[hb.Region]
	if !ok {
		region = make(map[string]WorkerHeadroom)
		s.fleets[hb.Region] = region
	}
	region[hb.WorkerID] = WorkerHeadroom{
		WorkerID: hb.WorkerID,
		Region:   hb.Region,
		Headroom: headroom,
		LastSeen: time.Now(),
	}
}

// SnapshotFleet returns a stable, sorted copy of the fleet view for
// `region`. Exported for the cluster admin endpoint (PR #4) and tests.
// Returns nil when no heartbeats have arrived for the region.
func (s *Service) SnapshotFleet(region string) []WorkerHeadroom {
	s.mu.RLock()
	defer s.mu.RUnlock()
	regionMap, ok := s.fleets[region]
	if !ok {
		return nil
	}
	out := make([]WorkerHeadroom, 0, len(regionMap))
	for _, w := range regionMap {
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].WorkerID < out[j].WorkerID })
	return out
}

// formatSeconds is a tiny helper to keep the cooldown reason
// readable. Avoids dragging in a humanize dep for one log line.
func formatSeconds(s float64) string {
	if s < 1 {
		return "<1s"
	}
	return fmt.Sprintf("%.0fs", s)
}
