package autoscale

import (
	"fmt"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
)

// legacyAssumedAppSlots is the fallback used when a worker's
// heartbeat pre-dates #85 (no ClusterHeadroom). Without the field we
// can't measure capacity, so we assume a conservative 50 slots —
// matching the historical default `PortPool` pre-population count
// before the 100-slot bump landed in PR #1.
//
// Picking 50 (vs. 0 or 100) biases the autoscaler toward *not*
// scale-up on legacy workers: under-counting capacity means we'd
// scale up sooner, over-counting means we'd never scale up. 50 is
// the midpoint that matches the pre-#85 default and is documented in
// the migration notes.
const legacyAssumedAppSlots = 50

// WorkerHeadroom is the autoscaler's per-worker view. It is
// reconstructed from every heartbeat the service receives. LastSeen
// is used by the staleness check (a worker that hasn't heartbeated
// in 3× the cadence is dropped from the fleet view so scale-down can
// fire).
type WorkerHeadroom struct {
	WorkerID string
	Region   string
	// Headroom is the worker-reported ClusterHeadroom, or nil for
	// pre-#85 workers that don't report capacity.
	Headroom *nats.ClusterHeadroom
	LastSeen time.Time
}

// FleetState is the autoscaler's view of one region at one decision
// tick. Workers with LastSeen older than StaleAfter are filtered
// out by the service before passing to ComputeDecision — the
// decision function itself is pure and does no time-based eviction.
type FleetState struct {
	Workers     []WorkerHeadroom
	DesiredApps int // total active_deployments rows in this region
}

// Decision is the autoscaler's verdict on what to do next. The
// Service layer turns a non-noop Decision into an `autoscale_events`
// row (and a CloudProvider call) once any cooldown gate has been
// applied.
type Decision struct {
	Action    domain.AutoscaleAction
	FromCount int
	ToCount   int
	// Reason is a short, machine-readable explanation of why this
	// decision was made. Persisted to `autoscale_events.reason` so
	// operators can answer "why did the fleet size change?" without
	// correlating logs.
	Reason string
}

// ComputeDecision returns the next Decision for `state` given the
// configured `cfg`. Pure function: no I/O, no time, no logging. The
// service layer is responsible for time-based eviction, cooldown,
// and execution. Keeping the decision logic pure means it can be
// table-tested without any testcontainer, clock stub, or mock.
//
// Decision priority (highest first):
//  1. Below min_workers  → scale_up to min_workers (we lost a worker;
//     tenant traffic is at risk).
//  2. Above max_workers  → scale_down to max_workers (operator
//     override or transient join spike).
//  3. Free slots < needed  → scale_up by 1 (gradual: we add one
//     worker per tick until free slots catch
//     up; avoids runaway provisioning if a
//     fleet-wide outage triggered all regions
//     to scale up simultaneously).
//  4. Free slots > 2 × needed → scale_down by 1 (sustained over-
//     provisioning; the 2× hysteresis stops
//     flapping when desired-apps dips briefly
//     below the band edge).
//  5. Otherwise → noop with reason "within target".
//
// `needed = DesiredApps × (1 + TargetHeadroomPct/100)` rounded up,
// with a floor of 1 so the autoscaler always reserves one slot even
// when no deployments are active. (Why: a brand-new tenant calling
// `edge deploy` shouldn't have to wait 30s for the first worker to
// come online if the fleet has gone to zero slots.)
func ComputeDecision(state FleetState, cfg Config) Decision {
	current := len(state.Workers)

	if cfg.MinWorkers > 0 && current < cfg.MinWorkers {
		return Decision{
			Action:    domain.AutoscaleUp,
			FromCount: current,
			ToCount:   cfg.MinWorkers,
			Reason:    fmt.Sprintf("below min_workers (current=%d, min=%d)", current, cfg.MinWorkers),
		}
	}
	if cfg.MaxWorkers > 0 && current > cfg.MaxWorkers {
		return Decision{
			Action:    domain.AutoscaleDown,
			FromCount: current,
			ToCount:   cfg.MaxWorkers,
			Reason:    fmt.Sprintf("above max_workers (current=%d, max=%d)", current, cfg.MaxWorkers),
		}
	}

	totalFreeSlots := 0
	for _, w := range state.Workers {
		if w.Headroom != nil {
			totalFreeSlots += int(w.Headroom.AppSlots)
		} else {
			totalFreeSlots += legacyAssumedAppSlots
		}
	}

	// needed = desired × (1 + buffer/100), with a floor of 1 so we
	// always keep at least one slot reserved.
	needed := state.DesiredApps + (state.DesiredApps*cfg.TargetHeadroomPct)/100
	if needed < 1 {
		needed = 1
	}

	if totalFreeSlots < needed && (cfg.MaxWorkers == 0 || current < cfg.MaxWorkers) {
		return Decision{
			Action:    domain.AutoscaleUp,
			FromCount: current,
			ToCount:   current + 1,
			Reason:    fmt.Sprintf("free_slots=%d needed=%d", totalFreeSlots, needed),
		}
	}
	if totalFreeSlots > needed*2 && (cfg.MinWorkers == 0 || current > cfg.MinWorkers) {
		return Decision{
			Action:    domain.AutoscaleDown,
			FromCount: current,
			ToCount:   current - 1,
			Reason:    fmt.Sprintf("free_slots=%d excess needed=%d", totalFreeSlots, needed),
		}
	}

	return Decision{
		Action:    domain.AutoscaleNoop,
		FromCount: current,
		ToCount:   current,
		Reason:    "within target",
	}
}
