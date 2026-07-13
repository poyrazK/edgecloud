package autoscale

import (
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
)

// makeHeadroom builds a WorkerHeadroom with an optional
// ClusterHeadroom. nil Headroom models a pre-#85 worker (legacy).
func makeHeadroom(id string, region string, appSlots uint32) WorkerHeadroom {
	if appSlots == 0 && id != "" {
		// Heuristic: if id is set but no slots, treat as legacy
		// (Headroom nil). Otherwise, with id="" and slots=0, return
		// a headroom with 0 slots (worker reporting full).
		return WorkerHeadroom{WorkerID: id, Region: region, Headroom: nil}
	}
	return WorkerHeadroom{
		WorkerID: id,
		Region:   region,
		Headroom: &nats.ClusterHeadroom{AppSlots: appSlots},
	}
}

func TestComputeDecision(t *testing.T) {
	tests := []struct {
		name     string
		state    FleetState
		cfg      Config
		wantAct  domain.AutoscaleAction
		wantTo   int
		wantReas string
	}{
		{
			name:     "below min_workers scales up regardless of slots",
			state:    FleetState{Workers: nil, DesiredApps: 0},
			cfg:      Config{MinWorkers: 2, MaxWorkers: 50, TargetHeadroomPct: 20},
			wantAct:  domain.AutoscaleUp,
			wantTo:   2,
			wantReas: "below min_workers",
		},
		{
			name: "above max_workers scales down regardless of slots",
			state: FleetState{
				Workers: []WorkerHeadroom{
					makeHeadroom("w1", "fra", 50),
					makeHeadroom("w2", "fra", 50),
					makeHeadroom("w3", "fra", 50),
					makeHeadroom("w4", "fra", 50),
					makeHeadroom("w5", "fra", 50),
					makeHeadroom("w6", "fra", 50),
				},
				DesiredApps: 1,
			},
			cfg:      Config{MinWorkers: 2, MaxWorkers: 5, TargetHeadroomPct: 20},
			wantAct:  domain.AutoscaleDown,
			wantTo:   5,
			wantReas: "above max_workers",
		},
		{
			name: "free slots shortage scales up by one",
			state: FleetState{
				Workers: []WorkerHeadroom{
					makeHeadroom("w1", "fra", 10),
				},
				DesiredApps: 20, // needed = 20 + 4 = 24; 10 < 24 → scale_up
			},
			cfg:      Config{MinWorkers: 1, MaxWorkers: 50, TargetHeadroomPct: 20},
			wantAct:  domain.AutoscaleUp,
			wantTo:   2,
			wantReas: "free_slots=10 needed=24",
		},
		{
			name: "free slots excess scales down by one",
			state: FleetState{
				Workers: []WorkerHeadroom{
					makeHeadroom("w1", "fra", 200),
					makeHeadroom("w2", "fra", 200),
					makeHeadroom("w3", "fra", 200),
				},
				DesiredApps: 5, // needed = 5 + 1 = 6; > 6*2=12 (200+200+200=600)
			},
			cfg:      Config{MinWorkers: 2, MaxWorkers: 50, TargetHeadroomPct: 20},
			wantAct:  domain.AutoscaleDown,
			wantTo:   2,
			wantReas: "free_slots=600 excess needed=6",
		},
		{
			name: "exactly on target returns noop",
			state: FleetState{
				Workers: []WorkerHeadroom{
					makeHeadroom("w1", "fra", 50),
					makeHeadroom("w2", "fra", 50),
				},
				DesiredApps: 50, // needed = 50 + 10 = 60; total slots = 100 (in band)
			},
			cfg:      Config{MinWorkers: 2, MaxWorkers: 50, TargetHeadroomPct: 20},
			wantAct:  domain.AutoscaleNoop,
			wantTo:   2,
			wantReas: "within target",
		},
		{
			name: "legacy workers assume 50 slots each",
			state: FleetState{
				Workers: []WorkerHeadroom{
					makeHeadroom("w1", "fra", 0), // legacy: nil headroom
					makeHeadroom("w2", "fra", 0), // legacy
				},
				DesiredApps: 30, // 30 + 6 = 36; 50+50=100 (within band, noop)
			},
			cfg:      Config{MinWorkers: 2, MaxWorkers: 50, TargetHeadroomPct: 20},
			wantAct:  domain.AutoscaleNoop,
			wantTo:   2,
			wantReas: "within target",
		},
		{
			name: "mixed legacy + modern sums both correctly",
			state: FleetState{
				Workers: []WorkerHeadroom{
					makeHeadroom("w1", "fra", 0),  // legacy: 50
					makeHeadroom("w2", "fra", 10), // modern: 10
				},
				DesiredApps: 50, // needed = 50 + 10 = 60; 50+10=60 (exactly needed, noop)
			},
			cfg:      Config{MinWorkers: 2, MaxWorkers: 50, TargetHeadroomPct: 20},
			wantAct:  domain.AutoscaleNoop,
			wantTo:   2,
			wantReas: "within target",
		},
		{
			name:     "zero desired apps floors needed to 1 — keeps slot reserved",
			state:    FleetState{Workers: nil, DesiredApps: 0},
			cfg:      Config{MinWorkers: 1, MaxWorkers: 50, TargetHeadroomPct: 20},
			wantAct:  domain.AutoscaleUp,
			wantTo:   1,
			wantReas: "below min_workers",
		},
		{
			name: "at min_workers with zero deployments still noops (slot floor doesn't force scale_up)",
			state: FleetState{
				Workers:     []WorkerHeadroom{makeHeadroom("w1", "fra", 50)},
				DesiredApps: 0,
			},
			cfg:      Config{MinWorkers: 1, MaxWorkers: 50, TargetHeadroomPct: 20},
			wantAct:  domain.AutoscaleNoop,
			wantTo:   1,
			wantReas: "within target",
		},
		{
			name: "scale_up respects max_workers ceiling",
			state: FleetState{
				Workers: []WorkerHeadroom{
					makeHeadroom("w1", "fra", 0),
					makeHeadroom("w2", "fra", 0),
					makeHeadroom("w3", "fra", 0),
					makeHeadroom("w4", "fra", 0),
					makeHeadroom("w5", "fra", 0),
				},
				DesiredApps: 1000, // needed very high; but at max
			},
			cfg:      Config{MinWorkers: 1, MaxWorkers: 5, TargetHeadroomPct: 20},
			wantAct:  domain.AutoscaleNoop,
			wantTo:   5,
			wantReas: "within target",
		},
		{
			name: "scale_down respects min_workers floor",
			state: FleetState{
				Workers: []WorkerHeadroom{
					makeHeadroom("w1", "fra", 1000),
					makeHeadroom("w2", "fra", 1000),
					makeHeadroom("w3", "fra", 1000),
				},
				DesiredApps: 1, // needed = 1; free = 3000 (excess); 3 workers > min=2
			},
			cfg:      Config{MinWorkers: 2, MaxWorkers: 50, TargetHeadroomPct: 20},
			wantAct:  domain.AutoscaleDown,
			wantTo:   2,
			wantReas: "free_slots=3000 excess needed=1",
		},
		{
			name: "2x hysteresis prevents one-slot flapping",
			state: FleetState{
				Workers: []WorkerHeadroom{
					makeHeadroom("w1", "fra", 100),
					makeHeadroom("w2", "fra", 100),
				},
				DesiredApps: 85, // needed = 85 + 17 = 102; 200 not > 102*2=204 → noop
			},
			cfg:      Config{MinWorkers: 1, MaxWorkers: 50, TargetHeadroomPct: 20},
			wantAct:  domain.AutoscaleNoop,
			wantTo:   2,
			wantReas: "within target",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ComputeDecision(tc.state, tc.cfg)
			if got.Action != tc.wantAct {
				t.Errorf("Action = %q, want %q", got.Action, tc.wantAct)
			}
			if got.ToCount != tc.wantTo {
				t.Errorf("ToCount = %d, want %d", got.ToCount, tc.wantTo)
			}
			// Reason contains a substring of the expected message;
			// ComputeDecision embeds live counts in the format string
			// so an exact match would couple tests to internals.
			if !strings.Contains(got.Reason, tc.wantReas) {
				t.Errorf("Reason = %q, want substring %q", got.Reason, tc.wantReas)
			}
			// FromCount must always equal the current worker count.
			if got.FromCount != len(tc.state.Workers) {
				t.Errorf("FromCount = %d, want %d (current workers)", got.FromCount, len(tc.state.Workers))
			}
		})
	}
}

// TestLegacyAssumedAppSlotsIs100 pins issue #641's autoscaler
// contract: legacy (pre-#85 / pre-#641) workers with no ClusterHeadroom
// must be assumed to have 100 free slots — matching the canonical
// PortPool pre-population count post-#641 (PR #657). If this value
// regresses to 50, the autoscaler will under-count capacity and
// scale up sooner than necessary; if it regresses to 200, it will
// over-count and never scale up. Catches accidental regression at
// the constant site.
func TestLegacyAssumedAppSlotsIs100(t *testing.T) {
	if legacyAssumedAppSlots != 100 {
		t.Errorf("legacyAssumedAppSlots = %d, want 100 (issue #641)", legacyAssumedAppSlots)
	}
}

// TestComputeDecision_LegacyWorkerUses100Slots pins the downstream
// effect: a fleet of one legacy-headroom worker (Headroom=nil) must
// be treated as having 100 free slots, not 50 (pre-#641) or 0/200
// (over/under-counting). With DesiredApps=10 and MinWorkers=1,
// totalFreeSlots=100 ≥ needed=11, so the decision must be noop,
// not scale_up.
func TestComputeDecision_LegacyWorkerUses100Slots(t *testing.T) {
	state := FleetState{
		Workers: []WorkerHeadroom{
			{WorkerID: "w_legacy", Region: "fra", Headroom: nil}, // pre-#85 worker
		},
		DesiredApps: 10,
	}
	cfg := Config{MinWorkers: 1, MaxWorkers: 10, TargetHeadroomPct: 10}
	got := ComputeDecision(state, cfg)
	if got.Action != domain.AutoscaleNoop {
		t.Errorf("Action = %q, want %q (legacy worker should report 100 slots, plenty for 10 apps)", got.Action, domain.AutoscaleNoop)
	}
}
