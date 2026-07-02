package domain

import "time"

// AutoscaleAction is the closed set of decisions the autoscaler can make.
// Mapped 1:1 to the `action` CHECK constraint on the `autoscale_events`
// table (migration 012_autoscale_events.up.sql). Adding a new variant
// here means adding it to the CHECK constraint AND updating the
// ComputeDecision switch in autoscale/decision.go.
type AutoscaleAction string

const (
	// AutoscaleUp requests provisioning a new worker in the region.
	AutoscaleUp AutoscaleAction = "scale_up"
	// AutoscaleDown requests deprovisioning the least-recently-heartbeated
	// worker in the region.
	AutoscaleDown AutoscaleAction = "scale_down"
	// AutoscaleNoop records a decision-tick that resulted in no action —
	// either the fleet is within target, or a cooldown gate suppressed
	// what would otherwise be a scale_up/scale_down. The reason field
	// disambiguates ("within target" vs "scale_up cooldown").
	AutoscaleNoop AutoscaleAction = "noop"
)

// AutoscaleEvent is one row in the `autoscale_events` table. Persisted
// by the autoscaler on every decision tick (including noops) so the
// cluster admin endpoint and operator dashboards can answer "why is
// the fleet this size right now?"
type AutoscaleEvent struct {
	ID           int64           `db:"id"`
	CreatedAt    time.Time       `db:"created_at"`
	Region       string          `db:"region"`
	Action       AutoscaleAction `db:"action"`
	FromCount    int             `db:"from_count"`
	ToCount      int             `db:"to_count"`
	Reason       string          `db:"reason"`
	ProviderKind string          `db:"provider_kind"`
	Succeeded    bool            `db:"succeeded"`
	// ErrorMessage is nil when Succeeded is true; non-nil pointer to
	// the cloud-provider error string when Succeeded is false. The
	// pointer-to-string shape matches the rest of the codebase's
	// nullable text columns (see Worker.IP).
	ErrorMessage *string `db:"error_message"`
}
