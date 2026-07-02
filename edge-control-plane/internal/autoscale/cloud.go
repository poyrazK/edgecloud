package autoscale

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
)

// CloudProvider is the pluggable surface through which the autoscaler
// adds or removes workers in a region. Implementations:
//
//   - NoopCloudProvider   — logs only; default for dev. The operator's
//     external system (k8s HPA, sidecar that scrapes the WARN log line,
//     Terraform automation) is the real provisioner.
//   - MockCloudProvider   — function-field mock; service tests.
//   - HetznerCloudProvider — separate future PR (issue #85 follow-up).
//
// The autoscaler records every Provision/Deprovision call as an
// `autoscale_events` row (success or failure) so the admin endpoint
// can answer "what would have happened in dev?" via the Noop provider.
type CloudProvider interface {
	// Kind returns the provider identifier used in the
	// `autoscale_events.provider_kind` column. Must be stable across
	// restarts — operators index on this.
	Kind() string
	// Provision requests a new worker in `region`. Returns:
	//   - (workerID, nil) on success — workerID is opaque to the
	//     autoscaler (the cloud-provider's own naming convention).
	//   - ("", nil) when the provider chose to no-op (e.g., the Noop
	//     provider). The autoscaler records this as a `noop` event.
	//   - ("", err) on failure. The autoscaler records a
	//     `succeeded=false` event and continues to the next decision
	//     tick — the failure doesn't halt the autoscaler.
	Provision(ctx context.Context, region string) (workerID string, err error)
	// Deprovision removes `workerID` from `region`. Same error
	// semantics: a failed deprovision is recorded but does not stop
	// the autoscaler from making future decisions.
	Deprovision(ctx context.Context, region string, workerID string) error
}

// NoopCloudProvider is the default provider for dev. It does not
// provision workers; instead it logs a WARN line so an external
// scraper (k8s HPA watching the control plane's stderr; a
// Terraform-driven controller) can react. The autoscaler records the
// call as a `noop` event with succeeded=true so operators see the
// intent in `GET /api/admin/cluster`.
type NoopCloudProvider struct {
	log *slog.Logger
}

// NewNoopCloudProvider returns a NoopCloudProvider that logs through
// `log`. Pass the same `*slog.Logger` the rest of the control plane
// uses so the WARN lines land in the operator's existing log stream.
func NewNoopCloudProvider(log *slog.Logger) *NoopCloudProvider {
	if log == nil {
		log = slog.Default()
	}
	return &NoopCloudProvider{log: log}
}

// Kind returns "noop" — the value persisted to `autoscale_events.provider_kind`.
func (n *NoopCloudProvider) Kind() string { return "noop" }

// Provision logs a WARN line and returns ("", nil) so the autoscaler
// records a successful noop event. We deliberately do NOT block or
// call out to any external API — that would couple the control plane
// to a specific cloud SDK.
func (n *NoopCloudProvider) Provision(_ context.Context, region string) (string, error) {
	n.log.Warn(
		"autoscale: noop provision requested (no worker will be created — wire an external provisioner)",
		"region", region,
	)
	return "", nil
}

// Deprovision logs a WARN line and returns nil — the autoscaler
// records a successful noop event.
func (n *NoopCloudProvider) Deprovision(_ context.Context, region, workerID string) error {
	n.log.Warn(
		"autoscale: noop deprovision requested (no worker will be removed — wire an external provisioner)",
		"region", region,
		"worker_id", workerID,
	)
	return nil
}

// MockCloudProvider is a function-field mock for service tests.
// Each method delegates to its function field; unset fields return
// zero values. Mirrors the pattern at
// `internal/service/worker_test.go:14-25`.
//
// ProvisionCount / DeprovisionCount are atomic counters incremented
// after the corresponding function-field call returns. Tests that
// need to wait for activity can poll these directly via
// ProvisionCalls() / DeprovisionCalls() without resorting to a
// package-level atomic.
type MockCloudProvider struct {
	KindFunc        func() string
	ProvisionFunc   func(ctx context.Context, region string) (string, error)
	DeprovisionFunc func(ctx context.Context, region, workerID string) error

	provisionCount   atomic.Int64
	deprovisionCount atomic.Int64
}

func (m *MockCloudProvider) Kind() string {
	if m.KindFunc != nil {
		return m.KindFunc()
	}
	return "mock"
}

func (m *MockCloudProvider) Provision(ctx context.Context, region string) (string, error) {
	var (
		id  string
		err error
	)
	if m.ProvisionFunc != nil {
		id, err = m.ProvisionFunc(ctx, region)
	}
	m.provisionCount.Add(1)
	return id, err
}

func (m *MockCloudProvider) Deprovision(ctx context.Context, region, workerID string) error {
	var err error
	if m.DeprovisionFunc != nil {
		err = m.DeprovisionFunc(ctx, region, workerID)
	}
	m.deprovisionCount.Add(1)
	return err
}

// ProvisionCalls returns the number of times Provision has been
// invoked. Safe to call from any goroutine.
func (m *MockCloudProvider) ProvisionCalls() int64 {
	return m.provisionCount.Load()
}

// DeprovisionCalls returns the number of times Deprovision has been
// invoked. Safe to call from any goroutine.
func (m *MockCloudProvider) DeprovisionCalls() int64 {
	return m.deprovisionCount.Load()
}

// ResetCounters zeroes the Provision / Deprovision counters. Useful
// when reusing a single MockCloudProvider across subtests.
func (m *MockCloudProvider) ResetCounters() {
	m.provisionCount.Store(0)
	m.deprovisionCount.Store(0)
}

// NewCloudProvider returns a CloudProvider based on `kind`. Today
// only "noop" is supported; an unknown kind returns an error so the
// autoscaler refuses to start with a typo'd config (or with
// "mock", which was an early dev placeholder that never wired up to
// anything — tests construct MockCloudProvider directly).
//
// Future kinds (hetzner, aws, gcp) land in separate follow-up PRs
// and add a `case` here.
func NewCloudProvider(kind string, log *slog.Logger) (CloudProvider, error) {
	switch kind {
	case "", "noop":
		return NewNoopCloudProvider(log), nil
	default:
		return nil, fmt.Errorf("autoscale: unknown provider_kind %q (supported: noop)", kind)
	}
}
