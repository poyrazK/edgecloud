// Package billing (metering_provider.go) — the merchant-agnostic
// metering seam and the data shape the providers consume.
//
// MeteringProvider is the parallel-of-BillingProvider interface that
// the metering drainer (internal/service/metering_drainer.go) calls.
// The two interfaces stay separate because their lifecycles and
// concurrency models differ:
//
//   - BillingProvider is request/response — CreateCheckoutSession is
//     synchronous on a tenant's HTTP request; VerifyWebhook is one-shot
//     per inbound POST. The BillingService holds a single instance for
//     the process lifetime.
//
//   - MeteringProvider is fire-and-record — RecordUsage is invoked
//     from the drainer goroutine on every claimed row of
//     billing_usage_events. It is safe to call concurrently for the
//     same tenant (Stripe's `IdempotencyKey` makes the duplicate call
//     collapse into one billed event) and the drainer holds the only
//     reference for the drain-tick lifetime.
//
// Adding a new metering provider = a new sub-package implementing
// this interface plus a one-line switch in app.go's newMeteringProvider.
package billing

import (
	"context"
	"errors"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// MeterUsage is the merchant-agnostic record the drainer dispatches
// to the configured MeteringProvider. One MeterUsage per
// billing_usage_events row — the drainer reads the row, maps fields
// onto this struct, and hands it to RecordUsage.
//
// The quantity unit is dimension-agnostic at this seam:
//   - "resident_seconds" → quantity is seconds (uint64)
//   - "request_count"    → quantity is request-count (uint64)
//   - "outbound_bytes"   → quantity is bytes (uint64)
//
// The provider owns the unit translation it needs (Stripe applies
// `usage_multiplier` and converts to its own price item; the noop
// provider ignores quantity entirely).
type MeterUsage struct {
	TenantID       string
	Kind           MeterKind
	Quantity       uint64
	IdempotencyKey string
	RecordedAt     time.Time
}

// MeterKind is the closed set of metered dimensions the platform
// knows about. The string values match the CHECK constraint on
// billing_usage_events.kind (migration 030) — adding a new dimension
// requires a migration that extends the CHECK list first.
type MeterKind = domain.MeterKind

// MeteringProvider is the seam. Implementations:
//
//   - internal/billing/stripe  — production; reports via
//     `subscriptionitem.NewUsageRecord` with `IdempotencyKey`
//     set to the row's idempotency_key.
//   - internal/billing/noop    — dev/CI/test; always succeeds.
//
// RecordUsage is invoked once per claimed billing_usage_events row.
// A non-nil error falls through to MarkFailed in the drainer; a nil
// error falls through to MarkProcessed.
type MeteringProvider interface {
	// Name returns the merchant identifier stamped on every
	// billing_usage_events.provider row (and looked up here at
	// construction time so the drainer can verify it matches
	// expected before dispatch).
	Name() domain.BillingProvider

	// RecordUsage dispatches one usage event to the merchant.
	// Implementations MUST be idempotent under duplicate calls with
	// the same IdempotencyKey (Stripe's `IdempotencyKey` parameter
	// does this; noop trivially returns nil).
	RecordUsage(ctx context.Context, in MeterUsage) error
}

// ErrInvalidKind is returned by RecordUsage on a MeterUsage whose
// Kind is not one of the three metered dimensions. The drainer treats
// this as terminal — a malformed row is not worth retrying.
//
// In practice the drainer never sees this error because the
// billing_usage_events.kind CHECK constraint refuses the bad value at
// INSERT time, but the sentinel exists so provider implementations
// can sanity-check defensively and so a future migration that adds a
// 4th dimension propagates here for testing.
var ErrInvalidKind = errors.New("billing: invalid meter kind")

// ErrTerminal wraps an underlying error to mark it non-retryable.
// Provider implementations return errors wrapping this sentinel when
// the underlying SDK call came back with a 4xx — a 4xx is a permanent
// failure (bad payload, missing meter, expired API key) and retrying
// wastes drainer capacity.
//
// The drainer checks errors.Is(err, ErrTerminal) and skips the
// exponential-backoff / MarkFailed cycle for these. Errors that do
// NOT wrap ErrTerminal are treated as transient (5xx, network, ctx).
var ErrTerminal = errors.New("billing: terminal error")
