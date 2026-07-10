package domain

import "time"

// BillingProvider is the merchant identifier stamped on every
// billing_subscriptions / billing_events row. The string is exposed in
// URLs and DB rows so operators can tell which provider handled a
// given tenant at a glance.
//
// Adding a new provider means:
//  1. Adding a new ProviderXxx constant below.
//  2. Implementing billing.BillingProvider in
//     internal/billing/<name>/<name>.go.
//  3. Wiring it in internal/app/app.go's newBillingProvider factory.
type BillingProvider string

const (
	// ProviderStripe is the canonical v1 provider (issue #419). Uses
	// stripe-go under internal/billing/stripe/. The string value is
	// stored verbatim in billing_subscriptions.provider — changing it
	// would orphan every existing row.
	ProviderStripe BillingProvider = "stripe"

	// ProviderNoop is the dev/CI/test provider. Always succeeds; never
	// signed; never wired in production (validateBillingConfig in
	// internal/config/config.go fail-closes on app.env != dev|test).
	// Used so integration tests can exercise the billing HTTP surface
	// without real merchant credentials.
	ProviderNoop BillingProvider = "noop"
)

// SubscriptionStatus mirrors the merchant's vocabulary one-to-one.
// We do NOT translate statuses — Stripe's "past_due" / "incomplete" /
// "trialing" all carry semantic meaning that operators recognize from
// the dashboard. Translating them would lose that signal.
type SubscriptionStatus string

const (
	SubscriptionActive     SubscriptionStatus = "active"
	SubscriptionPastDue    SubscriptionStatus = "past_due"
	SubscriptionCanceled   SubscriptionStatus = "canceled"
	SubscriptionIncomplete SubscriptionStatus = "incomplete"
	SubscriptionTrialing   SubscriptionStatus = "trialing"
)

// NormalizedEventType is the merchant-agnostic event class the billing
// service dispatches on. Each BillingProvider implementation maps its
// own event types onto these four. Future providers MUST map onto the
// same set so the service's switch statement stays closed.
type NormalizedEventType string

const (
	EventCheckoutCompleted   NormalizedEventType = "checkout.completed"
	EventSubscriptionUpdated NormalizedEventType = "subscription.updated"
	EventSubscriptionDeleted NormalizedEventType = "subscription.deleted"
	EventPaymentFailed       NormalizedEventType = "payment.failed"
)

// BillingSubscription mirrors the billing_subscriptions row. Read by
// GET /api/v1/billing/subscription (handler returns it directly) and
// by the webhook handler (looks up tenant by provider_customer_id when
// an inbound event doesn't carry tenant).
//
// JSON tags are snake_case to match the OpenAPI schema in
// cmd/api/docs/api/openapi.yaml. The wire shape is documented under
// BillingSubscriptionResponse.
type BillingSubscription struct {
	TenantID               string             `db:"tenant_id"                json:"tenant_id"`
	Provider               BillingProvider    `db:"provider"                 json:"provider"`
	ProviderCustomerID     string             `db:"provider_customer_id"     json:"provider_customer_id,omitempty"`
	ProviderSubscriptionID string             `db:"provider_subscription_id" json:"provider_subscription_id,omitempty"`
	Plan                   string             `db:"plan"                     json:"plan"`
	Status                 SubscriptionStatus `db:"status"                   json:"status"`
	CurrentPeriodEnd       *time.Time         `db:"current_period_end"       json:"current_period_end,omitempty"`
	CancelAtPeriodEnd      bool               `db:"cancel_at_period_end"     json:"cancel_at_period_end"`
	CreatedAt              time.Time          `db:"created_at"               json:"created_at"`
	UpdatedAt              time.Time          `db:"updated_at"               json:"updated_at"`
}

// BillingEvent mirrors the billing_events row. One row per inbound
// webhook delivery; PRIMARY KEY on event_id enforces dedup at the DB
// layer. Stamped received_at on INSERT, processed_at once the handler
// finishes dispatch (NULL = received but not yet processed).
//
// tenant_id is nullable: Stripe's customer.subscription.* events
// don't carry tenant in the payload, so the handler resolves it via
// (provider, provider_customer_id) before persisting. Provider-level
// events (no tenant context) stay NULL.
type BillingEvent struct {
	EventID     string              `db:"event_id"     json:"event_id"`
	Provider    BillingProvider     `db:"provider"     json:"provider"`
	EventType   NormalizedEventType `db:"event_type"   json:"event_type"`
	TenantID    *string             `db:"tenant_id"    json:"tenant_id,omitempty"`
	ReceivedAt  time.Time           `db:"received_at"  json:"received_at"`
	ProcessedAt *time.Time          `db:"processed_at" json:"processed_at,omitempty"`
	PayloadHash string              `db:"payload_hash" json:"payload_hash"`
}

// NormalizedEvent is the merchant-agnostic shape returned by a
// BillingProvider's VerifyWebhook. The service dispatches on
// EventType without ever inspecting provider-specific fields.
//
// TenantID may be empty: implementations that don't embed tenant in
// the event (Stripe) return empty here, and the service fills it from
// billing_subscriptions via (provider, provider_customer_id).
//
// Plan and Status are also optional: implementations return them only
// for events that carry that information (checkout.completed and
// subscription.updated). For payment.failed and subscription.deleted,
// only Status is meaningful.
type NormalizedEvent interface {
	EventID() string
	EventType() NormalizedEventType
	TenantID() string
	Plan() string
	Status() string
	Provider() BillingProvider
}

// MeterKind is the closed set of metered dimensions the platform
// knows about (issue #485). The string values mirror the CHECK
// constraint on billing_usage_events.kind (migration 030) so the
// domain type and the schema stay in lock-step; a new dimension
// requires a migration that extends the CHECK list first.
//
// Symmetry with `BillingProvider`: a typed-domain alias that the
// application code reads/writes while the DB sees the plain string,
// so a future migration adding a fourth dimension only requires
// adding a constant here plus the schema change.
type MeterKind string

const (
	// MeterKindResidentSeconds is the LongRunning-resident-time
	// dimension. The worker stamps it on each heartbeat (issue #484)
	// and the MeteringProvider records it as Stripe metered usage
	// (issue #485). Quantity = wall-clock seconds.
	MeterKindResidentSeconds MeterKind = "resident_seconds"

	// MeterKindRequestCount is the per-request dimension. Summed from
	// every accepted HTTP request across Handler (FaaS) and
	// long-running app entries. Quantity = request count.
	MeterKindRequestCount MeterKind = "request_count"

	// MeterKindOutboundBytes is the outbound-bandwidth dimension.
	// Summed from RequestMeter.outbound_bytes at heartbeat time.
	// Quantity = bytes.
	MeterKindOutboundBytes MeterKind = "outbound_bytes"
)

// MeterUsageEvent mirrors a single billing_usage_events row (issue
// #485, migration 030). One row per (tenant, kind, quantity) batch —
// the heartbeat pipeline writes one row per dimension per
// `dedupe_id`, and the MeteringDrainer reads + dispatches them to the
// configured MeteringProvider.
//
// `Kind` is constrained by the schema's CHECK constraint; the Go type
// enforces the same closed set at compile time. A typed error
// surfaces when the database ever returns an unknown value (it
// shouldn't, but the repository is defensive about future schema
// drifts that pre-empt a code update).
//
// The `db:` tags match the column names verbatim so sqlx can scan
// RETURNING rows directly into this struct. `ProcessedAt` is a
// pointer so the `IS NULL` column reads cleanly without a separate
// nullable wrapper type.
type MeterUsageEvent struct {
	ID             int64      `db:"id"`
	TenantID       string     `db:"tenant_id"`
	Kind           MeterKind  `db:"kind"`
	Quantity       int64      `db:"quantity"`
	IdempotencyKey string     `db:"idempotency_key"`
	RecordedAt     time.Time  `db:"recorded_at"`
	ProcessedAt    *time.Time `db:"processed_at"`
	Provider       string     `db:"provider"`
}
