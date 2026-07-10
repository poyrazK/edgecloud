package service

import (
	"context"
	"errors"
	"log"
	"math"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/billing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// BillingUsageClaimer is the minimal repo surface the drainer uses.
// Defined here so tests can stand in a fake without spinning up
// sqlmock; the production code path passes a
// *repository.BillingUsageRepository which satisfies the interface
// via structural typing (Go).
type BillingUsageClaimer interface {
	ClaimDue(ctx context.Context, limit int) ([]repository.BillingUsageEventWithID, error)
	MarkProcessed(ctx context.Context, id int64) error
}

// MeteringDrainer dispatches billing_usage_events rows to the
// configured MeteringProvider (issue #485). One row ↔ one RecordUsage
// call. Multi-instance safe via FOR UPDATE SKIP LOCKED in
// BillingUsageRepository.ClaimDue.
//
// Lifecycle: Run blocks until ctx cancels. Each tick claims a batch,
// iterates the rows, calls provider.RecordUsage, and either
// MarkProcessed (success or terminal) or leaves the row alone
// (transient; next tick re-claims). A terminal error from the
// provider (wraps billing.ErrTerminal) routes to
// MarkProcessed-with-warn so the row stops cycling — operators must
// fix the underlying config (missing subscription_item_id, bad
// Stripe API key, etc.) and the next heartbeat re-enqueues.
//
// Backoff schedule mirrors OutboxDrainer (5s base, 5min cap). The
// drainer is the SOLE caller of MeteringProvider.RecordUsage; the
// heartbeat pipeline only writes to billing_usage_events.
type MeteringDrainer struct {
	repo        BillingUsageClaimer
	provider    billing.MeteringProvider
	interval    time.Duration
	batchSize   int
	maxAttempts int
	// rates is an optional short-circuit map. A non-empty map is
	// read at construction time and stored as a snapshot; the
	// drainer looks up rates[kind] and skips RecordUsage when the
	// value is zero (or the entry is missing entirely). The
	// snapshot is the (rate, kind)-shaped subset of
	// BillingMeteringConfig.Rates the drainer owns at startup; an
	// operator who flips rates at runtime must restart the process
	// to pick up the new values — same posture as the Stripe API
	// key. This is intentional: rate changes mid-tick are
	// confusing to reason about, and the cost of a restart is low.
	rates map[string]float64
}

// NewMeteringDrainer constructs a drainer. interval / batchSize /
// maxAttempts wire from env (METERING_DRAIN_INTERVAL / BATCH_SIZE /
// MAX_ATTEMPTS) in app.NewApp. rates may be nil — the drainer
// short-circuits when rates[kind] is zero OR when the entry is
// absent; a nil map behaves identically to an empty one.
func NewMeteringDrainer(
	repo BillingUsageClaimer,
	provider billing.MeteringProvider,
	interval time.Duration,
	batchSize, maxAttempts int,
	rates map[string]float64,
) *MeteringDrainer {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if batchSize <= 0 {
		batchSize = 50
	}
	if maxAttempts <= 0 {
		maxAttempts = 10
	}
	if rates == nil {
		rates = map[string]float64{}
	}
	return &MeteringDrainer{
		repo:        repo,
		provider:    provider,
		interval:    interval,
		batchSize:   batchSize,
		maxAttempts: maxAttempts,
		rates:       rates,
	}
}

// Run is the long-lived loop. Blocks until ctx is cancelled. On
// transient errors the loop logs and continues — the row's
// MarkFailed path will re-schedule it.
func (d *MeteringDrainer) Run(ctx context.Context) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.Tick(ctx)
		}
	}
}

// Tick is one drain cycle. Exported so tests can drive a deterministic
// cycle instead of relying on time.
func (d *MeteringDrainer) Tick(ctx context.Context) {
	rows, err := d.repo.ClaimDue(ctx, d.batchSize)
	if err != nil {
		log.Printf("metering: ClaimDue failed: %v", err)
		return
	}
	if len(rows) >= d.batchSize {
		log.Printf("metering: drainer claimed a full batch (%d rows) — backlog growing", len(rows))
	}
	for i := range rows {
		d.processRow(ctx, &rows[i])
	}
}

// processRow dispatches one billing_usage_events row to the
// configured MeteringProvider. The flow:
//
//  1. Check the rate card: rate[kind] == 0 (or entry missing) →
//     MarkProcessed-and-return. This is the billing-neutral
//     rollout path (zero-rate default — consumption allowed but
//     not billed).
//
//  2. Otherwise call provider.RecordUsage with the merchant-agnostic
//     MeterUsage shape. The provider owns the SDK call + error
//     translation (4xx → wraps ErrTerminal; 5xx → transient).
//
//  3. nil → MarkProcessed.
//     wraps-billing.ErrTerminal → MarkProcessed-and-log (the row
//     stops cycling; a config fix + next heartbeat re-enqueues).
//     Transient → leave the row alone (processed_at IS NULL); the
//     next tick re-claims it. Stripe's Idempotency-Key makes the
//     retry safe — a duplicate call with the same Identifier
//     dedupes inside Stripe's rolling 24h window.
//
// We don't carry a last_error / attempt_count column on the v1
// billing_usage_events schema (migration 030) because metering is
// best-effort: a sustained Stripe outage will show up via the
// `metering_drainer` log stream and the
// `processed_at IS NULL` row count, not via per-row state. If
// operators want explicit per-row last_error, a follow-up
// migration (031) can add the columns; the drainer's surface
// doesn't need to change.
func (d *MeteringDrainer) processRow(ctx context.Context, row *repository.BillingUsageEventWithID) {
	// Zero-rate short-circuit. We treat both "rate is zero" AND
	// "rate entry missing" as zero — the plan's locked design is
	// "consumption allowed but not billed", and the missing-entry
	// case is the default-empty-map case on a fresh install.
	if rate, ok := d.rates[string(row.Kind)]; !ok || rate == 0 {
		// Mark the row processed so it stops cycling. We don't
		// stamp last_error because this is the expected happy
		// path on a zero-rate install — flooding logs with
		// "skipping because rate=0" would bury real signals.
		if err := d.repo.MarkProcessed(ctx, row.ID); err != nil {
			log.Printf("metering: row %d zero-rate MarkProcessed failed: %v", row.ID, err)
		}
		return
	}

	err := d.provider.RecordUsage(ctx, billing.MeterUsage{
		TenantID:       row.TenantID,
		Kind:           row.Kind,
		Quantity:       uint64(row.Quantity),
		IdempotencyKey: row.IdempotencyKey,
		RecordedAt:     row.RecordedAt,
	})
	if err == nil {
		if err := d.repo.MarkProcessed(ctx, row.ID); err != nil {
			log.Printf("metering: row %d MarkProcessed failed: %v", row.ID, err)
		}
		return
	}

	// Terminal: 4xx, missing config, etc. Mark processed (so the row
	// stops cycling) and log so operators can see why. A config
	// fix + the next heartbeat re-enqueues a new row — the failed
	// row is the operator's inspection surface via
	// `SELECT * FROM billing_usage_events WHERE processed_at IS NULL`.
	if errors.Is(err, billing.ErrTerminal) {
		log.Printf("metering: row %d (tenant=%s kind=%s) terminal: %v — marking processed",
			row.ID, row.TenantID, row.Kind, err)
		if mErr := d.repo.MarkProcessed(ctx, row.ID); mErr != nil {
			log.Printf("metering: row %d MarkProcessed failed: %v", row.ID, mErr)
		}
		return
	}

	// Transient: 5xx, network, ctx. Leave the row alone — the
	// next tick (interval later, default 30s) re-claims it. Stripe's
	// Idempotency-Key makes the retry safe.
	log.Printf("metering: row %d (tenant=%s kind=%s) transient: %v — will retry next tick",
		row.ID, row.TenantID, row.Kind, err)
}

// backoffForMetering is unused now (we don't carry a per-row
// attempt_count column on the v1 schema) but kept as the canonical
// home for the backoff schedule so a future migration that adds
// last_error_at / next_attempt_at can drop it in without re-
// reasoning. Mirrors outbox_drainer.backoffFor byte-for-byte so the
// "exhaustion" dashboards (issue #581's `given_up_total`) carry the
// same meaning across both drains.
//
// Cap is hit at attempt=6 (2^6 * 5s = 320s > 300s) and stays flat
// after that.
func backoffForMetering(attempt int) time.Duration {
	const cap = 5 * time.Minute
	const base = 5 * time.Second
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 30 {
		return cap
	}
	mul := math.Pow(2, float64(attempt))
	d := time.Duration(mul) * base
	if d > cap || d < 0 {
		return cap
	}
	return d
}
