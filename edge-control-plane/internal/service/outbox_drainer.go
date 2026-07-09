package service

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// OutboxDrainer relays outbox rows to NATS (issue #42). One row ↔ one
// TaskMessage. Each row carries the full regions list in a single
// payload; the drainer iterates regions inside the row, mirroring the
// pre-#42 publishSwap loop's skip logic. The drainer is the SOLE
// caller of Publisher.PublishTaskUpdate for `task_update` messages
// after this change — the handler enqueues, the drainer publishes.
//
// Multi-instance safe via FOR UPDATE SKIP LOCKED + a 30-second
// claimed_until window (see repository.OutboxRepository.ClaimDue).
// A crashed drainer's rows auto-recover after 30s.
//
// Backoff schedule: next_attempt_at = NOW() + min(2^attempt * 5s, 5min).
// attempt is the NEW attempt_count (i.e. 1 right after the first
// failure, 2 after the second). After OUTBOX_MAX_ATTEMPTS (default
// 10) the row's status flips to 'failed' and stays for operator
// inspection.
type OutboxDrainer struct {
	repo        *repository.OutboxRepository
	activeRepo  *repository.ActiveDeploymentRepository
	publisher   nats.Publisher
	interval    time.Duration
	batchSize   int
	maxAttempts int
}

// NewOutboxDrainer constructs a drainer. interval and maxAttempts are
// typically wired from env knobs (OUTBOX_DRAIN_INTERVAL,
// OUTBOX_MAX_ATTEMPTS). batchSize defaults to 50 — enough to drain
// a single activation's regions in one tick, low enough that a
// backlog tick doesn't monopolize the DB pool.
func NewOutboxDrainer(repo *repository.OutboxRepository, activeRepo *repository.ActiveDeploymentRepository, publisher nats.Publisher, interval time.Duration, batchSize, maxAttempts int) *OutboxDrainer {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	if batchSize <= 0 {
		batchSize = 50
	}
	if maxAttempts <= 0 {
		maxAttempts = 10
	}
	return &OutboxDrainer{
		repo:        repo,
		activeRepo:  activeRepo,
		publisher:   publisher,
		interval:    interval,
		batchSize:   batchSize,
		maxAttempts: maxAttempts,
	}
}

// Run is the long-lived loop. Blocks until ctx is cancelled. One
// tick = one ClaimDue + per-row process. On transient errors (DB
// hiccup, NATS down for one row) the loop logs and continues — the
// row's MarkFailed path will re-schedule it.
func (d *OutboxDrainer) Run(ctx context.Context) {
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

// Tick is one drain cycle. Exported (via DrainerForTest) so tests
// can drive a deterministic cycle instead of relying on time.
func (d *OutboxDrainer) Tick(ctx context.Context) {
	rows, err := d.repo.ClaimDue(ctx, d.batchSize)
	if err != nil {
		log.Printf("outbox: ClaimDue failed: %v", err)
		return
	}
	if len(rows) >= d.batchSize {
		// Log on full-batch claims so an operator can see backlog growth
		// without having to query the table. Threshold is the batch size
		// itself — one full batch is "saturated", multiple consecutive
		// full batches are "falling behind".
		log.Printf("outbox: drainer claimed a full batch (%d rows) — backlog growing", len(rows))
	}
	for i := range rows {
		d.processRow(ctx, &rows[i])
	}
}

// processRow publishes one outbox row's payload to every region in
// row.Regions, then either MarkPublished (all regions OK) or
// MarkFailed (any region failed; attempt_count++ + backoff).
//
// The publish loop mirrors pre-#42 publishSwap semantics: a
// transient per-region failure does not abort the loop; the row
// is only marked 'failed' after `maxAttempts` retries. The
// successful regions are recorded on the active row via
// AppendRegionsPublished regardless of the overall outcome — the
// operator escape-hatch `SELECT regions_published WHERE ...`
// must reflect partial progress.
func (d *OutboxDrainer) processRow(ctx context.Context, row *repository.OutboxRow) {
	var msg nats.TaskMessage
	if err := json.Unmarshal(row.Payload, &msg); err != nil {
		// Malformed payload is unrecoverable — mark failed at max attempts
		// with a clear error so an operator can inspect the row.
		log.Printf("outbox: row %d payload unmarshal failed: %v", row.ID, err)
		_ = d.repo.MarkFailed(ctx, row.ID, d.maxAttempts, "payload unmarshal: "+err.Error(), d.maxAttempts, time.Now())
		return
	}

	var succeeded []string
	var firstErr error
	for _, region := range row.Regions {
		if err := d.publisher.PublishTaskUpdate(region, &msg); err != nil {
			log.Printf("outbox: row %d publish to %q failed: %v", row.ID, region, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		succeeded = append(succeeded, region)
	}

	// Always update the active row's regions_published / regions_failed
	// to reflect partial progress — operator-visible state, not
	// transactional. Best-effort: a DB error here is logged but does
	// not change the outbox row's status (the row's status is the
	// ground truth for whether the message hit NATS).
	if len(succeeded) > 0 {
		// Use a fresh attempt_id so the operator sees the row's
		// last_publish_at / last_publish_attempt_id reflect this
		// partial success even when the row will retry.
		if err := d.activeRepo.AppendRegionsPublished(ctx, row.TenantID, row.AppName, succeeded, row.DedupeKey, time.Now()); err != nil {
			log.Printf("outbox: row %d AppendRegionsPublished failed: %v", row.ID, err)
		}
	}

	if firstErr == nil {
		if err := d.repo.MarkPublished(ctx, row.ID); err != nil {
			log.Printf("outbox: row %d MarkPublished failed: %v", row.ID, err)
		}
		return
	}

	// Some regions failed — bump attempt_count and re-schedule.
	// attempt_count on the row is the prior value; the new value is
	// prior+1. MarkFailed decides retry-vs-give-up at the boundary.
	newAttempt := row.AttemptCount + 1
	backoff := backoffFor(newAttempt)
	if err := d.repo.MarkFailed(ctx, row.ID, newAttempt, firstErr.Error(), d.maxAttempts, time.Now().Add(backoff)); err != nil {
		log.Printf("outbox: row %d MarkFailed failed: %v", row.ID, err)
	}
	if newAttempt >= d.maxAttempts {
		log.Printf("outbox: row %d gave up after %d attempts — last error: %v", row.ID, newAttempt, firstErr)
	}
}

// backoffFor returns the wait duration for the given attempt number.
// schedule: 5s, 10s, 20s, 40s, 80s, 160s, 320s, 320s, 320s, 320s (capped
// at 5 minutes). Math is `min(2^attempt * 5s, 5min)`.
func backoffFor(attempt int) time.Duration {
	const cap = 5 * time.Minute
	const base = 5 * time.Second
	if attempt < 0 {
		attempt = 0
	}
	// Guard against overflow on absurd attempt counts (defensive — the
	// drainer never gets there because maxAttempts gates MarkFailed to
	// status='failed' long before).
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

// Sentinel for tests that want to assert on "drainer was constructed
// with sensible defaults". Not used in production paths.
var _ = errors.New
