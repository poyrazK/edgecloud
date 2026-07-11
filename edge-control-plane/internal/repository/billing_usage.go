package repository

import (
	"context"
	"errors"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// BillingUsageEventWithID is the subset of MeterUsageEvent the
// drainer needs to identify a claimed row + dispatch it through
// MeteringProvider.RecordUsage. The integer id is what MarkProcessed
// keys off; the rest mirrors domain.MeterUsageEvent one-to-one so a
// sqlx RETURNING scan can populate both halves in one round trip.
//
// The flat (non-nested) shape is intentional: sqlx does not honor
// `db:""` prefixes to flatten nested structs unless you pass the
// `db.WithInstance(db.Unsafe())` flag. We avoid the unsafe path
// here — every column has an explicit `db:` tag and the SELECT/RETURNING
// projection lines up byte-for-byte.
type BillingUsageEventWithID struct {
	ID             int64            `db:"id"`
	TenantID       string           `db:"tenant_id"`
	Kind           domain.MeterKind `db:"kind"`
	Quantity       int64            `db:"quantity"`
	IdempotencyKey string           `db:"idempotency_key"`
	RecordedAt     time.Time        `db:"recorded_at"`
	ProcessedAt    *time.Time       `db:"processed_at"`
	Provider       string           `db:"provider"`
}

// BillingUsageRepository handles the metering ledger (issue #485,
// migration 030). All methods run against either the shared *sqlx.DB
// (drainer) or the tx-scoped *sqlx.Tx (heartbeat-path enqueue).
//
// The shape mirrors OutboxRepository because the operator mental
// model is identical: durable queue → background drainer → terminal
// success OR retry-with-backoff → eventual give-up. Engineers learn
// the pattern once.
type BillingUsageRepository struct {
	db DBTX
}

// NewBillingUsageRepository returns a repository bound to the shared
// DB. Used by the drainer.
func NewBillingUsageRepository(db *sqlx.DB) *BillingUsageRepository {
	return &BillingUsageRepository{db: db}
}

// WithTx returns a new repository bound to the provided tx. Used by
// the heartbeat-side enqueue path so the metering ledger write joins
// the same transaction as the active_deployments mutation (when the
// caller elects to do so — for now we dual-write outside any tx and
// the row is independent).
func (r *BillingUsageRepository) WithTx(tx *sqlx.Tx) *BillingUsageRepository {
	return &BillingUsageRepository{db: tx}
}

// Enqueue writes a new ledger row. Idempotent on
// (tenant_id, idempotency_key) — a redelivered heartbeat with the
// same dedupe_id collapses to one row via the UNIQUE constraint.
//
// Returns ErrDuplicateIdempotencyKey on conflict; the heartbeat
// pipeline treats this as the normal "already recorded" path (drops
// the duplicate silently). The drainer never sees this error — the
// drainer only reads rows it has not yet claimed.
func (r *BillingUsageRepository) Enqueue(ctx context.Context, row *domain.MeterUsageEvent) error {
	const query = `
		INSERT INTO billing_usage_events (
			tenant_id, kind, quantity, idempotency_key, provider
		) VALUES ($1, $2, $3, $4, $5)
	`
	_, err := r.db.ExecContext(ctx, query,
		row.TenantID, string(row.Kind), row.Quantity, row.IdempotencyKey, row.Provider)
	if err != nil && isUniqueViolation(err) {
		return ErrDuplicateIdempotencyKey
	}
	return err
}

// EnqueueUsageEvent is the heartbeat-pipeline-friendly form of
// Enqueue. The caller passes the per-app fields; the method stamps
// provider="" so the row waits for the drainer to decide which
// merchant receives it (one row may be dispatched through stripe on
// the next tick and the operator's choice of merchant at that
// point). Idempotency_key format is "<dedupe_id>:<kind>" — the
// dedupe_id is the AppStatus.DedupeID stamped by the worker (issue
// #418); the kind suffix distinguishes the rows within one
// heartbeat that contribute to multiple dimensions.
//
// Returns nil on ErrDuplicateIdempotencyKey (silent dedup), since
// the heartbeat pipeline is the right place to absorb the
// duplicate — a redelivered AppStatus should not error out.
func (r *BillingUsageRepository) EnqueueUsageEvent(
	ctx context.Context,
	tenantID string,
	kind domain.MeterKind,
	quantity uint64,
	idempotencyKey string,
) error {
	err := r.Enqueue(ctx, &domain.MeterUsageEvent{
		TenantID:       tenantID,
		Kind:           kind,
		Quantity:       int64(quantity),
		IdempotencyKey: idempotencyKey,
		Provider:       "", // drainer fills at dispatch time
	})
	if errors.Is(err, ErrDuplicateIdempotencyKey) {
		return nil
	}
	return err
}

// ClaimDue atomically transitions up to `limit` unprocessed rows
// to processed_at = NOW() and returns them. Uses the partial
// index idx_billing_usage_events_unprocessed (migration 030:85-87)
// to keep the WHERE selective; the UPDATE…FROM…RETURNING shape
// mirrors OutboxRepository (issue #42) so concurrent drainers
// racing on the same set each claim a disjoint subset — the
// FOR UPDATE SKIP LOCKED lives on the inner SELECT.
//
// Rows are returned in recorded_at ASC order (oldest first), so
// the drainer is fair across tenants: a backlog from one tenant
// doesn't starve a backlog from another.
func (r *BillingUsageRepository) ClaimDue(ctx context.Context, limit int) ([]BillingUsageEventWithID, error) {
	const query = `
		WITH due AS (
			SELECT id FROM billing_usage_events
			WHERE processed_at IS NULL
			ORDER BY recorded_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE billing_usage_events bue
		SET processed_at = NOW()
		FROM due
		WHERE bue.id = due.id
		RETURNING bue.id, bue.tenant_id, bue.kind, bue.quantity, bue.idempotency_key,
		          bue.recorded_at, bue.processed_at, bue.provider
	`
	rows := []BillingUsageEventWithID{}
	if err := r.db.SelectContext(ctx, &rows, query, limit); err != nil {
		return nil, err
	}
	return rows, nil
}

// MarkProcessed stamps processed_at on a successfully dispatched
// row. The drainer calls this after MeteringProvider.RecordUsage
// returns nil.
//
// The row stays in the table forever (audit-retention posture, same
// as billing_events); processed_at IS NOT NULL is the only "done"
// marker. There is no MarkUnprocessed / reversal method.
//
// The WHERE clause guards against re-marking a row that was already
// processed (defensive — the drainer should never call this twice
// for the same id, but operator-level SQL poking must be safe).
func (r *BillingUsageRepository) MarkProcessed(ctx context.Context, id int64) error {
	const query = `
		UPDATE billing_usage_events
		SET processed_at = NOW()
		WHERE id = $1 AND processed_at IS NOT NULL
	`
	_, err := r.db.ExecContext(ctx, query, id)
	return err
}

// ErrDuplicateIdempotencyKey is returned by Enqueue when the UNIQUE
// constraint on (tenant_id, idempotency_key) fires. The heartbeat
// pipeline treats this as "already recorded" and drops the
// duplicate silently — a normal redelivery behavior.
var ErrDuplicateIdempotencyKey = errors.New("billing_usage: duplicate idempotency_key")
