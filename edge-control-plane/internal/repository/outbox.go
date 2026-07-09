package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// OutboxRow is one durable-publish row (issue #42). The payload is the
// already-marshaled NATS TaskMessage — the drainer reads it verbatim
// and unmarshals into a *nats.TaskMessage at publish time. Keeping the
// on-wire bytes in the row (rather than the typed struct) lets the
// payload schema evolve independently of the outbox schema: a new
// TaskMessage field shows up on the wire without a code change here.
type OutboxRow struct {
	ID            int64          `db:"id"`
	TenantID      string         `db:"tenant_id"`
	AppName       string         `db:"app_name"`
	Kind          string         `db:"kind"`
	Payload       []byte         `db:"payload"`
	Regions       pq.StringArray `db:"regions"`
	AttemptCount  int            `db:"attempt_count"`
	NextAttemptAt time.Time      `db:"next_attempt_at"`
	Status        string         `db:"status"`
	LastError     sql.NullString `db:"last_error"`
	DedupeKey     string         `db:"dedupe_key"`
	CreatedAt     time.Time      `db:"created_at"`
	PublishedAt   sql.NullTime   `db:"published_at"`
	ClaimedUntil  sql.NullTime   `db:"claimed_until"`
}

// OutboxRepository handles the durable-publish queue. All methods run
// against either the shared *sqlx.DB (drainer) or the tx-scoped
// *sqlx.Tx (handler-side enqueue).
type OutboxRepository struct {
	db DBTX
}

func NewOutboxRepository(db *sqlx.DB) *OutboxRepository {
	return &OutboxRepository{db: db}
}

// WithTx returns a new OutboxRepository bound to the provided tx.
// Used by DeploymentService.activateDeployment to enqueue the row
// inside the same transaction as the active_deployments mutation.
func (r *OutboxRepository) WithTx(tx *sqlx.Tx) *OutboxRepository {
	return &OutboxRepository{db: tx}
}

// Enqueue writes a new outbox row. Always invoked inside the caller's
// tx (the row is written atomically with the active_deployments
// mutation it accompanies — see DeploymentService.activateDeployment
// at internal/service/deployment.go).
//
// Returns ErrDuplicateDedupeKey if the UNIQUE(dedupe_key) constraint
// fires. The dedupe key is `<tenant>:<app>:<attempt_id>` where
// attempt_id is a fresh UUID per enqueue, so a constraint violation
// only happens on a buggy double-enqueue. The drainer separately
// guards against double-publish via the status='in_flight' claim.
func (r *OutboxRepository) Enqueue(ctx context.Context, row *OutboxRow) error {
	const query = `
		INSERT INTO outbox (
			tenant_id, app_name, kind, payload, regions, dedupe_key
		) VALUES ($1, $2, $3, $4, $5, $6)
	`
	regions := pq.StringArray{}
	if len(row.Regions) > 0 {
		regions = row.Regions
	}
	_, err := r.db.ExecContext(ctx, query,
		row.TenantID, row.AppName, row.Kind, row.Payload, regions, row.DedupeKey)
	if err != nil && isUniqueViolation(err) {
		return ErrDuplicateDedupeKey
	}
	return err
}

// ClaimDue atomically transitions up to `limit` rows from pending → in_flight
// and returns them. The CTE pattern + FOR UPDATE SKIP LOCKED makes
// multi-instance draining safe: two drainers racing on the same set
// of pending rows each claim a disjoint subset.
//
// `claimed_until` is set to NOW() + 30s as a forward-compatible
// safety net — if the drainer crashes after claiming a row but
// before MarkPublished/MarkFailed, the row auto-recovers (the next
// ClaimDue sees status='in_flight' AND claimed_until <= NOW() and
// can re-claim it). Single-instance deployments don't strictly need
// this, but the cost is one extra column + one extra predicate in
// the WHERE clause — cheap insurance for a future scale-out.
//
// pending rows that fail the `next_attempt_at <= NOW()` check are
// skipped (no row lock, no claim). Future rows stay pending.
func (r *OutboxRepository) ClaimDue(ctx context.Context, limit int) ([]OutboxRow, error) {
	const query = `
		WITH due AS (
			SELECT id FROM outbox
			WHERE status = 'pending'
			  AND next_attempt_at <= NOW()
			ORDER BY next_attempt_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox o
		SET status = 'in_flight',
		    claimed_until = NOW() + INTERVAL '30 seconds'
		FROM due
		WHERE o.id = due.id
		RETURNING o.id, o.tenant_id, o.app_name, o.kind, o.payload, o.regions,
		          o.attempt_count, o.next_attempt_at, o.status, o.last_error,
		          o.dedupe_key, o.created_at, o.published_at, o.claimed_until
	`
	var rows []OutboxRow
	if err := r.db.SelectContext(ctx, &rows, query, limit); err != nil {
		return nil, err
	}
	return rows, nil
}

// MarkPublished flips a claimed row to status='published' and stamps
// published_at = NOW(). Clears last_error so a successful retry doesn't
// leave stale failure noise on the row.
//
// Called from the drainer after every PublishTaskUpdate in the row's
// regions list returns nil.
func (r *OutboxRepository) MarkPublished(ctx context.Context, id int64) error {
	const query = `
		UPDATE outbox
		SET status = 'published',
		    published_at = NOW(),
		    last_error = NULL,
		    claimed_until = NULL
		WHERE id = $1
	`
	_, err := r.db.ExecContext(ctx, query, id)
	return err
}

// MarkFailed records a failed publish attempt and re-schedules the
// row. Behavior depends on attemptCount vs maxAttempts:
//
//   - attemptCount < maxAttempts: status stays 'pending' (so the next
//     ClaimDue picks it up after next_attempt_at elapses);
//     attempt_count is incremented; next_attempt_at is the caller-
//     supplied backoff; last_error is the failure message.
//   - attemptCount >= maxAttempts: status flips to 'failed' (terminal);
//     same column updates; the row stays for operator inspection and
//     is excluded from ClaimDue forever after.
//
// The terminal-vs-retry decision is computed here rather than in the
// caller so the repository owns the state-machine semantics.
func (r *OutboxRepository) MarkFailed(ctx context.Context, id int64, attemptCount int, errMsg string, maxAttempts int, nextAttemptAt time.Time) error {
	status := "pending"
	if attemptCount >= maxAttempts {
		status = "failed"
	}
	const query = `
		UPDATE outbox
		SET status = $2,
		    attempt_count = $3,
		    last_error = $4,
		    next_attempt_at = $5,
		    claimed_until = NULL
		WHERE id = $1
	`
	_, err := r.db.ExecContext(ctx, query, id, status, attemptCount, errMsg, nextAttemptAt)
	return err
}

// ErrDuplicateDedupeKey is returned by Enqueue when the UNIQUE
// constraint on dedupe_key fires. The handler treats this as a
// programming error and surfaces it as 500 — a duplicate dedupe key
// only happens on a buggy double-enqueue.
var ErrDuplicateDedupeKey = errors.New("outbox: duplicate dedupe_key")

// isUniqueViolation returns true when err is a Postgres UNIQUE
// constraint violation. sqlx wraps the underlying pq error so we
// check by error message — there's no exported sentinel on the pq
// driver and the integration tests confirm the literal message.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	const msg = `duplicate key value violates unique constraint`
	return errors.Is(err, err) && containsString(err.Error(), msg)
}

// containsString is a tiny helper so we don't need strings.Contains
// in the import set of this file (which would otherwise only need
// it for one call).
func containsString(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if len(needle) > len(haystack) {
		return false
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
