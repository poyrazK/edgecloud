package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// AutoscaleRepository persists autoscaler decisions to the
// `autoscale_events` table (migration 012_autoscale_events.up.sql).
//
// Every decision the autoscaler makes — including `noop`s from inside
// the target band or from the cooldown gate — is recorded here so
// operators can answer "why did the fleet size change?" via the
// /api/admin/cluster endpoint and the `edge cluster events` CLI.
type AutoscaleRepository struct {
	db DBTX
}

func NewAutoscaleRepository(db *sqlx.DB) *AutoscaleRepository {
	return &AutoscaleRepository{db: db}
}

// WithTx returns a new AutoscaleRepository using the provided transaction.
// Mirrors WorkerRepository.WithTx — callers that compose multiple writes
// (e.g., a future scale-and-record pattern that wants Insert to be in the
// same tx as a domain update) can pass the tx here.
func (r *AutoscaleRepository) WithTx(tx *sqlx.Tx) *AutoscaleRepository {
	return &AutoscaleRepository{db: tx}
}

// Insert persists a single decision. Returns the new row's id so callers
// can correlate the event with downstream effects (logs, external
// provisioner callbacks). The `created_at` column defaults to now() on
// the server side — we don't bind it so all clocks are DB-authoritative.
func (r *AutoscaleRepository) Insert(ctx context.Context, e *domain.AutoscaleEvent) (int64, error) {
	const query = `
		INSERT INTO autoscale_events
			(region, action, from_count, to_count, reason, provider_kind, succeeded, error_message)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id`
	var id int64
	err := r.db.QueryRowxContext(ctx, query,
		e.Region, string(e.Action), e.FromCount, e.ToCount, e.Reason,
		e.ProviderKind, e.Succeeded, e.ErrorMessage,
	).Scan(&id)
	return id, err
}

// ListRecent returns the most recent events for a region, newest first.
// `limit` caps the result; callers should pass a small number (50-200)
// since this drives the admin cluster view. An empty region string
// matches all regions (use sparingly — the cluster admin endpoint is the
// only known caller).
func (r *AutoscaleRepository) ListRecent(ctx context.Context, region string, limit int) ([]domain.AutoscaleEvent, error) {
	if limit <= 0 {
		return nil, nil
	}
	var rows []domain.AutoscaleEvent
	var (
		err  error
		args []any
	)
	if region == "" {
		const query = `
			SELECT id, created_at, region, action, from_count, to_count,
			       reason, provider_kind, succeeded, error_message
			FROM autoscale_events
			ORDER BY created_at DESC
			LIMIT $1`
		err = r.db.SelectContext(ctx, &rows, query, limit)
	} else {
		const query = `
			SELECT id, created_at, region, action, from_count, to_count,
			       reason, provider_kind, succeeded, error_message
			FROM autoscale_events
			WHERE region = $1
			ORDER BY created_at DESC
			LIMIT $2`
		args = []any{region, limit}
		err = r.db.SelectContext(ctx, &rows, query, args...)
	}
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return rows, err
}

// CountByRegion returns the total number of events recorded for a
// region. Used by the autoscaler to assert "we've actually been
// recording decisions" in tests; not on the hot path.
func (r *AutoscaleRepository) CountByRegion(ctx context.Context, region string) (int, error) {
	var count int
	err := r.db.GetContext(ctx, &count,
		`SELECT COUNT(*) FROM autoscale_events WHERE region = $1`, region)
	return count, err
}

// DeleteOlderThanBatched deletes up to `batchSize` rows whose created_at
// is older than `retention` (server-side: NOW() - retention), looping
// until either the DB has no more matching rows or `maxBatches` is hit.
// Returns the total rows deleted across all batches.
//
// Paginated shape mirrors LogEntryRepository.DeleteOlderThanBatched —
// see that method's doc-comment for the lock-duration and clock-skew
// rationale. The retention GC driving this method is
// service.AutoscaleEventGCService (issue #574); the GC refuses to run
// with non-positive retention, so this guard is belt-and-suspenders.
//
// Index path: the (created_at) index added in migration
// 031_gc_retention_indexes covers the WHERE created_at < … predicate.
// The (region, created_at DESC) index from migration 012 remains for
// the ListRecent(region=) read path; this method does not use it.
func (r *AutoscaleRepository) DeleteOlderThanBatched(
	ctx context.Context, retention time.Duration, batchSize, maxBatches int,
) (int64, error) {
	if retention <= 0 {
		return 0, fmt.Errorf("retention must be positive, got %s", retention)
	}
	const cap = 10_000
	if batchSize <= 0 || batchSize > cap {
		batchSize = cap
	}
	if maxBatches <= 0 {
		maxBatches = 1
	}

	var total int64
	for i := 0; i < maxBatches; i++ {
		if ctx.Err() != nil {
			return total, ctx.Err()
		}
		res, err := r.db.ExecContext(ctx,
			`DELETE FROM autoscale_events WHERE id IN (SELECT id FROM autoscale_events WHERE created_at < NOW() - make_interval(secs => $1) LIMIT $2)`,
			retention.Seconds(), int64(batchSize))
		if err != nil {
			return total, fmt.Errorf("deleting old autoscale_events (batch %d): %w", i, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, fmt.Errorf("rows affected (batch %d): %w", i, err)
		}
		total += n
		if n < int64(batchSize) {
			break
		}
	}
	return total, nil
}
