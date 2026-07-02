package repository

import (
	"context"
	"database/sql"

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
