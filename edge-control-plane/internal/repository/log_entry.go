package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// LogEntryRepository persists tenant log records (issue #76).
type LogEntryRepository struct {
	db DBTX
}

func NewLogEntryRepository(db *sqlx.DB) *LogEntryRepository {
	return &LogEntryRepository{db: db}
}

// WithTx returns a new LogEntryRepository using the provided transaction.
func (r *LogEntryRepository) WithTx(tx *sqlx.Tx) *LogEntryRepository {
	return &LogEntryRepository{db: tx}
}

// InsertBatch writes a slice of log entries in one round-trip.
//
// On an empty input the function is a no-op (returns nil). Callers do not need
// to short-circuit — the worker batcher (LogForwarder) will normally hand us a
// non-empty slice, but tests + graceful shutdown paths benefit from a tolerant
// repository.
//
// TS is intentionally omitted from the column list so the DB DEFAULT NOW()
// applies; stamping TS in Go would force every caller to remember it, and a
// stale time skew across multiple workers would produce inconsistent logs.
// logEntryColumns lists the columns populated by InsertBatch, in order.
// The INSERT statement and per-row placeholder math are derived from this
// slice, so adding a column means appending here plus matching the
// corresponding args in the row loop below.
var logEntryColumns = []string{
	"tenant_id",
	"deployment_id",
	"app_name",
	"worker_id",
	"region",
	"level",
	"message",
	"labels",
}

func (r *LogEntryRepository) InsertBatch(ctx context.Context, entries []domain.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO logs (")
	sb.WriteString(strings.Join(logEntryColumns, ", "))
	sb.WriteString(") VALUES ")

	args := make([]any, 0, len(entries)*len(logEntryColumns))
	for i, e := range entries {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i*len(logEntryColumns) + 1
		// Build the placeholder list, e.g. "($1, $2, $3)".
		placeholders := make([]string, len(logEntryColumns))
		for j := range logEntryColumns {
			placeholders[j] = fmt.Sprintf("$%d", base+j)
		}
		sb.WriteByte('(')
		sb.WriteString(strings.Join(placeholders, ", "))
		sb.WriteByte(')')

		// labels may be nil (omitted) or sent as JSON null / an empty
		// array; the JSONB column is NOT NULL with DEFAULT '{}'::jsonb,
		// so we normalize all "empty-ish" inputs to '{}' for predictable
		// downstream behavior (labels->>'key' works on every row).
		labels := e.Labels
		s := string(labels)
		if len(labels) == 0 || s == "null" || s == "[]" {
			labels = []byte("{}")
		}
		args = append(args,
			e.TenantID, e.DeploymentID, e.AppName, e.WorkerID,
			e.Region, e.Level, e.Message, []byte(labels),
		)
	}

	if _, err := r.db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("inserting log batch: %w", err)
	}
	return nil
}

// DeleteOlderThanBatched deletes up to `batchSize` rows whose ts is older than
// `retention` (server-side: NOW() - retention), looping until either the DB
// has no more matching rows or `maxBatches` is hit. Returns the total rows
// deleted across all batches.
//
// Two correctness reasons for the paginated shape:
//
//  1. Lock duration: a single unbounded DELETE on the logs table can hold
//     row locks long enough to stall ingest (`POST /api/internal/logs` blocks
//     on INSERTs that need to acquire the same lock). Batches of 10k rows
//     amortize the round-trip cost and bound worst-case lock duration.
//
//  2. Clock-skew immunity: the cutoff is computed server-side as
//     `NOW() - make_interval(secs => $1)`, so the DB clock — not the Go
//     process clock — is the time authority. If the control plane and DB
//     hosts disagree on wall-clock time, the wrong rows used to be deleted
//     or kept (e.g. a 5-minute skew with a 7-day retention would push the
//     cutoff into the future and wipe the table).
//
// `retention <= 0` is rejected up front. The service layer also refuses
// to start with non-positive retention; this is defense in depth.
func (r *LogEntryRepository) DeleteOlderThanBatched(
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
			`DELETE FROM logs WHERE id IN (SELECT id FROM logs WHERE ts < NOW() - make_interval(secs => $1) LIMIT $2)`,
			retention.Seconds(), int64(batchSize))
		if err != nil {
			return total, fmt.Errorf("deleting old logs (batch %d): %w", i, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, fmt.Errorf("rows affected (batch %d): %w", i, err)
		}
		total += n
		if n < int64(batchSize) {
			// Last batch was short — DB has no more matching rows.
			break
		}
	}
	return total, nil
}

// GetByID is a test/debug helper that fetches a single row by its BIGSERIAL id.
// It returns (nil, nil) when no row exists. Not used by the public ingest path.
func (r *LogEntryRepository) GetByID(ctx context.Context, id int64) (*domain.LogEntry, error) {
	var e domain.LogEntry
	query := `SELECT id, tenant_id, deployment_id, app_name, worker_id, region, level, message, labels, ts FROM logs WHERE id = $1`
	err := r.db.GetContext(ctx, &e, query, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}

// LogListFilter scopes ListByTenantApp. The repository itself applies no
// defaults — the service layer is responsible for substituting a non-zero
// Since and clamping Limit. A zero Since or nil Levels means "no filter".
//
// Why this lives in the repository layer rather than the handler:
//
//   - Since: the cutoff is computed server-side as `NOW() - make_interval(secs => $1)`
//     to defend against control-plane/DB clock skew (same defense as
//     DeleteOlderThanBatched). Pushing this into SQL keeps the Go-side value
//     strictly a duration, not a wall-clock time the handler has to compute.
//
//   - Levels: the level = ANY($4::text[]) filter is a recheck after the
//     (tenant_id, app_name, ts DESC) index range scan. Documenting this here
//     so a future contributor doesn't try to "optimize" the level into the
//     index — adding more index columns would slow down ingest (PR #98) for
//     marginal read-path gain.
type LogListFilter struct {
	Since  time.Duration
	Levels []string
	Limit  int
}

// ListByTenantApp returns the most recent log entries for (tenantID, appName),
// newest first. The (tenant_id, app_name, ts DESC) index covers the WHERE +
// ORDER BY; the optional level filter is applied as a recheck.
//
// Returns an empty slice (never nil) when no rows match. The service layer
// enforces a positive Limit before calling; this method does not silently
// default to "no limit" because an unbounded query on a logging table is
// the kind of foot-gun that takes a control plane down at 3am.
func (r *LogEntryRepository) ListByTenantApp(
	ctx context.Context, tenantID, appName string, filter LogListFilter,
) ([]domain.LogEntry, error) {
	if filter.Limit <= 0 {
		return nil, fmt.Errorf("limit must be positive, got %d", filter.Limit)
	}

	var sb strings.Builder
	sb.WriteString(`SELECT id, tenant_id, deployment_id, app_name, worker_id, region, level, message, labels, ts
FROM logs
WHERE tenant_id = $1 AND app_name = $2`)

	args := []any{tenantID, appName}
	// nextPlaceholder returns the $N for the next bound arg. Centralizing the
	// index math keeps the SQL builder honest — adding a clause means
	// appending to args and asking nextPlaceholder for the new position.
	nextPlaceholder := func() string {
		return fmt.Sprintf("$%d", len(args)+1)
	}

	if filter.Since > 0 {
		sb.WriteString(" AND ts >= NOW() - make_interval(secs => ")
		sb.WriteString(nextPlaceholder())
		sb.WriteString(")")
		args = append(args, filter.Since.Seconds())
	}
	if len(filter.Levels) > 0 {
		sb.WriteString(" AND level = ANY(")
		sb.WriteString(nextPlaceholder())
		sb.WriteString("::text[])")
		args = append(args, pq.Array(filter.Levels))
	}
	sb.WriteString(" ORDER BY ts DESC LIMIT ")
	sb.WriteString(nextPlaceholder())
	args = append(args, filter.Limit)

	out := make([]domain.LogEntry, 0, filter.Limit)
	if err := r.db.SelectContext(ctx, &out, sb.String(), args...); err != nil {
		return nil, fmt.Errorf("listing logs: %w", err)
	}
	return out, nil
}
