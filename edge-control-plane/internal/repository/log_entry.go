package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
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

// DeleteOlderThan removes log rows with ts < cutoff. Returns the number of
// rows deleted. Used by the retention GC service (LogGCService.Run).
func (r *LogEntryRepository) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := r.db.ExecContext(ctx, `DELETE FROM logs WHERE ts < $1`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("deleting old logs: %w", err)
	}
	if res == nil {
		return 0, nil
	}
	return res.RowsAffected()
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
