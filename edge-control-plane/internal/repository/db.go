package repository

import (
	"context"
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// DBTX is an interface that both *sqlx.DB and *sqlx.Tx satisfy.
// This allows repositories to work with both regular connections and transactions.
type DBTX interface {
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	SelectContext(ctx context.Context, dest any, query string, args ...any) error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	// QueryRowxContext returns a single row from a query that may
	// return zero rows (caller checks sql.ErrNoRows). Both *sqlx.DB
	// and *sqlx.Tx expose this method; we add it here so repositories
	// can do single-row reads from inside a CTE / RETURNING without
	// having to switch to *sqlx.DB at the call site.
	QueryRowxContext(ctx context.Context, query string, args ...any) *sqlx.Row
}

// NewDB creates a new database connection.
func NewDB(dsn string) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(1 * time.Minute)
	return db, nil
}

// Transaction executes fn within a database transaction.
// If fn returns an error, the transaction is rolled back.
// If fn succeeds, the transaction is committed.
func Transaction(ctx context.Context, db *sqlx.DB, fn func(tx *sqlx.Tx) error) error {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return rbErr
		}
		return err
	}
	return tx.Commit()
}
