package repository

import (
	"context"
	"database/sql"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// WorkerRepository handles worker data access.
type WorkerRepository struct {
	db DBTX
}

func NewWorkerRepository(db *sqlx.DB) *WorkerRepository {
	return &WorkerRepository{db: db}
}

// WithTx returns a new WorkerRepository using the provided transaction.
func (r *WorkerRepository) WithTx(tx *sqlx.Tx) *WorkerRepository {
	return &WorkerRepository{db: tx}
}

func (r *WorkerRepository) Create(ctx context.Context, w *domain.Worker) error {
	query := `INSERT INTO workers (id, region, ip, memory_mb, last_seen, created_at) VALUES ($1, $2, $3, $4, $5, $6)`
	_, err := r.db.ExecContext(ctx, query, w.ID, w.Region, w.IP, w.MemoryMB, w.LastSeen, w.CreatedAt)
	return err
}

func (r *WorkerRepository) GetByID(ctx context.Context, id string) (*domain.Worker, error) {
	var w domain.Worker
	query := `SELECT id, region, ip, memory_mb, last_seen, created_at FROM workers WHERE id = $1`
	err := r.db.GetContext(ctx, &w, query, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &w, err
}

func (r *WorkerRepository) List(ctx context.Context) ([]domain.Worker, error) {
	var workers []domain.Worker
	query := `SELECT id, region, ip, memory_mb, last_seen, created_at FROM workers ORDER BY region, created_at DESC`
	err := r.db.SelectContext(ctx, &workers, query)
	return workers, err
}

func (r *WorkerRepository) UpdateLastSeen(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE workers SET last_seen = NOW() WHERE id = $1`, id)
	return err
}

func (r *WorkerRepository) UpsertStatus(ctx context.Context, ws *domain.WorkerStatus) error {
	query := `INSERT INTO worker_status (worker_id, apps, last_report) VALUES ($1, $2, $3) ON CONFLICT (worker_id) DO UPDATE SET apps = $2, last_report = $3`
	_, err := r.db.ExecContext(ctx, query, ws.WorkerID, ws.Apps, ws.LastReport)
	return err
}

func (r *WorkerRepository) GetStatus(ctx context.Context, workerID string) (*domain.WorkerStatus, error) {
	var ws domain.WorkerStatus
	query := `SELECT worker_id, apps, last_report FROM worker_status WHERE worker_id = $1`
	err := r.db.GetContext(ctx, &ws, query, workerID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &ws, err
}
