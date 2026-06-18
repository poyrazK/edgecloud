package repository

import (
	"context"
	"database/sql"
	"time"

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
	query := `INSERT INTO workers (id, tenant_id, region, ip, memory_mb, last_seen, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`
	_, err := r.db.ExecContext(ctx, query, w.ID, w.TenantID, w.Region, w.IP, w.MemoryMB, w.LastSeen, w.CreatedAt)
	return err
}

func (r *WorkerRepository) GetByID(ctx context.Context, id string) (*domain.Worker, error) {
	var w domain.Worker
	query := `SELECT id, tenant_id, region, ip, memory_mb, last_seen, created_at FROM workers WHERE id = $1`
	err := r.db.GetContext(ctx, &w, query, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &w, err
}

func (r *WorkerRepository) List(ctx context.Context) ([]domain.Worker, error) {
	var workers []domain.Worker
	query := `SELECT id, tenant_id, region, ip, memory_mb, last_seen, created_at FROM workers ORDER BY region, created_at DESC`
	err := r.db.SelectContext(ctx, &workers, query)
	return workers, err
}

func (r *WorkerRepository) CountByTenant(ctx context.Context, tenantID string) (int, error) {
	var count int
	query := `SELECT COUNT(*) FROM workers WHERE tenant_id = $1`
	err := r.db.GetContext(ctx, &count, query, tenantID)
	return count, err
}

func (r *WorkerRepository) ListByTenant(ctx context.Context, tenantID string) ([]domain.Worker, error) {
	var workers []domain.Worker
	query := `SELECT id, tenant_id, region, ip, memory_mb, last_seen, created_at FROM workers WHERE tenant_id = $1 ORDER BY region, created_at DESC`
	err := r.db.SelectContext(ctx, &workers, query, tenantID)
	return workers, err
}

func (r *WorkerRepository) UpdateLastSeen(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE workers SET last_seen = NOW() WHERE id = $1`, id)
	return err
}

// UpdateAddr updates the worker's public IP. Called from the heartbeat handler
// when a heartbeat carries a non-empty `worker_addr` so the column reflects
// the worker's current routable address. A previous good value is preserved
// when the new value is empty (defensive default — operators should always
// set `EDGE_WORKER_ADDR` on the worker).
func (r *WorkerRepository) UpdateAddr(ctx context.Context, id, addr string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE workers SET ip = $2 WHERE id = $1`, id, addr)
	return err
}

// ListRunningAppTargets joins workers and worker_status to enumerate every
// running app, with the per-app port and tenant_id extracted from the
// JSONB apps blob. Used by the public ingress (cold start) and by the
// CLI's `edge status` to validate a URL is live.
func (r *WorkerRepository) ListRunningAppTargets(ctx context.Context) ([]domain.AppTarget, error) {
	const query = `
		SELECT
			apps.key                                    AS app_name,
			apps.value->>'tenant_id'                    AS tenant_id,
			workers.id                                  AS worker_id,
			workers.region                              AS region,
			COALESCE(workers.ip, '')                    AS worker_addr,
			COALESCE((apps.value->>'port')::int, 0)     AS port
		FROM workers
		JOIN worker_status ON worker_status.worker_id = workers.id
		CROSS JOIN LATERAL jsonb_each(worker_status.apps) AS apps
		WHERE apps.value->>'status' = 'running'
		  AND (apps.value->>'port') IS NOT NULL
		  AND COALESCE((apps.value->>'port')::int, 0) > 0
		  AND workers.ip IS NOT NULL
		  AND workers.ip <> ''`
	var targets []domain.AppTarget
	if err := r.db.SelectContext(ctx, &targets, query); err != nil {
		return nil, err
	}
	return targets, nil
}

func (r *WorkerRepository) Upsert(ctx context.Context, tenantID string, req *domain.RegisterWorkerRequest) (wasCreated bool, err error) {
	memoryMB := req.MemoryMB
	if memoryMB == 0 {
		memoryMB = 4096
	}
	var ip *string
	if req.IP != "" {
		ip = &req.IP
	}
	now := time.Now()
	query := `
		INSERT INTO workers (id, tenant_id, region, ip, memory_mb, last_seen, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO UPDATE SET
			last_seen = EXCLUDED.last_seen,
			ip        = EXCLUDED.ip
		RETURNING (xmax = 0) AS was_created`
	var wasCreatedRow bool
	err = r.db.GetContext(ctx, &wasCreatedRow, query, req.WorkerID, tenantID, req.Region, ip, memoryMB, now, now)
	if err != nil {
		return false, err
	}
	return wasCreatedRow, nil
}

func (r *WorkerRepository) Delete(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM workers WHERE id = $1`, id)
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
