package repository

import (
	"context"
	"database/sql"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
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

// ListRunningAppTarget returns the running target for a single
// `(tenant_id, app_name)` pair, or an empty slice if none. The query
// pushes the tenant + app filter into SQL so the handler does not have
// to load the fleet-wide result and scan it in Go.
//
// The `apps.key` filter uses `split_part(key, ':', 1)` so both legacy
// heartbeat keys (just `app_name`) and canary/blue-green keys
// (`app_name:deployment_id`, written by edge-worker so the ingress can
// distinguish concurrent instances of the same app) match the bare
// `appName` argument. `split_part('app:d_abc', ':', 1)` returns `'app'`
// and `split_part('app', ':', 1)` returns `'app'` — so this query works
// against both key formats without any application-side branching.
func (r *WorkerRepository) ListRunningAppTarget(ctx context.Context, tenantID, appName string) ([]domain.AppTarget, error) {
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
		WHERE split_part(apps.key, ':', 1) = $1
		  AND apps.value->>'tenant_id' = $2
		  AND apps.value->>'status' = 'running'
		  AND (apps.value->>'port') IS NOT NULL
		  AND COALESCE((apps.value->>'port')::int, 0) > 0
		  AND workers.ip IS NOT NULL
		  AND workers.ip <> ''`
	var targets []domain.AppTarget
	if err := r.db.SelectContext(ctx, &targets, query, appName, tenantID); err != nil {
		return nil, err
	}
	return targets, nil
}

// CountRunningWorkers returns the number of distinct workers running
// each of the given apps for a tenant. Apps with zero running workers
// are absent from the result map. The key is the bare app_name (the
// split_part strips canary suffixes like `myapp:d_xxx`). Used by the
// reconcile loop for under-replication monitoring (issue #316).
func (r *WorkerRepository) CountRunningWorkers(ctx context.Context, tenantID string, appNames []string) (map[string]int, error) {
	if len(appNames) == 0 {
		return map[string]int{}, nil
	}
	const query = `
		SELECT split_part(apps.key, ':', 1) AS app_name, COUNT(DISTINCT worker_id) AS running_count
		FROM workers
		JOIN worker_status ON worker_status.worker_id = workers.id
		CROSS JOIN LATERAL jsonb_each(worker_status.apps) AS apps
		WHERE apps.value->>'tenant_id' = $1
		  AND apps.value->>'status' = 'running'
		  AND split_part(apps.key, ':', 1) = ANY($2)
		GROUP BY split_part(apps.key, ':', 1)`
	var rows []struct {
		AppName      string `db:"app_name"`
		RunningCount int    `db:"running_count"`
	}
	if err := r.db.SelectContext(ctx, &rows, query, tenantID, pq.Array(appNames)); err != nil {
		return nil, err
	}
	out := make(map[string]int, len(rows))
	for _, row := range rows {
		out[row.AppName] = row.RunningCount
	}
	return out, nil
}

// GetAppStatus returns the worker-reported status for one of the
// tenant's apps, or `nil, nil` when no worker has reported on this
// (tenant, app) pair yet. Powers GET /api/v1/apps/{appName}/status.
//
// The query deliberately drops the `status = 'running'` and `port` /
// `ip IS NOT NULL` filters that ListRunningAppTarget uses, because the
// caller (WorkerStatusHandler) wants to surface ANY worker-reported
// state — including `crashed`, `hung`, `starting`, `stopping` — so the
// tenant can debug a broken app. Port/ip filters would mask a crashed
// app whose worker has lost its routable address.
//
// The cross-tenant guard is `apps.value->>'tenant_id' = $1`: the JSONB
// key match alone (`apps.key = $2`) is necessary but not sufficient,
// because the same `app_name` can exist on multiple workers hosting
// apps from different tenants. A t_evil request for an app_name that
// happens to be deployed by t_victim returns no rows here, which the
// service translates to "unknown" (no information leak).
func (r *WorkerRepository) GetAppStatus(ctx context.Context, tenantID, appName string) (*domain.AppWorkerStatus, error) {
	const query = `
		SELECT
			apps.key                                    AS app_name,
			apps.value->>'status'                       AS status,
			apps.value->>'deployment_id'                AS deployment_id,
			worker_status.last_report                   AS last_heartbeat,
			workers.region                              AS region,
			workers.id                                  AS worker_id,
			NULLIF(apps.value->>'exit_code', '')::int4  AS exit_code
		FROM workers
		JOIN worker_status ON worker_status.worker_id = workers.id
		CROSS JOIN LATERAL jsonb_each(worker_status.apps) AS apps
		WHERE split_part(apps.key, ':', 1) = $1
		  AND apps.value->>'tenant_id' = $2
		ORDER BY worker_status.last_report DESC
		LIMIT 1`
	var row domain.AppWorkerStatus
	if err := r.db.GetContext(ctx, &row, query, appName, tenantID); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &row, nil
}

// TenantsHostedBy returns the deduplicated set of tenant IDs this
// worker is currently hosting (issue #491 constraint #2). A tenant
// counts as "hosted" iff its tenant_id appears in worker_status.apps
// with status = 'running' — crashed / hung / stopping entries are
// excluded. A worker whose heartbeat hasn't landed yet (no
// worker_status row, or empty apps) returns ([]string{}, nil).
//
// The JOIN to workers (rather than worker_status alone) is intentional:
// a worker_status row whose workers row has been GC'd (WorkerGC) must
// not leak stale hosting data. Mirrors the join shape used by
// ListRunningAppTarget and GetAppStatus so all three queries agree on
// "what counts as a live worker".
//
// DISTINCT collapses duplicates at the SQL layer; we additionally
// dedupe in Go to keep the result stable across drivers (sqlmock
// returns raw rows without applying DISTINCT).
func (r *WorkerRepository) TenantsHostedBy(ctx context.Context, workerID string) ([]string, error) {
	const query = `
		SELECT DISTINCT apps.value->>'tenant_id' AS tenant_id
		FROM workers
		JOIN worker_status ON worker_status.worker_id = workers.id
		CROSS JOIN LATERAL jsonb_each(worker_status.apps) AS apps
		WHERE workers.id = $1
		  AND apps.value->>'tenant_id' IS NOT NULL
		  AND apps.value->>'status'     = 'running'`
	var rows []struct {
		TenantID string `db:"tenant_id"`
	}
	if err := r.db.SelectContext(ctx, &rows, query, workerID); err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(rows))
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		if row.TenantID == "" {
			continue
		}
		if _, ok := seen[row.TenantID]; ok {
			continue
		}
		seen[row.TenantID] = struct{}{}
		out = append(out, row.TenantID)
	}
	return out, nil
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
	// `ip = COALESCE(EXCLUDED.ip, workers.ip)` (instead of `EXCLUDED.ip`)
	// preserves a previously-known IP when a worker re-registers without
	// one (EXCLUDED.ip is NULL). The heartbeat path (WorkerRepository.UpdateAddr)
	// is the primary writer of `workers.ip`; a worker restart that omits IP
	// from the register body must not clobber the IP that heartbeats have
	// established — otherwise the public ingress's ListRunningAppTarget
	// filter (`workers.ip IS NOT NULL`) drops the app from routing and
	// every public request 502s until the next heartbeat lands.
	query := `
		INSERT INTO workers (id, tenant_id, region, ip, memory_mb, last_seen, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO UPDATE SET
			last_seen = EXCLUDED.last_seen,
			ip        = COALESCE(EXCLUDED.ip, workers.ip)
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

// DeleteOlderThan removes worker records whose last_seen is older than
// the given duration. Returns the number of deleted rows.
func (r *WorkerRepository) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	res, err := r.db.ExecContext(ctx,
		`DELETE FROM workers WHERE last_seen < NOW() - make_interval(secs => $1)`,
		age.Seconds())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
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

// GetLatestStatuses returns the most-recent worker_status row for each
// worker in `workerIDs`, in a single query. Workers with no status row
// are simply absent from the result map — callers should treat absence
// as "no heartbeat yet" (AppCount=0).
//
// Used by ClusterService.List() to avoid the N+1 of calling GetStatus()
// once per worker.
func (r *WorkerRepository) GetLatestStatuses(ctx context.Context, workerIDs []string) (map[string]domain.WorkerStatus, error) {
	out := make(map[string]domain.WorkerStatus)
	if len(workerIDs) == 0 {
		return out, nil
	}
	var rows []domain.WorkerStatus
	// sqlx + the pgx driver accept []string as a Postgres text[] array,
	// which binds to ANY($1) directly.
	query := `SELECT DISTINCT ON (worker_id) worker_id, apps, last_report FROM worker_status WHERE worker_id = ANY($1) ORDER BY worker_id, last_report DESC`
	if err := r.db.SelectContext(ctx, &rows, query, pq.Array(workerIDs)); err != nil {
		return nil, err
	}
	for _, ws := range rows {
		out[ws.WorkerID] = ws
	}
	return out, nil
}
