package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// AppRepository handles app data access.
type AppRepository struct {
	db DBTX
}

func NewAppRepository(db *sqlx.DB) *AppRepository {
	return &AppRepository{db: db}
}

// NewAppRepositoryFromDBTX constructs a repository bound to a DBTX
// (either *sqlx.DB or *sqlx.Tx). Used by tests that need a tx-bound
// repo without standing up a full database connection.
func NewAppRepositoryFromDBTX(dbtx DBTX) *AppRepository {
	return &AppRepository{db: dbtx}
}

// WithTx returns a new AppRepository using the provided transaction.
func (r *AppRepository) WithTx(tx *sqlx.Tx) *AppRepository {
	return &AppRepository{db: tx}
}

func (r *AppRepository) Create(ctx context.Context, app *domain.App) error {
	query := `INSERT INTO apps (id, tenant_id, name, description, created_at) VALUES ($1, $2, $3, $4, $5)`
	_, err := r.db.ExecContext(ctx, query, app.ID, app.TenantID, app.Name, app.Description, app.CreatedAt)
	return err
}

func (r *AppRepository) Get(ctx context.Context, tenantID, appName string) (*domain.App, error) {
	var app domain.App
	query := `SELECT id, tenant_id, name, description, created_at FROM apps WHERE tenant_id = $1 AND name = $2`
	err := r.db.GetContext(ctx, &app, query, tenantID, appName)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &app, err
}

// GetForUpdate locks the (tenant_id, name) row with `SELECT … FOR UPDATE`
// and returns the row. The lock is held for the lifetime of the
// surrounding transaction; concurrent writers targeting the same
// (tenant, app) block until the transaction commits or rolls back.
//
// Used by service.DomainService.AddDomain to serialize concurrent
// domain inserts against the same app so the per-app quota
// (MaxDomainsPerApp) cannot be overshot by a count-then-insert race.
// The same pattern is used by service.DeploymentService.ActivateDeployment
// on the active_deployments row.
//
// Returns (nil, nil) when no app exists for (tenantID, appName) —
// the service layer maps that to ErrAppNotFound.
func (r *AppRepository) GetForUpdate(ctx context.Context, tenantID, appName string) (*domain.App, error) {
	var app domain.App
	query := `SELECT id, tenant_id, name, description, created_at FROM apps WHERE tenant_id = $1 AND name = $2 FOR UPDATE`
	err := r.db.GetContext(ctx, &app, query, tenantID, appName)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &app, err
}

// List returns up to limit apps for a tenant, paginated by name via
// keyset (issue #58). The caller passes the previous page's last
// visible app's name as afterName; the empty string means "first
// page" (no lower-bound predicate). Ordering is `name ASC` so the
// (tenant_id, name) UNIQUE constraint guarantees a strict total
// order across the page boundary — there can be no ties on name
// within a tenant, so the strict-tuple (created_at, id) fallback
// that deployments uses is not needed here.
//
// Backed by idx_apps_tenant_id (from migration 004_apps) for the
// WHERE filter; the keyset predicate `name > $2` lets the planner
// walk that index in cursor order without an in-memory sort. The
// page-walk loop in the service layer calls this with afterName = ""
// on the first page and with the previous page's last name on every
// subsequent page.
func (r *AppRepository) List(ctx context.Context, tenantID string, limit int, afterName string) ([]domain.App, error) {
	var apps []domain.App
	query := `SELECT id, tenant_id, name, description, created_at FROM apps WHERE tenant_id = $1 AND name > $2 ORDER BY name LIMIT $3`
	err := r.db.SelectContext(ctx, &apps, query, tenantID, afterName, limit)
	return apps, err
}

func (r *AppRepository) Delete(ctx context.Context, tenantID, appName string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM apps WHERE tenant_id = $1 AND name = $2`, tenantID, appName)
	return err
}

func (r *AppRepository) Exists(ctx context.Context, tenantID, appName string) (bool, error) {
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM apps WHERE tenant_id = $1 AND name = $2)`
	err := r.db.GetContext(ctx, &exists, query, tenantID, appName)
	return exists, err
}

// AtomicDelete deletes an app atomically and returns whether a row was deleted.
func (r *AppRepository) AtomicDelete(ctx context.Context, tenantID, appName string) (bool, error) {
	var deleted bool
	err := r.db.GetContext(ctx, &deleted,
		`DELETE FROM apps WHERE tenant_id = $1 AND name = $2 RETURNING true`,
		tenantID, appName)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return deleted, err
}

// DeleteIfNoDeployments removes the apps row for (tenantID, appName)
// only if it has zero deployments. Used as the compensating write
// when DeploymentService.Deploy's artifact save fails on the very
// first deploy of an app: CreateIfNotExists has just inserted an
// apps row, the deployments row was created next, and then the
// artifact save failed — both rows have been rolled back, but the
// apps row remains orphaned unless we also delete it conditionally
// on "no deployments ever succeeded for this app."
//
// The NOT EXISTS subquery makes the operation safe to call
// concurrently with other deploys: if a concurrent successful
// deploy exists (or appears during the call), the DELETE is a
// no-op. Returns whether a row was deleted.
func (r *AppRepository) DeleteIfNoDeployments(ctx context.Context, tenantID, appName string) (bool, error) {
	var deleted bool
	err := r.db.GetContext(ctx, &deleted,
		`DELETE FROM apps
		 WHERE tenant_id = $1 AND name = $2
		   AND NOT EXISTS (
		       SELECT 1 FROM deployments
		       WHERE tenant_id = $1 AND app_name = $2
		   )
		 RETURNING true`,
		tenantID, appName)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return deleted, err
}

// Update updates mutable fields of an existing app.
func (r *AppRepository) Update(ctx context.Context, app *domain.App) error {
	query := `UPDATE apps SET description = $1 WHERE id = $2 AND tenant_id = $3`
	_, err := r.db.ExecContext(ctx, query, app.Description, app.ID, app.TenantID)
	return err
}

// CountByTenant returns the number of apps for a tenant.
func (r *AppRepository) CountByTenant(ctx context.Context, tenantID string) (int, error) {
	var count int
	query := `SELECT COUNT(*) FROM apps WHERE tenant_id = $1`
	err := r.db.GetContext(ctx, &count, query, tenantID)
	return count, err
}

// InsertIfNotExists inserts an app only if no app with this tenant+name exists.
// Returns true if a row was inserted, false if it already existed.
func (r *AppRepository) InsertIfNotExists(ctx context.Context, app *domain.App) (bool, error) {
	var inserted bool
	query := `
		INSERT INTO apps (id, tenant_id, name, description, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (tenant_id, name) DO NOTHING
		RETURNING (xmax = 0)`
	err := r.db.GetContext(ctx, &inserted, query, app.ID, app.TenantID, app.Name, app.Description, app.CreatedAt)
	return inserted, err
}

// GetRateLimit returns the per-app rate limit override for (tenantID, appName).
// Returns (nil, nil) when the app does not exist.
func (r *AppRepository) GetRateLimit(ctx context.Context, tenantID, appName string) (*domain.AppRateLimit, error) {
	var rl domain.AppRateLimit
	query := `SELECT rate_limit_rps AS rps, rate_limit_burst AS burst FROM apps WHERE tenant_id = $1 AND name = $2`
	err := r.db.GetContext(ctx, &rl, query, tenantID, appName)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &rl, err
}

// GetL4Port returns the persisted L4 public port for (tenantID, appName),
// or (0, nil) if the app has no allocated port yet. Issue #548.
//
// Returns (0, sql.ErrNoRows) ONLY when the app itself does not exist
// (the caller can distinguish app-missing from port-unset by also
// calling Exists; in practice callers always call this AFTER Exists
// because the route is gated on the app existing).
func (r *AppRepository) GetL4Port(ctx context.Context, tenantID, appName string) (uint16, error) {
	var port *int
	query := `SELECT l4_public_port FROM apps WHERE tenant_id = $1 AND name = $2`
	err := r.db.GetContext(ctx, &port, query, tenantID, appName)
	if err == sql.ErrNoRows {
		return 0, err
	}
	if err != nil {
		return 0, err
	}
	if port == nil {
		return 0, nil
	}
	if *port < 0 || *port > 65535 {
		return 0, fmt.Errorf("invalid l4_public_port in db: %d", *port)
	}
	return uint16(*port), nil
}

// AllocateL4Port atomically sets `l4_public_port = $1` for (tenantID,
// appName) IF AND ONLY IF the column is currently NULL. Returns the
// persisted port on success. Returns (existingPort, nil) when another
// caller raced and won — the caller's input port is dropped and the
// stored one is returned so the caller converges to the same value the
// racing caller wrote.
//
// Issue #548. Atomicity is the point: with two edge-ingress instances
// polling the same /l4-port endpoint, both can race to write different
// ports. The `l4_public_port IS NULL` guard in the UPDATE means only
// one wins; the loser re-reads and returns the winner's port.
//
// Returns (0, sql.ErrNoRows) when the app itself does not exist.
func (r *AppRepository) AllocateL4Port(ctx context.Context, tenantID, appName string, port uint16) (uint16, error) {
	if port == 0 {
		return 0, fmt.Errorf("invalid l4_public_port: 0 is reserved")
	}
	const query = `
		UPDATE apps
		   SET l4_public_port = $1
		 WHERE tenant_id = $2 AND name = $3
		   AND l4_public_port IS NULL
		RETURNING l4_public_port`
	var written *int
	err := r.db.GetContext(ctx, &written, query, port, tenantID, appName)
	if err == sql.ErrNoRows {
		// Either the app does not exist OR another caller won the race.
		// Re-read the row to distinguish. sqlx returns ErrNoRows on
		// zero-row RETURNING for both cases.
		existing, getErr := r.GetL4Port(ctx, tenantID, appName)
		if getErr == sql.ErrNoRows {
			return 0, sql.ErrNoRows
		}
		if getErr != nil {
			return 0, getErr
		}
		if existing == 0 {
			// Column is still NULL — that means the app doesn't exist
			// (the only way a non-existent row yields zero-row RETURNING
			// from the UPDATE above AND a NULL column on the SELECT
			// below). Surface as ErrNoRows for the caller's branch.
			return 0, sql.ErrNoRows
		}
		// Lost the race; another caller wrote a port first.
		return existing, nil
	}
	if err != nil {
		return 0, err
	}
	if written == nil {
		return 0, fmt.Errorf("unexpected nil RETURNING from AllocateL4Port")
	}
	return uint16(*written), nil
}

// ReleaseL4Port clears the persisted L4 public port for (tenantID,
// appName). Called by AppService.Delete after the apps row is removed
// (the column goes away with the row), so this is primarily a no-op
// safety net for the case where a future migration keeps orphan
// ports. Returns nil even when the row does not exist (idempotent).
//
// Issue #548.
func (r *AppRepository) ReleaseL4Port(ctx context.Context, tenantID, appName string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE apps SET l4_public_port = NULL WHERE tenant_id = $1 AND name = $2`,
		tenantID, appName)
	return err
}

// AllocatedL4Ports returns the set of L4 public ports currently
// allocated across all tenants. Used by AppService.AllocateL4Port to
// skip ports already taken when picking a fresh one for an app.
// Returns a map keyed by port (uint16) for O(1) lookup; empty when no
// L4 apps exist yet.
//
// Issue #548.
func (r *AppRepository) AllocatedL4Ports(ctx context.Context) (map[uint16]struct{}, error) {
	rows, err := r.db.QueryxContext(ctx,
		`SELECT l4_public_port FROM apps WHERE l4_public_port IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[uint16]struct{})
	for rows.Next() {
		var port *int
		if err := rows.Scan(&port); err != nil {
			return nil, err
		}
		if port == nil {
			continue
		}
		if *port < 0 || *port > 65535 {
			continue
		}
		out[uint16(*port)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
