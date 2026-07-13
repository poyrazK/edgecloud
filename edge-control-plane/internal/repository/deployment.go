package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// DeploymentRepository handles deployment data access.
type DeploymentRepository struct {
	db DBTX
}

func NewDeploymentRepository(db *sqlx.DB) *DeploymentRepository {
	return &DeploymentRepository{db: db}
}

// WithTx returns a new DeploymentRepository using the provided transaction.
func (r *DeploymentRepository) WithTx(tx *sqlx.Tx) *DeploymentRepository {
	return &DeploymentRepository{db: tx}
}

func (r *DeploymentRepository) Create(ctx context.Context, d *domain.Deployment) error {
	// `regions` is `NOT NULL DEFAULT '{}'` in the schema (migration 008).
	// sqlx/pq maps a Go nil slice to SQL NULL, which would violate the
	// constraint — so the service layer is responsible for passing a
	// non-nil slice (it does, via the regions-with-default path in
	// `Deploy` and `ActivateDeployment`). Defensive: treat nil as
	// empty here so a future caller that forgets the invariant gets
	// `[]` on disk, not a constraint error. The field is
	// pq.StringArray (which is []string underneath) so the nil check
	// and the pq.Array() marshal both work as for a plain slice.
	regions := d.Regions
	if regions == nil {
		regions = pq.StringArray{}
	}
	// `build_attestation` is `JSONB NULLABLE` (migration 020); an
	// empty json.RawMessage maps to SQL NULL, which is the right
	// shape for "no attestation yet" rows (pre-PR2 deployments or a
	// deploy that didn't supply build metadata).
	//
	// `preview_id`, `preview_pr_number`, `preview_expires_at`
	// (migration 021, issue #308) are NULLABLE; the service layer
	// passes a *string/*int/*time.Time that's nil for non-preview
	// deploys, and sqlx maps nil → SQL NULL via the pq driver.
	query := `INSERT INTO deployments (id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`
	_, err := r.db.ExecContext(ctx, query, d.ID, d.TenantID, d.AppName, d.Status, d.Hash, pq.Array(regions), d.CreatedAt, d.AutoRollbackEnabled, d.Signature, d.SigningKeyID, d.BuildAttestation, d.DesiredReplicas, d.PreviewID, d.PreviewPRNumber, d.PreviewExpiresAt)
	return err
}

func (r *DeploymentRepository) GetByID(ctx context.Context, id string) (*domain.Deployment, error) {
	var d domain.Deployment
	// Issue #548: join apps to fetch the app's protocol so the
	// activate / env-write paths can stamp `socket_mode="allow-all"`
	// for L4 deployments via `nats.SocketModeForProtocol`. The
	// join matches `apps.value->>'deployment_id'` (= our `id`) so
	// the same deployment always sees the same protocol even if
	// the active_deployments row has moved on.
	query := `SELECT d.id, d.tenant_id, d.app_name, d.status, d.hash, d.regions, d.created_at, d.auto_rollback_enabled, d.signature, d.signing_key_id, d.build_attestation, d.desired_replicas, d.preview_id, d.preview_pr_number, d.preview_expires_at,
		         COALESCE(apps.value->>'protocol', 'http') AS protocol
		    FROM deployments d
		    LEFT JOIN apps
		      ON apps.value->>'tenant_id' = d.tenant_id
		     AND apps.value->>'app_name'  = d.app_name
		   WHERE d.id = $1`
	err := r.db.GetContext(ctx, &d, query, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &d, err
}

func (r *DeploymentRepository) ListByApp(ctx context.Context, tenantID, appName string) ([]domain.Deployment, error) {
	var deployments []domain.Deployment
	query := `SELECT d.id, d.tenant_id, d.app_name, d.status, d.hash, d.regions, d.created_at, d.auto_rollback_enabled, d.signature, d.signing_key_id, d.build_attestation, d.desired_replicas, d.preview_id, d.preview_pr_number, d.preview_expires_at,
		         COALESCE(apps.value->>'protocol', 'http') AS protocol
		    FROM deployments d
		    LEFT JOIN apps
		      ON apps.value->>'tenant_id' = d.tenant_id
		     AND apps.value->>'app_name'  = d.app_name
		   WHERE d.tenant_id = $1 AND d.app_name = $2
		   ORDER BY d.created_at DESC`
	err := r.db.SelectContext(ctx, &deployments, query, tenantID, appName)
	return deployments, err
}

func (r *DeploymentRepository) ListByAppPaginated(ctx context.Context, tenantID, appName string, limit, offset int) ([]domain.Deployment, error) {
	var deployments []domain.Deployment
	query := `SELECT d.id, d.tenant_id, d.app_name, d.status, d.hash, d.regions, d.created_at, d.auto_rollback_enabled, d.signature, d.signing_key_id, d.build_attestation, d.desired_replicas, d.preview_id, d.preview_pr_number, d.preview_expires_at,
		         COALESCE(apps.value->>'protocol', 'http') AS protocol
		    FROM deployments d
		    LEFT JOIN apps
		      ON apps.value->>'tenant_id' = d.tenant_id
		     AND apps.value->>'app_name'  = d.app_name
		   WHERE d.tenant_id = $1 AND d.app_name = $2
		   ORDER BY d.created_at DESC LIMIT $3 OFFSET $4`
	err := r.db.SelectContext(ctx, &deployments, query, tenantID, appName, limit, offset)
	return deployments, err
}

func (r *DeploymentRepository) CountByApp(ctx context.Context, tenantID, appName string) (int, error) {
	var count int
	query := `SELECT COUNT(*) FROM deployments WHERE tenant_id = $1 AND app_name = $2`
	err := r.db.GetContext(ctx, &count, query, tenantID, appName)
	return count, err
}

func (r *DeploymentRepository) UpdateStatus(ctx context.Context, id, status string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE deployments SET status = $2 WHERE id = $1`, id, status)
	return err
}

// UpdateHashAndSignature writes the post-artifact-save fields
// (`hash`, `signature`, `signing_key_id`, `build_attestation`) on a
// row created earlier in the same request. Used by the migration
// service, which creates the row with empty hash before
// `SaveAndHash` (so the AppService quota check fires), then fills
// in the real hash + signature + attestation after the artifact is
// on disk. Idempotent: returns nil if no row was affected (the row
// was deleted by a concurrent compensating write).
//
// Called from `service.MigrationService.MigrateTree` and (via the
// same code path) from the test-helper `mockDeploymentRepo`. Issues
// #307: signing was added after the row was created, so the original
// "Create-only" flow no longer fits. PR2 adds the build_attestation
// column to the same UPDATE so the row stays single-write-atomic:
// callers don't have to do a follow-up PATCH after this UPDATE.
func (r *DeploymentRepository) UpdateHashAndSignature(ctx context.Context, d *domain.Deployment) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE deployments SET hash = $2, signature = $3, signing_key_id = $4, build_attestation = $5 WHERE id = $1`,
		d.ID, d.Hash, d.Signature, d.SigningKeyID, d.BuildAttestation)
	if err != nil {
		return err
	}
	if _, err := res.RowsAffected(); err != nil {
		return err
	}
	// No-op on missing row is the documented contract — a compensating
	// DeleteByID raced ahead of us. Don't surface as an error.
	return nil
}

func (r *DeploymentRepository) DeleteByApp(ctx context.Context, tenantID, appName string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM deployments WHERE tenant_id = $1 AND app_name = $2`, tenantID, appName)
	return err
}

// DeleteByID removes a deployment row by its ID. Idempotent on missing
// row: returns nil if no row was deleted. Used as the compensating
// write in the Create-then-Save services (Migrate, MigrateTree,
// Deploy) when the artifact save fails after the row was inserted.
func (r *DeploymentRepository) DeleteByID(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM deployments WHERE id = $1`, id)
	return err
}

// DeletedDeployment identifies a row removed by DeleteOlderThanBatched.
// Carries enough information for the caller to remove the artifact on
// disk after the DB row is gone — the registry stores artifacts at
// `/registry/{tenant_id}/{app_name}/{deployment_id}.wasm` and is
// referenced by (id, tenant_id, app_name) in the deployments table.
type DeletedDeployment struct {
	ID       string `db:"id"`
	TenantID string `db:"tenant_id"`
	AppName  string `db:"app_name"`
}

// DeleteOlderThanBatched deletes up to `batchSize` inactive deployment
// rows older than `retention` (server-side: NOW() - retention), looping
// until either the DB has no more matching rows or `maxBatches` is hit.
// Returns the deleted rows so the caller can clean up the artifact
// blobs at /registry/{tenant_id}/{app_name}/{id}.wasm in lockstep.
//
// Three correctness reasons for this shape:
//
//  1. Lock duration: a single unbounded DELETE on the deployments
//     table holds row locks long enough to stall every tenant-facing
//     path that touches it — Deploy / Activate / Status / Rollback /
//     traffic-split reconcile. Batches of 10k rows amortize the
//     round-trip cost and bound worst-case lock duration. The same
//     defense is in LogEntryRepository.DeleteOlderThanBatched.
//
//  2. Clock-skew immunity: the cutoff is computed server-side as
//     `NOW() - make_interval(secs => $1)`, so the DB clock — not the
//     Go process clock — is the time authority. With the previous
//     implementation the Go process computed the cutoff locally and a
//     skewed host clock could push the cutoff into the future and
//     wipe the table.
//
//  3. Artifact parity: returning the deleted (id, tenant_id, app_name)
//     lets the caller call ArtifactStore.Delete for each row. The
//     previous implementation only returned a count, which forced the
//     caller to either leave artifacts behind (forever-leaking
//     /registry disk) or do a second SELECT-then-DELETE round-trip
//     per row. RETURNING inside the DELETE keeps it atomic per-batch.
//
// `retention <= 0` is rejected up front. The service layer also
// refuses to start with non-positive retention; this is defense in
// depth — a future caller bypassing the service guard still gets a
// clear error rather than `WHERE created_at < NOW() - make_interval(secs => -3600)`
// landing in the future and wiping the table.
//
// The NOT EXISTS subquery skips rows that are still in
// active_deployments. A row that is removed from active_deployments
// between the SELECT and the DELETE in a concurrent transaction is
// still safe to delete (the reconcile loop will recreate the
// active_deployments row from the next heartbeat if needed).
func (r *DeploymentRepository) DeleteOlderThanBatched(
	ctx context.Context, retention time.Duration, batchSize, maxBatches int,
) ([]DeletedDeployment, error) {
	if retention <= 0 {
		return nil, fmt.Errorf("retention must be positive, got %s", retention)
	}
	const cap = 10_000
	if batchSize <= 0 || batchSize > cap {
		batchSize = cap
	}
	if maxBatches <= 0 {
		maxBatches = 1
	}

	var total []DeletedDeployment
	for i := 0; i < maxBatches; i++ {
		if ctx.Err() != nil {
			return total, ctx.Err()
		}
		var batch []DeletedDeployment
		// DELETE ... RETURNING inside the inner SELECT lets the
		// DELETE and the row-return ride on the same plan, so the
		// service knows which (id, tenant_id, app_name) tuples to
		// unlink from /registry/ without a second SELECT round-trip.
		err := r.db.SelectContext(ctx, &batch,
			`DELETE FROM deployments
			 WHERE id IN (
			   SELECT id FROM deployments
			   WHERE created_at < NOW() - make_interval(secs => $1)
			     AND NOT EXISTS (
			       SELECT 1 FROM active_deployments ad
			       WHERE ad.deployment_id = deployments.id
			     )
			   LIMIT $2
			 )
			 RETURNING id, tenant_id, app_name`,
			retention.Seconds(), int64(batchSize))
		if err != nil {
			return total, fmt.Errorf("deleting old deployments (batch %d): %w", i, err)
		}
		total = append(total, batch...)
		if len(batch) < batchSize {
			// Last batch was short — DB has no more matching rows.
			break
		}
	}
	return total, nil
}

// PreviewBlobRef identifies the (tenant, app, deployment) tuple the
// GC needs to unlink from /registry/{tenant_id}/{app_name}/{id}.wasm
// after the deployments row is gone. Returned by
// ListExpiredPreviewBlobs so the PreviewGCService can call
// ArtifactStore.Delete for each row.
//
// Distinct from DeletedDeployment because the sweep key is
// `preview_expires_at < NOW()` (the preview-only expiry), not
// `created_at + retention`. Two different GC loops, two different
// shapes — sharing one struct would force callers to ignore the
// fields they don't care about.
type PreviewBlobRef struct {
	ID       string `db:"id"`
	TenantID string `db:"tenant_id"`
	AppName  string `db:"app_name"`
}

// ListExpiredPreviewBlobs returns up to `batchSize` preview
// deployments whose `preview_expires_at` is in the past, ordered
// oldest-expiry-first so a thundering-herd of expired previews is
// reclaimed in a predictable order. Used by PreviewGCService to
// enumerate the artifact blobs that need to be unlinked from
// /registry/ before the DB row is deleted.
//
// Three reasons for separating this from DeleteExpiredPreviews:
//
//  1. Order matters: the GC must delete the artifact blob first
//     and the row second. If the blob delete fails after the row
//     is gone, we leak a blob (cheap; an orphan on disk).
//     Reversing the order would let a row point at a missing blob,
//     which the worker's download handler has to handle by
//     re-issuing a 404 mid-stream — a worse failure mode.
//
//  2. Clock-skew immunity: same as DeleteOlderThanBatched — the
//     cutoff is server-side NOW() (not Go's time.Now()).
//
//  3. Partial index utilization: the WHERE clause
//     `preview_expires_at IS NOT NULL AND preview_expires_at < NOW()`
//     matches the partial index `idx_deployments_preview_expires_at`
//     from migration 021, so the sweep stays cheap as the table
//     grows with non-preview rows.
func (r *DeploymentRepository) ListExpiredPreviewBlobs(
	ctx context.Context, batchSize int,
) ([]PreviewBlobRef, error) {
	if batchSize <= 0 {
		return nil, fmt.Errorf("batchSize must be positive, got %d", batchSize)
	}
	var refs []PreviewBlobRef
	err := r.db.SelectContext(ctx, &refs,
		`SELECT id, tenant_id, app_name
		 FROM deployments
		 WHERE preview_expires_at IS NOT NULL
		   AND preview_expires_at < NOW()
		 ORDER BY preview_expires_at ASC
		 LIMIT $1`,
		int64(batchSize))
	if err != nil {
		return nil, fmt.Errorf("listing expired preview blobs: %w", err)
	}
	return refs, nil
}

// DeleteExpiredPreviewsByIDs deletes up to `batchSize` preview
// deployments by ID, looping until either the input set is empty
// or `maxBatches` is hit. Returns the deleted rows so the caller
// can audit / log the GC outcome (the blob cleanup already
// happened in the ListExpiredPreviewBlobs step).
//
// Idempotent on missing IDs: rows already deleted (e.g., by a
// concurrent sweep) are silently skipped, because the WHERE id IN
// (...) predicate matches whatever's still present.
//
// `ids` is the slice returned by ListExpiredPreviewBlobs (possibly
// split into chunks by the caller). The service layer is
// responsible for the chunking — this method just runs the DELETE
// per chunk to keep the batch size bounded.
func (r *DeploymentRepository) DeleteExpiredPreviewsByIDs(
	ctx context.Context, ids []string,
) ([]DeletedDeployment, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var deleted []DeletedDeployment
	err := r.db.SelectContext(ctx, &deleted,
		`DELETE FROM deployments
		 WHERE id = ANY($1)
		   AND preview_expires_at IS NOT NULL
		   AND preview_expires_at < NOW()
		 RETURNING id, tenant_id, app_name`,
		pq.Array(ids))
	if err != nil {
		return nil, fmt.Errorf("deleting expired preview rows: %w", err)
	}
	return deleted, nil
}
