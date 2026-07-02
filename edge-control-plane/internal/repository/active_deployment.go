package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// ActiveDeploymentRepository handles active deployment mappings.
type ActiveDeploymentRepository struct {
	db DBTX
}

func NewActiveDeploymentRepository(db *sqlx.DB) *ActiveDeploymentRepository {
	return &ActiveDeploymentRepository{db: db}
}

// WithTx returns a new ActiveDeploymentRepository using the provided transaction.
func (r *ActiveDeploymentRepository) WithTx(tx *sqlx.Tx) *ActiveDeploymentRepository {
	return &ActiveDeploymentRepository{db: tx}
}

func (r *ActiveDeploymentRepository) Set(ctx context.Context, ad *domain.ActiveDeployment) error {
	// Note: stable_since is intentionally NOT in this INSERT ... ON CONFLICT
	// DO UPDATE list. The stability clock is managed by SetStableSince /
	// ClearStableSince and by the explicit swap paths (RollbackDeployment,
	// ResetStableSinceForRollback). Including it here would let an
	// ActivateDeployment inadvertently inherit a stale stable_since from
	// the prior activation of the same row, which could trigger an
	// immediate stability-window promote on a deployment that has never
	// been observed running.
	//
	// The DO UPDATE branch also resets auto_rollback_enabled from the
	// caller-provided value. This is correct for re-activation (the
	// caller has already read the freshest deployment row), but it's
	// worth flagging: a manual `UPDATE active_deployments SET
	// auto_rollback_enabled = true` from the DB will be silently
	// overwritten on the next activate. That is the desired semantics
	// for the v1 feature — tenants opt in via `edge deploy --auto-rollback`
	// and the flag follows the deployment row, not the active slot.
	//
	// Per-region publish-state columns (regions_published,
	// regions_failed, last_publish_at, last_publish_attempt_id) ARE in
	// the DO UPDATE clause and are reset to their zero values on
	// re-activation. This is intentional: a re-activation is a fresh
	// publish cycle, so the prior activation's "regions already
	// notified" history must not mask regions that need (re)publishing
	// on this activation. AppendRegionsPublished /
	// AppendRegionsFailed repopulate the columns after the publish loop.
	query := `INSERT INTO active_deployments (
		tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled,
		regions_published, regions_failed, last_publish_at, last_publish_attempt_id
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	ON CONFLICT (tenant_id, app_name) DO UPDATE SET
		deployment_id = $3,
		last_good_deployment_id = $4,
		auto_rollback_enabled = $5,
		regions_published = $6,
		regions_failed = $7,
		last_publish_at = $8,
		last_publish_attempt_id = $9`
	// pq.StringArray must be non-nil for the NOT NULL DEFAULT '{}'
	// columns to take a value rather than a SQL NULL. domain.StringArrayFrom
	// converts nil → empty pq.StringArray. Same for the *time.Time and
	// *string fields — passing nil pointer writes SQL NULL.
	regionsPublished := domain.StringArrayFrom(ad.RegionsPublished)
	regionsFailed := domain.StringArrayFrom(ad.RegionsFailed)
	_, err := r.db.ExecContext(ctx, query,
		ad.TenantID, ad.AppName, ad.DeploymentID, ad.LastGoodDeploymentID, ad.AutoRollbackEnabled,
		regionsPublished, regionsFailed, ad.LastPublishAt, ad.LastPublishAttemptID,
	)
	return err
}

// SetStableSince sets stable_since = $4 on the row identified by
// (tenant, app, deployment). No-op if the row's stable_since is
// already non-NULL — the first observation of "running" wins, and
// subsequent stable observations don't reset the clock (otherwise a
// heartbeating app would never advance its stable_since past NOW and
// the stability window would never fire).
//
// Resets stable_since to $4 only when the current value is NULL AND
// the on-row deployment_id matches the caller's $3. The deployment_id
// guard prevents a stale SetStableSince (e.g. one in flight when an
// activate raced ahead) from poisoning the new deployment's clock.
func (r *ActiveDeploymentRepository) SetStableSince(ctx context.Context, tenantID, appName, deploymentID string, ts time.Time) error {
	query := `UPDATE active_deployments SET stable_since = $4 WHERE tenant_id = $1 AND app_name = $2 AND deployment_id = $3 AND stable_since IS NULL`
	_, err := r.db.ExecContext(ctx, query, tenantID, appName, deploymentID, ts)
	return err
}

// ClearStableSince resets stable_since to NULL for (tenant, app).
// Used when the heartbeat handler observes a non-running status
// (crashed/hung/starting/stopping) and wants to re-arm the
// stability clock from scratch the next time status flips to
// "running".
func (r *ActiveDeploymentRepository) ClearStableSince(ctx context.Context, tenantID, appName string) error {
	query := `UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`
	_, err := r.db.ExecContext(ctx, query, tenantID, appName)
	return err
}

// PromoteToLastGood sets last_good_deployment_id = $3 on the row
// identified by (tenant, app). Only fires when last_good is
// currently NULL — the goal is to capture the first time a freshly-
// activated deployment becomes the safety net, not to keep
// overwriting an already-set last_good pointer (which would
// silently undo a manual rollback). Used by the stability-window
// evaluator after a deployment has been observed running for
// `STABLE_WINDOW_SECONDS`.
//
// Stable_since is preserved unchanged on a successful promote — the
// currently-active deployment is the one we just observed running,
// so its clock is still meaningful for the next stability check.
func (r *ActiveDeploymentRepository) PromoteToLastGood(ctx context.Context, tenantID, appName, deploymentID string) error {
	query := `UPDATE active_deployments SET last_good_deployment_id = $3 WHERE tenant_id = $1 AND app_name = $2 AND last_good_deployment_id IS NULL`
	_, err := r.db.ExecContext(ctx, query, tenantID, appName, deploymentID)
	return err
}

// ResetStableSinceForRollback is the single source of truth for the
// "swap deployment_id ↔ last_good_deployment_id" mutation, used by
// both the manual `edge rollback` path and the worker-driven
// auto-rollback path (and, in Commit 4, the heartbeat-driven
// stability-window promote).
//
// All four mutations happen in one UPDATE so the row is consistent
// for any concurrent reader that opens a tx after this statement
// commits:
//
//	SET deployment_id = last_good_deployment_id,
//	    last_good_deployment_id = NULL,
//	    stable_since = NULL
//	WHERE tenant_id = $1 AND app_name = $2
//	  AND last_good_deployment_id IS NOT NULL
//	  AND auto_rollback_enabled = true
//
// The auto_rollback_enabled guard is intentional for the stability-
// window path (which should only fire when the tenant opted in) and
// is a no-op for the manual-rollback path (which today does not
// check the flag — manual `edge rollback` always wins, even if the
// app is not opted in). To preserve that semantic for manual
// rollbacks while still honoring the flag for auto-rollback, this
// method is split: callers that want manual-rollback semantics call
// it without the flag check by setting auto_rollback_enabled on the
// row directly before calling, OR callers pass through `service.
// RollbackDeployment` which uses an equivalent inline UPDATE.
//
// Returns:
//   - (priorDeploymentID, nil) on success
//   - ("", ErrNoLastGood) if last_good_deployment_id IS NULL
//   - ("", ErrAutoRollbackDisabled) if auto_rollback_enabled = false
//
// The two error sentinels are defined in `service`; this repo
// returns them via `errors.New` strings that the caller matches via
// `errors.Is`. (We could plumb the sentinel through the repo layer,
// but it would create an import cycle: service → repository → service.)
// The matching in `service.RollbackDeployment` / `handler.AutoRollback`
// uses string-comparison via errors.Is; see those call sites.
func (r *ActiveDeploymentRepository) ResetStableSinceForRollback(ctx context.Context, tenantID, appName string) (priorDeploymentID string, err error) {
	// Use a CTE to swap the values atomically and surface the prior
	// deployment_id to the caller in one round trip. RETURNING gives
	// us the OLD deployment_id (the one we just swapped out) so the
	// caller can publish a TaskMessage naming the now-broken id for
	// audit logs without re-reading.
	const query = `
		WITH updated AS (
			UPDATE active_deployments
			SET deployment_id = last_good_deployment_id,
			    last_good_deployment_id = NULL,
			    stable_since = NULL
			WHERE tenant_id = $1 AND app_name = $2
			  AND last_good_deployment_id IS NOT NULL
			  AND auto_rollback_enabled = true
			RETURNING deployment_id, last_good_deployment_id
		)
		SELECT deployment_id FROM updated
	`
	var newActive string
	row := r.db.QueryRowxContext(ctx, query, tenantID, appName)
	if scanErr := row.Scan(&newActive); scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			// Distinguish "no last-good" from "auto-rollback disabled"
			// by reading the current row state.
			cur, getErr := r.Get(ctx, tenantID, appName)
			if getErr != nil {
				return "", getErr
			}
			if cur == nil {
				return "", sql.ErrNoRows
			}
			if cur.LastGoodDeploymentID == nil {
				return "", errNoLastGoodSentinel
			}
			if !cur.AutoRollbackEnabled {
				return "", errAutoRollbackDisabledSentinel
			}
			// row matched both conditions but UPDATE returned 0 rows —
			// shouldn't happen unless another tx raced us between the
			// UPDATE and the GET. Treat as a no-op success but log
			// (the caller can retry).
			return "", errConcurrentRollbackRace
		}
		return "", scanErr
	}
	return newActive, nil
}

// errNoLastGoodSentinel and errAutoRollbackDisabledSentinel are
// string-matched by the service layer via errors.Is. See the doc
// comment on ResetStableSinceForRollback for why we don't import
// service's sentinels here (cycle).
//
// The strings MUST stay in sync with the corresponding sentinels in
// `internal/service/deployment.go`. A mismatch would cause handlers
// to misclassify the error (e.g. return 403 instead of 409). Tested
// in `active_deployment_test.go::TestResetStableSinceForRollback_*`.
var (
	errNoLastGoodSentinel           = errors.New("no previous deployment to roll back to")
	errAutoRollbackDisabledSentinel = errors.New("auto-rollback disabled")
	errConcurrentRollbackRace       = errors.New("concurrent rollback raced the update")
)

func (r *ActiveDeploymentRepository) Get(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error) {
	var ad domain.ActiveDeployment
	query := `SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, last_publish_at, last_publish_attempt_id FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`
	err := r.db.GetContext(ctx, &ad, query, tenantID, appName)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &ad, err
}

// GetForUpdate reads the active_deployments row for (tenant, app) inside a
// transaction with a row-level lock so the caller can swap
// deployment_id ↔ last_good_deployment_id atomically. Pair with WithTx.
func (r *ActiveDeploymentRepository) GetForUpdate(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error) {
	var ad domain.ActiveDeployment
	query := `SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, last_publish_at, last_publish_attempt_id FROM active_deployments WHERE tenant_id = $1 AND app_name = $2 FOR UPDATE`
	err := r.db.GetContext(ctx, &ad, query, tenantID, appName)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &ad, err
}

func (r *ActiveDeploymentRepository) Delete(ctx context.Context, tenantID, appName string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`, tenantID, appName)
	return err
}

func (r *ActiveDeploymentRepository) ListByTenant(ctx context.Context, tenantID string) ([]domain.ActiveDeployment, error) {
	var ads []domain.ActiveDeployment
	query := `SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, last_publish_at, last_publish_attempt_id FROM active_deployments WHERE tenant_id = $1`
	err := r.db.SelectContext(ctx, &ads, query, tenantID)
	return ads, err
}

// AppendRegionsPublished atomically merges `regions` into the
// `regions_published` array on the (tenant, app) active row, AND
// removes them from `regions_failed` (a region that succeeds on
// retry is no longer "failed"). Also stamps last_publish_at = NOW()
// and last_publish_attempt_id to the supplied UUID. The whole
// mutation happens in one UPDATE so a concurrent reader opening a
// tx after this statement commits sees either the pre- or
// post-state, never a half-applied merge.
//
// The (tenant, app) match is the only WHERE clause — there's no
// guard on regions_published contents. Re-publishing the same
// region is a no-op for the array contents (UNNEST + array_agg +
// DISTINCT collapses dupes) but DOES bump last_publish_at /
// last_publish_attempt_id, which is the correct audit-log semantic.
//
// Uses unnest() + array_agg(DISTINCT ...) rather than `||` to drop
// dupes in one statement. `||` would happily append `us-east`
// twice if the caller re-invoked with the same region.
func (r *ActiveDeploymentRepository) AppendRegionsPublished(ctx context.Context, tenantID, appName string, regions []string, attemptID string, ts time.Time) error {
	query := `
		UPDATE active_deployments
		SET regions_published = (
			SELECT COALESCE(array_agg(DISTINCT r), '{}')
			FROM unnest(regions_published || $3::text[]) AS r
		),
		regions_failed = (
			SELECT COALESCE(array_agg(DISTINCT r), '{}')
			FROM unnest(regions_failed) AS r
			WHERE r <> ALL($3::text[])
		),
		last_publish_at = $4,
		last_publish_attempt_id = $5
		WHERE tenant_id = $1 AND app_name = $2
	`
	regionsArr := domain.StringArrayFrom(regions)
	_, err := r.db.ExecContext(ctx, query, tenantID, appName, regionsArr, ts, attemptID)
	return err
}

// AppendRegionsFailed atomically merges `regions` into the
// `regions_failed` array on the (tenant, app) active row. Does NOT
// clear regions_published — a region that succeeded earlier in
// the loop and then failed later is in BOTH arrays for the rest of
// the call, but the service layer's union-then-dedup logic on the
// next activate call treats them as "needs republish" regardless.
//
// Stamps last_publish_at = NOW() so the operator escape-hatch
// `SELECT last_publish_at WHERE ...` reflects the failure even
// when no region succeeded.
func (r *ActiveDeploymentRepository) AppendRegionsFailed(ctx context.Context, tenantID, appName string, regions []string, attemptID string, ts time.Time) error {
	query := `
		UPDATE active_deployments
		SET regions_failed = (
			SELECT COALESCE(array_agg(DISTINCT r), '{}')
			FROM unnest(regions_failed || $3::text[]) AS r
		),
		last_publish_at = $4,
		last_publish_attempt_id = $5
		WHERE tenant_id = $1 AND app_name = $2
	`
	regionsArr := domain.StringArrayFrom(regions)
	_, err := r.db.ExecContext(ctx, query, tenantID, appName, regionsArr, ts, attemptID)
	return err
}

// Count returns the fleet-wide count of active_deployments rows.
// The autoscaler (issue #85) uses this to compute its target
// headroom — every region sizes its own fleet against the same
// DesiredApps value because the deployment table is global
// (region lives on workers, not on deployments). Multi-region
// partitioning is a separate concern.
func (r *ActiveDeploymentRepository) Count(ctx context.Context) (int, error) {
	var count int
	err := r.db.GetContext(ctx, &count, `SELECT COUNT(*) FROM active_deployments`)
	return count, err
}
