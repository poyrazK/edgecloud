package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
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
	// regions_failed, last_publish_at, last_publish_attempt_id) AND
	// the per-region cache-state columns (regions_cached,
	// regions_cache_failed) ARE in the DO UPDATE clause. The four
	// publish-state columns are reset to their zero values on
	// re-activation — intentional: a re-activation is a fresh publish
	// cycle, so the prior activation's "regions already notified"
	// history must not mask regions that need (re)publishing on this
	// activation. AppendRegionsPublished / AppendRegionsFailed
	// repopulate the columns after the publish loop.
	//
	// The two cache-state columns use a CONDITIONAL wipe:
	//
	//	regions_cached       = CASE WHEN deployment_id = $3 THEN regions_cached       ELSE $8 END
	//	regions_cache_failed = CASE WHEN deployment_id = $3 THEN regions_cache_failed ELSE $9 END
	//
	// i.e. they are preserved when the active row is being upserted
	// with the SAME deployment_id (a no-op re-activation — e.g. a
	// retry, a Rollback to the same id, a deploy + immediate re-
	// activate), and wiped to '$8' / '$9' (the caller-supplied zero
	// arrays) only when the deployment_id is CHANGING. This is
	// required for issue #332 Layer 3: the cache-skip-on-activation
	// logic in publishSwap consults current.RegionsCached to decide
	// whether to push bytes to each region. If Set unconditionally
	// wiped RegionsCached on every upsert, the cache-skip branch
	// could never fire — the row would always read empty after a
	// Set, defeating the optimization.
	//
	// Note: the new deployment_id only commits inside the DO UPDATE
	// branch (the INSERT branch inserts a row that didn't exist, so
	// RegionsCached is implicitly empty there). The CASE expression
	// on the INSERT branch would short-circuit to $8 / $9 anyway (the
	// SELECT sees the just-inserted value, which equals $3, so the
	// THEN branch would pick the empty INSERT value — no observable
	// difference), but the simpler form is to write the same
	// CASE expression in both branches for symmetry.
	query := `INSERT INTO active_deployments (
		tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled,
		regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id,
		preview_id, preview_pr_number, activation_attempt_started_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
	ON CONFLICT (tenant_id, app_name) DO UPDATE SET
		deployment_id = $3,
		last_good_deployment_id = $4,
		auto_rollback_enabled = $5,
		regions_published = $6,
		regions_failed = $7,
		regions_cached = CASE WHEN active_deployments.deployment_id = $3 THEN active_deployments.regions_cached ELSE $8 END,
		regions_cache_failed = CASE WHEN active_deployments.deployment_id = $3 THEN active_deployments.regions_cache_failed ELSE $9 END,
		last_publish_at = $10,
		last_publish_attempt_id = $11,
		preview_id = $12,
		preview_pr_number = $13,
		activation_attempt_started_at = $14`
	// pq.StringArray must be non-nil for the NOT NULL DEFAULT '{}'
	// columns to take a value rather than a SQL NULL. domain.StringArrayFrom
	// converts nil → empty pq.StringArray. Same for the *time.Time and
	// *string fields — passing nil pointer writes SQL NULL. The two
	// preview columns (issue #308) are also *string/*int and follow
	// the same nil-pointer-writes-NULL convention.
	//
	// ActivationAttemptStartedAt (issue #440, migration 026) is
	// unconditional in DO UPDATE — every activate / rollback / promote
	// stamp resets it to NOW(). The disable path observes the new
	// value as "an activate is in flight for this row" and waits for
	// the matching last_publish_at stamp before publishing empty.
	regionsPublished := domain.StringArrayFrom(ad.RegionsPublished)
	regionsFailed := domain.StringArrayFrom(ad.RegionsFailed)
	regionsCached := domain.StringArrayFrom(ad.RegionsCached)
	regionsCacheFailed := domain.StringArrayFrom(ad.RegionsCacheFailed)
	_, err := r.db.ExecContext(ctx, query,
		ad.TenantID, ad.AppName, ad.DeploymentID, ad.LastGoodDeploymentID, ad.AutoRollbackEnabled,
		regionsPublished, regionsFailed, regionsCached, regionsCacheFailed, ad.LastPublishAt, ad.LastPublishAttemptID,
		ad.PreviewID, ad.PreviewPRNumber, ad.ActivationAttemptStartedAt,
		ad.DesiredReplicas,
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
	query := `SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id, preview_id, preview_pr_number, activation_attempt_started_at FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`
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
	query := `SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id, preview_id, preview_pr_number, activation_attempt_started_at FROM active_deployments WHERE tenant_id = $1 AND app_name = $2 FOR UPDATE`
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
	query := `SELECT tenant_id, app_name, deployment_id, last_good_deployment_id, auto_rollback_enabled, stable_since, regions_published, regions_failed, regions_cached, regions_cache_failed, last_publish_at, last_publish_attempt_id, preview_id, preview_pr_number, activation_attempt_started_at FROM active_deployments WHERE tenant_id = $1`
	err := r.db.SelectContext(ctx, &ads, query, tenantID)
	return ads, err
}

// JoinedActiveDeployment pairs an active_deployments row with its
// referenced deployments row's `hash` and `regions` columns. The
// ReconcileService uses this to fan out per-(tenant, app) in a single
// round trip instead of an N+1 (one active-list + M deployment
// lookups + M env lists). See ReconcileService.reconcileTenant /
// BuildFullSync.
//
// Hash is sql.NullString (not plain string) because the LEFT JOIN
// passes SQL NULL through for orphan rows — an active row whose
// deployment_id no longer exists in the deployments table. The
// service layer uses Hash.Valid (or equivalently, the absence of a
// non-empty Hash.String) to detect orphans and skip them, logging
// the count so operator-actionable broken-state is visible instead
// of being silently dropped (which is what the previous INNER JOIN
// did).
type JoinedActiveDeployment struct {
	domain.ActiveDeployment
	Hash      sql.NullString `db:"hash"`
	Signature sql.NullString `db:"signature"`
	// SigningKeyID mirrors the deployments.signing_key_id column
	// (issue #307 follow-up PR1). Picked up by reconcile / full_sync
	// to populate AppConfig.SigningKeyID on the NATS wire so workers
	// can pick the right pubkey from their keyring. Empty for legacy
	// rows (NULL column) — workers treat empty as "default key".
	SigningKeyID sql.NullString `db:"signing_key_id"`
	Regions      pq.StringArray `db:"regions"`
}

// ListByTenantWithDeployment returns one row per active deployment
// for the tenant, with the deployment's hash and regions joined in.
//
//	SELECT ad.*, d.hash, d.regions
//	FROM active_deployments ad
//	LEFT JOIN deployments d ON d.id = ad.deployment_id
//	WHERE ad.tenant_id = $1
//
// LEFT JOIN semantics (was INNER JOIN in the earlier draft): an
// active row whose referenced deployment_id no longer exists is
// returned with Hash="" and Regions=nil. Calling code skips the
// publish step for those rows and surfaces the count to the operator
// so the broken (active, missing-deployment) pair isn't silently
// dropped — the operator must resolve it (re-activate or delete the
// active row). The previous log-and-continue on this case in the
// pre-N+1 reconcile loop is preserved here, just centralised at the
// service layer.
func (r *ActiveDeploymentRepository) ListByTenantWithDeployment(ctx context.Context, tenantID string) ([]JoinedActiveDeployment, error) {
	var rows []JoinedActiveDeployment
	query := `
		SELECT ad.tenant_id, ad.app_name, ad.deployment_id, ad.last_good_deployment_id,
		       ad.auto_rollback_enabled, ad.stable_since, ad.regions_published,
		       ad.regions_failed, ad.regions_cached, ad.regions_cache_failed,
		       ad.last_publish_at, ad.last_publish_attempt_id,
		       ad.preview_id, ad.preview_pr_number, ad.activation_attempt_started_at,
		       d.hash, d.signature, d.signing_key_id, d.regions
		FROM active_deployments ad
		LEFT JOIN deployments d ON d.id = ad.deployment_id
		WHERE ad.tenant_id = $1
	`
	err := r.db.SelectContext(ctx, &rows, query, tenantID)
	return rows, err
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

// AppendRegionsCacheState (issue #332, PR 2 follow-up) atomically
// merges `succeeded` into the `regions_cached` array AND `failed` into
// the `regions_cache_failed` array on the (tenant, app) active row,
// in a single UPDATE statement. Called by publishSwap after the
// per-region cache-push loop completes, inside the same
// repository.Transaction block as AppendRegionsPublished /
// AppendRegionsFailed — so all three appends are atomic: if any one
// fails, the tx rolls back the others.
//
// The two-column single-UPDATE shape is required for atomicity: a
// two-UPDATE version would let a reader observe the success
// (regions_cached) without the matching failure
// (regions_cache_failed), and vice versa. Combining the dedup
// logic into one statement keeps the array math server-side and
// avoids a row-level read-modify-write cycle in the application.
//
// `succeeded` carries the regions where the cache push returned
// 2xx. `failed` carries the regions where the cache push errored
// (timeout, non-2xx, transport). The two slices are disjoint
// per-call (publishSwap's cache loop partitions into one or the
// other) — the SQL does NOT enforce that, so a caller that
// accidentally passes the same region in both would end up with
// it in BOTH arrays. publishSwap is the only caller today and it
// never does that; if a future caller is added, the partitioning
// invariant must be documented there.
//
// `ts` is intentionally NOT stamped onto the row. A
// `last_cache_pushed_at` audit column would be noise here — cache
// pushes are best-effort, not a financial metric, and overloading
// the existing `last_publish_at` would conflate two semantics. If
// operators want per-region cache freshness they can `ls
// <cache_base_path>/<tenant>/<app>/<id>.wasm` directly. The
// parameter is kept for signature symmetry with
// AppendRegionsPublished / AppendRegionsFailed (which DO stamp
// last_publish_at) and reserved for a future audit column.
//
// Re-adding the same region is a no-op for the array contents
// (UNNEST + array_agg + DISTINCT collapses dupes) but DOES re-run
// the UPDATE row-locks. Acceptable: this is rare (only retries
// with overlapping regions) and the lock contention is bounded by
// the activation window.
func (r *ActiveDeploymentRepository) AppendRegionsCacheState(ctx context.Context, tenantID, appName string, succeeded, failed []string, ts time.Time) error {
	query := `
		UPDATE active_deployments
		SET regions_cached = (
			SELECT COALESCE(array_agg(DISTINCT r), '{}')
			FROM unnest(regions_cached || $3::text[]) AS r
		),
		regions_cache_failed = (
			SELECT COALESCE(array_agg(DISTINCT r), '{}')
			FROM unnest(regions_cache_failed || $4::text[]) AS r
		)
		WHERE tenant_id = $1 AND app_name = $2
	`
	succeededArr := domain.StringArrayFrom(succeeded)
	failedArr := domain.StringArrayFrom(failed)
	_, err := r.db.ExecContext(ctx, query, tenantID, appName, succeededArr, failedArr)
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
