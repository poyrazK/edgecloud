package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// addColumnAccumulators is the whitelist of columns addColumn will accept.
// The column name is interpolated into SQL via fmt.Sprintf AFTER this
// whitelist check, so no caller-supplied SQL fragment can reach the query.
// Adding a new usage counter means (a) adding the column here and
// (b) writing a thin public wrapper below.
var addColumnAccumulators = map[string]bool{
	"used_outbound_bytes":   true,
	"used_request_count":    true,
	"used_resident_seconds": true,
	"used_compute_ms":       true,
}

// quotaColumnList is the canonical column projection used by every query
// that scans a `quotas` row into a domain.Quota. Keeping a single source
// of truth means adding the fourth metered dimension (issue #555,
// used_compute_ms) only requires updating this list — every RETURNING /
// SELECT that scans into Quota picks it up automatically.
//
// The five trailing rate-limit columns (issue #305) live on the same
// row and are scanned into the matching fields on domain.Quota. sqlx
// requires every `db:` tagged column on the struct to appear in the
// projection; if you add a new field here without extending the list,
// GetByTenantID / addColumn / AddMemoryMB / SetRateLimit's RETURNING
// clause all fail at scan time with "missing destination name".
const quotaColumnList = `tenant_id, max_deployments, max_apps, max_workers,
	          max_memory_mb, max_outbound_mb, max_requests_per_month,
	          max_resident_seconds_per_month, max_compute_ms_per_month,
	          used_outbound_bytes, used_request_count, used_memory_mb,
	          used_resident_seconds, used_compute_ms,
	          quota_period_start, quota_lock_grace_until,
	          tenant_rate_limit_rps, tenant_rate_limit_burst,
	          tenant_concurrent_limit, tenant_bandwidth_bps,
	          tenant_rate_limit_set_at`

// addColumn atomically adds delta to one of the per-month usage counters on
// the quotas row, with a lazy month rollover against quota_period_start (the
// counter resets if the stored period is in a past UTC month). Used by
// AddOutboundBytes, AddRequestCount, AddResidentSeconds and AddComputeMs —
// the public wrappers are the only callers.
func (r *QuotaRepository) addColumn(ctx context.Context, tenantID string, delta int64, col string) (*domain.Quota, error) {
	if !addColumnAccumulators[col] {
		return nil, fmt.Errorf("quota repo: refusing to add to non-allowlisted column %q", col)
	}
	var q domain.Quota
	// #nosec G201 — col is whitelisted above; never caller-supplied SQL.
	query := fmt.Sprintf(`
		UPDATE quotas SET
			%s = CASE
				WHEN date_trunc('month', quota_period_start AT TIME ZONE 'UTC')
				     < date_trunc('month', now() AT TIME ZONE 'UTC')
				THEN $2
				ELSE %s + $2
			END,
			quota_period_start = CASE
				WHEN date_trunc('month', quota_period_start AT TIME ZONE 'UTC')
				     < date_trunc('month', now() AT TIME ZONE 'UTC')
				THEN date_trunc('month', now() AT TIME ZONE 'UTC')
				ELSE quota_period_start
			END
		WHERE tenant_id = $1
		RETURNING %s`, col, col, quotaColumnList)
	err := r.db.GetContext(ctx, &q, query, tenantID, delta)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &q, err
}

// QuotaRepository handles quota data access.
type QuotaRepository struct {
	db DBTX
}

func NewQuotaRepository(db *sqlx.DB) *QuotaRepository {
	return &QuotaRepository{db: db}
}

// WithTx returns a new QuotaRepository using the provided transaction.
func (r *QuotaRepository) WithTx(tx *sqlx.Tx) *QuotaRepository {
	return &QuotaRepository{db: tx}
}

func (r *QuotaRepository) Create(ctx context.Context, q *domain.Quota) error {
	query := `INSERT INTO quotas (tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month) VALUES ($1, $2, $3, $4, $5, $6, $7)`
	_, err := r.db.ExecContext(ctx, query, q.TenantID, q.MaxDeployments, q.MaxApps, q.MaxWorkers, q.MaxMemoryMB, q.MaxOutboundMB, q.MaxRequestsPerMonth)
	return err
}

func (r *QuotaRepository) GetByTenantID(ctx context.Context, tenantID string) (*domain.Quota, error) {
	var q domain.Quota
	query := `SELECT ` + quotaColumnList + ` FROM quotas WHERE tenant_id = $1`
	err := r.db.GetContext(ctx, &q, query, tenantID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &q, err
}

// AddOutboundBytes atomically accumulates delta into used_outbound_bytes and
// returns the updated quota row. When the stored quota_period_start is in a
// past calendar month (UTC), the counter and period are reset first so the
// monthly cap applies to the current month only — no separate cron required.
func (r *QuotaRepository) AddOutboundBytes(ctx context.Context, tenantID string, delta uint64) (*domain.Quota, error) {
	return r.addColumn(ctx, tenantID, int64(delta), "used_outbound_bytes")
}

// AddRequestCount atomically accumulates delta into used_request_count and
// returns the updated quota row. Mirrors AddOutboundBytes: a lazy month
// rollover against quota_period_start resets the counter when the stored
// period is in a past calendar month (UTC). Used by
// service.WorkerService.checkRequestCount on every heartbeat.
func (r *QuotaRepository) AddRequestCount(ctx context.Context, tenantID string, delta uint64) (*domain.Quota, error) {
	return r.addColumn(ctx, tenantID, int64(delta), "used_request_count")
}

// AddResidentSeconds atomically accumulates delta into used_resident_seconds
// and returns the updated quota row (issue #484 / #485, third metered
// dimension). Mirrors AddRequestCount: the lazy month rollover against
// quota_period_start resets the counter when the stored period is in a past
// calendar month (UTC). Used by service.WorkerService.checkResidentSeconds
// on every heartbeat for every LongRunning app — Handler (FaaS) apps
// contribute 0 (worker stamps ResidentSeconds=null) and never call this.
func (r *QuotaRepository) AddResidentSeconds(ctx context.Context, tenantID string, delta uint64) (*domain.Quota, error) {
	return r.addColumn(ctx, tenantID, int64(delta), "used_resident_seconds")
}

// AddComputeMs atomically accumulates delta into used_compute_ms and
// returns the updated quota row (issue #555, fourth metered dimension).
// Mirrors AddRequestCount / AddResidentSeconds: the lazy month rollover
// against quota_period_start resets the counter when the stored period
// is in a past calendar month (UTC). Used by
// service.WorkerService.checkComputeMs on every heartbeat for every
// Handler (FaaS) app — LongRunning apps contribute 0 (the dispatch path
// never stamps, so DurationMsTotal is omitted on the wire and the
// helper folds it to 0) and never call this.
func (r *QuotaRepository) AddComputeMs(ctx context.Context, tenantID string, delta uint64) (*domain.Quota, error) {
	return r.addColumn(ctx, tenantID, int64(delta), "used_compute_ms")
}

// MemoryQuotaRepository is the transaction-bound variant of QuotaRepository
// for the *one* operation whose correctness depends on running inside the
// caller's tx: AddMemoryMB (issue #44, part 2). The outer QuotaRepository
// intentionally does NOT expose AddMemoryMB — calling it from outside a
// tx would open a separate connection, commit the counter mutation
// independently of the active_deployments row mutation, and leave the
// counter ahead of the row set on tx abort (or behind on abort after a
// rollback). The split type makes the requirement static: the compiler
// refuses any call site that didn't first acquire a tx via WithTx and
// then route through NewMemoryQuotaRepository(tx).
type MemoryQuotaRepository struct {
	tx *sqlx.Tx
}

// NewMemoryQuotaRepository returns a memory-only quota repository bound
// to the given transaction. There is no `(*QuotaRepository).AddMemoryMB`
// method on the outer repo — this type is the only way to call
// AddMemoryMB, and it is only constructible from a tx. Positive delta =
// activate / promote; negative delta = rollback.
func NewMemoryQuotaRepository(tx *sqlx.Tx) *MemoryQuotaRepository {
	return &MemoryQuotaRepository{tx: tx}
}

// AddMemoryMB atomically accumulates a signed delta into used_memory_mb
// and returns the updated quota row (issue #44, part 2). Unlike
// AddOutboundBytes / AddRequestCount this counter is NOT month-bounded —
// the cap is per-tenant-aggregate, not per-month, so the lazy-rollover
// CASE against quota_period_start would be wrong. Implemented as a
// direct UPDATE rather than routed through addColumn for the same
// reason. Must run inside a tx so the counter mutation is atomic with
// the active_deployments row mutation that triggered it.
func (r *MemoryQuotaRepository) AddMemoryMB(ctx context.Context, tenantID string, delta int64) (*domain.Quota, error) {
	var q domain.Quota
	err := r.tx.GetContext(ctx, &q, `
		UPDATE quotas SET used_memory_mb = used_memory_mb + $2
		WHERE tenant_id = $1
		RETURNING `+quotaColumnList,
		tenantID, delta)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &q, err
}

func (r *QuotaRepository) Update(ctx context.Context, q *domain.Quota) error {
	// Operator-driven update of the cap columns. The `used_*`
	// counters are intentionally NOT in the SET clause: they are
	// the platform's view of current consumption and must never be
	// drift-corrected by an operator writing through this path.
	// Use the dedicated AddOutboundBytes / AddRequestCount /
	// AddMemoryMB paths (or, in an emergency, a hand-written
	// UPDATE on the row) to nudge a counter — never this method.
	query := `UPDATE quotas SET max_deployments = $2, max_apps = $3, max_workers = $4, max_memory_mb = $5, max_outbound_mb = $6, max_requests_per_month = $7 WHERE tenant_id = $1`
	_, err := r.db.ExecContext(ctx, query, q.TenantID, q.MaxDeployments, q.MaxApps, q.MaxWorkers, q.MaxMemoryMB, q.MaxOutboundMB, q.MaxRequestsPerMonth)
	return err
}

// VerifyUnderCap is the deploy-time gate (issue #420). Returns true iff
// accepting the deploy would not push the tenant over the
// max_requests_per_month or max_outbound_mb cap on the very next request
// burst. MaxX < 0 is the unlimited sentinel; the WHERE clause treats it as
// "always passes".
//
// Semantics of the projection parameters:
//
//   - Pass the projected delta you want to gate on (e.g. 1 for the
//     deploy's first inbound call, plus a byte estimate if known).
//   - Pass 0 to SKIP that dimension entirely. The WHERE short-circuits
//     to TRUE on `$N = 0` so the dimension is not enforced. This is the
//     right knob when the caller doesn't know the projection (e.g. an
//     admin override that wants to test the OTHER axis), and it's the
//     reason the deploy-time gate can be called with (1, 0) — the
//     heartbeat pipeline is the real-time enforcement for outbound
//     bytes, and the request-time 402 at edge-ingress is the
//     user-facing backstop (see internal/handler/quota.go:GetQuotaInternal).
//   - Pass -1 to gate the dimension as "no slack" — equivalent to
//     passing `max_* - used_*` rounded up. Not currently used but
//     documented for future tests.
//
// We mutate the row by adding 0 so the row gets a write-lock without
// actually moving the counter. The heartbeat path is the only writer of
// used_*; the deploy-time path is verify-only. A concurrent heartbeat
// that lands between our SELECT and our UPDATE could push the counter
// over — that's acceptable: the caller's *next* deploy will catch it, and
// the request-time gate at edge-ingress is the user-facing backstop.
func (r *QuotaRepository) VerifyUnderCap(ctx context.Context, tenantID string, projectedRequests, projectedOutboundBytes int64) (bool, error) {
	var tenant string
	query := `
		UPDATE quotas
		SET used_request_count  = used_request_count  + 0,
		    used_outbound_bytes = used_outbound_bytes + 0
		WHERE tenant_id = $1
		  AND ($2 = 0 OR max_requests_per_month = -1
		                  OR used_request_count + $2 <= max_requests_per_month)
		  AND ($3 = 0 OR max_outbound_mb = -1
		                  OR used_outbound_bytes + $3 <= max_outbound_mb)
		RETURNING tenant_id`
	err := r.db.GetContext(ctx, &tenant, query, tenantID, projectedRequests, projectedOutboundBytes)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// VerifyMemoryUnderCap is the deploy-time gate for the
// MaxMemoryMB / aggregate-used_memory_mb cap (issue #44, part 2).
// Returns true iff accepting a deploy whose per-app memory footprint is
// perAppMemoryMB would not push the tenant over quota.MaxMemoryMB.
// max_memory_mb < 0 is the unlimited sentinel (enterprise plan) and
// short-circuits to TRUE; max_memory_mb == 0 / unset / admin-cleared
// falls through (the caller computes the per-app hint via the same
// buildPublishPayload ladder and picks up the default 256 fallback).
//
// Like VerifyUnderCap, mutates the row by +0 so a concurrent heartbeat
// that bumps the counter can't slip in between our SELECT and our
// UPDATE. A concurrent deploy that lands between our verify-return and
// the activate-time increment may over-accept at the gate; the next
// deploy catches the over-cap state. The request-time 402 at
// edge-ingress is the user-facing backstop.
func (r *QuotaRepository) VerifyMemoryUnderCap(ctx context.Context, tenantID string, perAppMemoryMB int64) (bool, error) {
	var tenant string
	query := `
		UPDATE quotas
		SET used_memory_mb = used_memory_mb + 0
		WHERE tenant_id = $1
		  AND (max_memory_mb = -1 OR used_memory_mb + $2 <= max_memory_mb)
		RETURNING tenant_id`
	err := r.db.GetContext(ctx, &tenant, query, tenantID, perAppMemoryMB)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// SetGraceUntil stamps quotas.quota_lock_grace_until for a free-tier
// first-cross (issue #420). Called by the heartbeat applyTenantDelta
// path the first time a free-tier tenant crosses a cap. The deploy-time
// gate still rejects new deploys while the grace clock is running; the
// request-time gate (edge-ingress) starts serving 402 only after the
// timestamp expires. Pass nil to clear (operator reset via the admin
// quota-override endpoint).
func (r *QuotaRepository) SetGraceUntil(ctx context.Context, tenantID string, until *time.Time) error {
	var v interface{}
	if until != nil {
		v = *until
	}
	_, err := r.db.ExecContext(ctx,
		`UPDATE quotas SET quota_lock_grace_until = $2 WHERE tenant_id = $1`,
		tenantID, v)
	return err
}

// GetRateLimit fetches the per-tenant rate-limit columns (issue #305)
// for the ingress TenantRateLimitCache fetcher. Returns (nil, nil)
// when the tenant has no quotas row — the ingress treats that as
// "no caps, feature disabled for this tenant" and the renderer skips
// emitting a rate_limit route (fail-open, same shape as the quota
// 402 cache at issue #420). The ingress does NOT want the full
// domain.Quota projection (counter fields, period start, grace
// timestamp — all noise to it), so this query projects only the five
// rate-limit columns the wire shape carries.
func (r *QuotaRepository) GetRateLimit(ctx context.Context, tenantID string) (*domain.TenantRateLimitResponse, error) {
	var rl domain.TenantRateLimitResponse
	err := r.db.GetContext(ctx, &rl, `
		SELECT tenant_id,
		       tenant_rate_limit_rps,
		       tenant_rate_limit_burst,
		       tenant_concurrent_limit,
		       tenant_bandwidth_bps
		  FROM quotas
		 WHERE tenant_id = $1`, tenantID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &rl, err
}

// SetRateLimit upserts the per-tenant rate-limit columns (issue #305).
// The handler validates that all four fields are non-negative before
// reaching this method; the WHERE clause enforces the row must exist
// (a tenant with no quotas row cannot be rate-limit-configured — the
// handler must call QuotaRepository.Create or provision the tenant
// via the existing tenant-create path first). Returns the post-write
// row, which the handler echoes back in the response body so the
// operator sees the exact stored values (including the
// tenant_rate_limit_set_at audit timestamp that this method stamps
// to NOW()).
//
// audit-log writing is the caller's responsibility (handler-level,
// not repo-level — same shape as the existing quota-override flow at
// internal/handler/tenant.go). The repo intentionally does not touch
// audit_logs.
func (r *QuotaRepository) SetRateLimit(ctx context.Context, tenantID string, req domain.TenantRateLimitRequest) (*domain.TenantRateLimitResponse, error) {
	var rl domain.TenantRateLimitResponse
	err := r.db.GetContext(ctx, &rl, `
		UPDATE quotas SET
		       tenant_rate_limit_rps     = $2,
		       tenant_rate_limit_burst   = $3,
		       tenant_concurrent_limit   = $4,
		       tenant_bandwidth_bps      = $5,
		       tenant_rate_limit_set_at  = NOW()
		 WHERE tenant_id = $1
		 RETURNING tenant_id,
		           tenant_rate_limit_rps,
		           tenant_rate_limit_burst,
		           tenant_concurrent_limit,
		           tenant_bandwidth_bps`,
		tenantID, req.RPS, req.Burst, req.ConcurrentLimit, req.BandwidthBPS)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &rl, err
}
