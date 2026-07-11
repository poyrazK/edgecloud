package service

import (
	"context"
	"database/sql"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// TestActivateDeployment_ConcurrentRace_SameIdempotencyKey_OnePublish
// is the headline regression test for the issue #439 fix.
//
// The issue body says: two concurrent ActivateDeployment calls for the
// same (tenant, app) both succeed and each enqueue a distinct task_update
// outbox row. The OutboxDrainer relays both, workers receive two
// TaskMessage{target=B} publishes per concurrent activate, and the
// worker-side restart-count → auto-rollback path trips on a false
// rollback. The fix gates activate / promote / rollback behind an
// Idempotency-Key HTTP header: a replay with the same key and the same
// (app_name, deployment_id) short-circuits inside the tx without
// enqueueing a new outbox row.
//
// This test drives N goroutines that all call ActivateDeployment with
// the SAME idempotency key for the SAME (app, deployment), then asserts:
//
//   - All N calls return nil (every goroutine observes a successful
//     activate from its perspective — either the fresh publish or the
//     replay cache hit).
//   - The post-commit drainer publishes exactly one TaskMessage.
//     N publishes would mean every goroutine enqueued an outbox row,
//     which is the bug.
//
// The test runs under `go test -race` — it is the explicit closure for
// the issue #439 race surface that the single-goroutine
// TestActivateDeployment_IdempotencyReplay_NoOutboxRow only proves
// sequentially. The RecordingPublisher mutex
// (deployment_regions_test.go #32-44) makes this safe under -race.
//
// Implementation notes:
//
//   - sqlmock's MatchExpectationsInOrder(false) lets N concurrent
//     goroutines consume a pool of pre-staged cache-hit expectations
//     without tripping the matcher. Internal sqlmock locking (see
//     DATA-DOG/go-sqlmock/sqlmock.go) makes the mock safe to call from
//     multiple goroutines.
//
//   - We pre-stage ONE "fresh activate" sequence (GetByID + Begin +
//     tenants FOR UPDATE + idempotency Lookup (miss) +
//     active_deployments FOR UPDATE + INSERT active_deployments +
//     ClearStableSince + quotas + app_env + outbox INSERT + memory add
//     + idempotency INSERT + Commit) plus (N-1) "cache hit" replays
//     (Begin + tenants FOR UPDATE + idempotency Lookup (hit) +
//     Commit). The drainer flow (ClaimDue + per-region publish +
//     MarkPublished) is staged exactly once because exactly one outbox
//     row exists.
//
//   - Each goroutine invokes ActivateDeployment in its own context. The
//     join barrier (sync.WaitGroup) collects results; an
//     atomic.Int64 counts nil returns so a partial failure is visible
//     without killing the test.
//
//   - The test does NOT assert on goroutine interleaving — that is by
//     design. The implementation's contract is "every concurrent
//     activate with the same idempotency key produces one outbox row
//     total", regardless of which goroutine wins the tenants FOR UPDATE
//     lock first.
//
// See issue-439-activate-idempotency-pr-603 memory entry for the PR
// that introduced the production fix.
func TestActivateDeployment_ConcurrentRace_SameIdempotencyKey_OnePublish(t *testing.T) {
	pub := newRecordingPublisher()

	// We can't reuse activateSvcForTest directly because that helper
	// wires sqlmock with MatchExpectationsInOrder(true) (the default
	// for QueryMatcherRegexp). The race test requires non-strict
	// ordering because N goroutines consume the cache-hit expectation
	// pool in arrival order, which is non-deterministic across runs.
	// sqlmock.New() defaults to QueryMatcherRegexp with strict order;
	// we flip the order flag explicitly via MatchExpectationsInOrder.
	mockDB, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
	)
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	mock.MatchExpectationsInOrder(false)
	t.Cleanup(func() { _ = mockDB.Close() })

	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	outboxRepo := repository.NewOutboxRepository(sqlxDB)
	activeRepo := repository.NewActiveDeploymentRepository(sqlxDB)
	svc := &DeploymentService{
		db:             sqlxDB,
		deploymentRepo: repository.NewDeploymentRepository(sqlxDB),
		activeRepo:     activeRepo,
		appEnvRepo:     repository.NewAppEnvRepository(sqlxDB),
		tenantRepo:     repository.NewTenantRepository(sqlxDB),
		quotaRepo:      repository.NewQuotaRepository(sqlxDB),
		outboxRepo:     outboxRepo,
		publisher:      pub,
		defaultRegion:  "global",

		memoryQuotaRepo: repository.NewMemoryQuotaRepository,
	}
	drainer := NewOutboxDrainer(outboxRepo, pub, time.Second, 50, 10)
	// Wire the activate idempotency repo with the same DB so the
	// WithTx-bound Lookup / Insert calls hit the staged expectations.
	svc.SetActivateIdempotencyRepo(repository.NewActiveDeploymentIdempotencyKeyRepo(svc.db))

	const (
		deploymentID   = "d_idem_race"
		appName        = "myapp"
		tenantID       = "t_idem_race"
		deploymentHash = "idemracehash"
		idemKey        = "01234567-89ab-cdef-0123-456789abcdef"
		N              = 10
	)
	now := time.Now()

	// ----- Pre-stage: GetByID is called outside the tx by every
	// ActivateDeployment invocation. Stage N copies so each
	// goroutine gets its own.

	for i := 0; i < N; i++ {
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
			WithArgs(deploymentID).
			WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
				AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, deploymentHash, `{"us-east"}`, now, false, "", "", []byte{}, 0, nil, nil, nil))
	}

	// ----- Pre-stage: one fresh-activate tx + (N-1) cache-hit replays.

	// Fresh activate: full SQL sequence. tenant FOR UPDATE gate first,
	// then idempotency Lookup (miss → cache is empty for this key),
	// then active_deployments, then everything else.
	mock.ExpectBegin()
	expectTenantForUpdateOK(mock, tenantID)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, idempotency_key, app_name, deployment_id, created_at FROM active_deployment_idempotency_keys`)).
		WithArgs(tenantID, idemKey, int64(repository.IdempotencyTTL.Seconds())).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs(tenantID, appName).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, max_resident_seconds_per_month, used_outbound_bytes, used_request_count, used_memory_mb, used_resident_seconds, quota_period_start, quota_lock_grace_until FROM quotas WHERE tenant_id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "max_requests_per_month", "max_resident_seconds_per_month", "used_outbound_bytes", "used_request_count", "used_memory_mb", "used_resident_seconds", "quota_period_start", "quota_lock_grace_until"}).
			AddRow(tenantID, 100, 50, 10, 512, 1024, 100_000, 0, 0, 0, 0, 0, now, nil))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	expectInTxOutboxInsert(mock, tenantID, appName)
	expectInTxMemoryAdd(mock, tenantID, 512)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployment_idempotency_keys`)).
		WithArgs(tenantID, idemKey, appName, deploymentID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// (N-1) cache-hit replays. Each one is Begin → tenant FOR UPDATE
	// → idempotency Lookup (hit) → Commit. No outbox INSERT, no
	// memory add, no deployment mutation.
	for i := 0; i < N-1; i++ {
		mock.ExpectBegin()
		expectTenantForUpdateOK(mock, tenantID)
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, idempotency_key, app_name, deployment_id, created_at FROM active_deployment_idempotency_keys`)).
			WithArgs(tenantID, idemKey, int64(repository.IdempotencyTTL.Seconds())).
			WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "idempotency_key", "app_name", "deployment_id", "created_at"}).
				AddRow(tenantID, idemKey, appName, deploymentID, now))
		mock.ExpectCommit()
	}

	// Drainer flow — exactly one outbox row to relay, regardless of N.
	expectDrainerTickSuccess(t, mock, tenantID, appName, deploymentID,
		[]string{"us-east"}, 512)

	// ----- Drive N goroutines. Every call gets the same idempotency
	// key, so every call after the first is a cache hit. We do not
	// serialize — the whole point is to verify the in-tx cache check
	// holds under contention.

	var wg sync.WaitGroup
	var successes atomic.Int64
	var failures atomic.Int64
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := svc.ActivateDeployment(context.Background(), tenantID, appName, deploymentID, idemKey); err != nil {
				failures.Add(1)
				t.Errorf("ActivateDeployment: %v", err)
				return
			}
			successes.Add(1)
		}()
	}
	wg.Wait()

	// Drive a single drainer tick after every goroutine returns.
	// Exactly one PublishTaskUpdate invocation should fire (the fresh
	// activate's outbox row). The N-1 replays have no outbox row to
	// relay, so they do not contribute publishes.
	drainer.Tick(context.Background())

	pub.mu.Lock()
	gotRegions := make([]string, len(pub.calls))
	for i, c := range pub.calls {
		gotRegions[i] = c.region
	}
	pub.mu.Unlock()

	if successes.Load() != N {
		t.Errorf("successes = %d, want %d", successes.Load(), N)
	}
	if failures.Load() != 0 {
		t.Errorf("failures = %d, want 0", failures.Load())
	}
	if len(gotRegions) != 1 {
		t.Errorf("publish calls = %d, want 1 (concurrent activates with same Idempotency-Key must dedupe to one outbox row): %v",
			len(gotRegions), gotRegions)
	} else if gotRegions[0] != "us-east" {
		t.Errorf("publish region = %q, want %q", gotRegions[0], "us-east")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestActivateDeployment_ConcurrentRace_NoIdempotencyKey_PreFixBehavior
// is the regression-baseline test for the issue #439 fix.
//
// Without an Idempotency-Key header, the activate/promote/rollback
// paths fall back to the pre-#439 behavior. Pre-#439, two concurrent
// ActivateDeployment calls for the same (tenant, app) each enqueue a
// distinct task_update outbox row (the for-update locks SERIALIZE the
// two txs but do not DEDUPLICATE them), and the drainer relays both,
// producing two TaskMessage{target=B} publishes per concurrent
// activate. This test pins the bug so any future change that re-opens
// the race surface (e.g. removing the Idempotency-Key cache lookup
// from the activate tx body) trips immediately rather than silently
// regressing in production.
//
// The assertion is the inverse of the post-fix test above: N goroutines
// → N outbox rows → N drainer-driven publishes. If this assertion ever
// changes from "N publishes" to "1 publish" without a corresponding
// always-on dedupe pass, the race surface has closed accidentally —
// investigate whether the Idempotency-Key header contract is now
// obsolete, or whether another mechanism is doing the dedupe.
func TestActivateDeployment_ConcurrentRace_NoIdempotencyKey_PreFixBehavior(t *testing.T) {
	pub := newRecordingPublisher()

	mockDB, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
	)
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	mock.MatchExpectationsInOrder(false)
	t.Cleanup(func() { _ = mockDB.Close() })

	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	outboxRepo := repository.NewOutboxRepository(sqlxDB)
	activeRepo := repository.NewActiveDeploymentRepository(sqlxDB)
	svc := &DeploymentService{
		db:             sqlxDB,
		deploymentRepo: repository.NewDeploymentRepository(sqlxDB),
		activeRepo:     activeRepo,
		appEnvRepo:     repository.NewAppEnvRepository(sqlxDB),
		tenantRepo:     repository.NewTenantRepository(sqlxDB),
		quotaRepo:      repository.NewQuotaRepository(sqlxDB),
		outboxRepo:     outboxRepo,
		publisher:      pub,
		defaultRegion:  "global",

		memoryQuotaRepo: repository.NewMemoryQuotaRepository,
	}
	drainer := NewOutboxDrainer(outboxRepo, pub, time.Second, 50, 10)

	const (
		deploymentID   = "d_idem_race_nokey"
		appName        = "myapp"
		tenantID       = "t_idem_race_nokey"
		deploymentHash = "idemracenokeyhash"
		// N=5 keeps the pre-staged-mock sequence readable in test
		// failure output. The contract here is "N goroutines → N
		// publishes", not "publishes across a specific N value".
		N = 5
	)
	now := time.Now()

	// ----- Pre-stage N copies of the full activate tx sequence.
	// Every ActivateDeployment call hits GetByID outside the tx,
	// then runs the same in-tx sequence (no cache lookup because
	// idempotencyKey == "" skips the cache path entirely).

	for i := 0; i < N; i++ {
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
			WithArgs(deploymentID).
			WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
				AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, deploymentHash, `{"us-east"}`, now, false, "", "", []byte{}, 0, nil, nil, nil))
		mock.ExpectBegin()
		expectTenantForUpdateOK(mock, tenantID)
		mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
			WithArgs(tenantID, appName).
			WillReturnError(sql.ErrNoRows)
		mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, max_resident_seconds_per_month, used_outbound_bytes, used_request_count, used_memory_mb, used_resident_seconds, quota_period_start, quota_lock_grace_until FROM quotas WHERE tenant_id =`)).
			WithArgs(tenantID).
			WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "max_requests_per_month", "max_resident_seconds_per_month", "used_outbound_bytes", "used_request_count", "used_memory_mb", "used_resident_seconds", "quota_period_start", "quota_lock_grace_until"}).
				AddRow(tenantID, 100, 50, 10, 512, 1024, 100_000, 0, 0, 0, 0, 0, now, nil))
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
			WithArgs(tenantID, appName).
			WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
		expectInTxOutboxInsert(mock, tenantID, appName)
		expectInTxMemoryAdd(mock, tenantID, 512)
		mock.ExpectCommit()
	}

	// Drainer flow stages N outbox rows. ClaimDue returns them as a
	// single batch (the production drainer pulls all due rows in one
	// query), then fires one MarkPublished UPDATE per claimed row.
	mergedRows := sqlmock.NewRows([]string{
		"id", "tenant_id", "app_name", "kind", "payload", "regions",
		"attempt_count", "next_attempt_at", "status", "last_error",
		"dedupe_key", "created_at", "published_at", "claimed_until",
	})
	for i := 0; i < N; i++ {
		mergedRows.AddRow(
			i+1, tenantID, appName, "task_update",
			outboxRowPayload(t, tenantID, appName, deploymentID, 512),
			pq.Array([]string{"us-east"}),
			0, time.Now(), "in_flight", nil,
			"dedupe-"+string(rune('a'+i)), time.Now(), nil, time.Now().Add(30*time.Second),
		)
	}
	mock.ExpectQuery(regexp.QuoteMeta(`WITH due AS (`)).
		WithArgs(50).
		WillReturnRows(mergedRows)
	// drainer.Tick fires one MarkPublished UPDATE per claimed row.
	for i := 0; i < N; i++ {
		mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
			WithArgs(i + 1).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}

	// ----- Drive N goroutines, no idempotency key.

	var wg sync.WaitGroup
	var successes atomic.Int64
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := svc.ActivateDeployment(context.Background(), tenantID, appName, deploymentID, ""); err == nil {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()

	drainer.Tick(context.Background())

	pub.mu.Lock()
	gotRegions := make([]string, len(pub.calls))
	for i, c := range pub.calls {
		gotRegions[i] = c.region
	}
	pub.mu.Unlock()

	if successes.Load() != N {
		t.Errorf("successes = %d, want %d (the race surface is open: callers without an Idempotency-Key header do not get the dedupe contract — the call still succeeds, just enqueues a duplicate task_update per concurrent call)",
			successes.Load(), N)
	}
	if len(gotRegions) != N {
		t.Errorf("publish calls = %d, want %d (one outbox row per concurrent activate call — this is the pre-#439 race surface that the Idempotency-Key header closes)",
			len(gotRegions), N)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}
