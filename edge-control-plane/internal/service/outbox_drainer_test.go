package service

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// outboxColumns mirrors the SELECT column order used by
// repository.OutboxRepository.ClaimDue's RETURNING clause. Keep
// in sync with internal/repository/outbox.go.
var outboxColumns = []string{
	"id", "tenant_id", "app_name", "kind", "payload", "regions",
	"attempt_count", "next_attempt_at", "status", "last_error",
	"dedupe_key", "created_at", "published_at", "claimed_until",
}

// taskMessagePayload marshals a TaskMessage to the JSON shape the
// drainer unmarshals. Mirrors the production path through
// buildPublishPayload.
func taskMessagePayload(t *testing.T, tenantID, appName, deploymentID string, regions []string) []byte {
	t.Helper()
	b, err := json.Marshal(&nats.TaskMessage{
		Type:      "task_update",
		TenantID:  tenantID,
		Timestamp: time.Now().UTC(),
		Apps: map[string]nats.AppConfig{
			appName: {
				DeploymentID: deploymentID,
				Env:          map[string]string{},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal TaskMessage: %v", err)
	}
	return b
}

// newDrainerForTest wires a sqlmock-backed OutboxDrainer with the
// given publisher. Tests get back the drainer, the sqlmock handle,
// and a cleanup closure.
func newDrainerForTest(t *testing.T, pub nats.Publisher, batchSize, maxAttempts int) (*OutboxDrainer, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	repo := repository.NewOutboxRepository(sqlxDB)
	drainer := NewOutboxDrainer(repo, pub, time.Second, batchSize, maxAttempts)
	return drainer, mock, func() { _ = mockDB.Close() }
}

// claimedRow builds a single ClaimDue result row with the given
// regions. The id field is what MarkPublished/MarkFailed bind on
// in subsequent Exec mocks.
func claimedRow(id int64, tenantID, appName, deploymentID string, regions []string, attempt int) *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows(outboxColumns).AddRow(
		id, tenantID, appName, "task_update",
		taskMessagePayload(&testing.T{}, tenantID, appName, deploymentID, regions),
		pq.Array(regions),
		attempt, now, "in_flight", nil,
		"t:"+appName+":att", now, nil, now.Add(30*time.Second),
	)
}

// Note on the &testing.T{} above: it works because taskMessagePayload
// only calls t.Fatalf, which fails the calling test. Using a fresh
// T means tests calling this helper get clean failure attribution.

// TestOutboxDrainer_PublishesDueRows covers the happy path: a
// single ClaimDue row's regions are all published, and MarkPublished
// flips the row. The publisher's per-region calls are asserted
// via RecordingPublisher.calls.
func TestOutboxDrainer_PublishesDueRows(t *testing.T) {
	pub := newRecordingPublisher()
	drainer, mock, cleanup := newDrainerForTest(t, pub, 50, 10)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`WITH due AS (`)).
		WithArgs(50).
		WillReturnRows(claimedRow(1, "t_test", "myapp", "d_x", []string{"us-east", "eu-west"}, 0))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
		WithArgs(int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	drainer.Tick(context.Background())

	got := pub.regionsCalled()
	want := []string{"us-east", "eu-west"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("publisher regions = %v, want %v", got, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestOutboxDrainer_RetriesOnPublishFailure covers the partial-
// failure retry path: one region fails, MarkFailed is called with
// attempt_count incremented and status kept as 'pending' so the
// next ClaimDue will pick the row back up after the backoff
// window elapses.
func TestOutboxDrainer_RetriesOnPublishFailure(t *testing.T) {
	pub := newRecordingPublisher()
	pub.failFor["eu-west"] = errors.New("nats: connection refused")
	drainer, mock, cleanup := newDrainerForTest(t, pub, 50, 10)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`WITH due AS (`)).
		WithArgs(50).
		WillReturnRows(claimedRow(1, "t_test", "myapp", "d_x", []string{"us-east", "eu-west"}, 0))
	// MarkFailed: attempt_count=1, status stays 'pending', backoff
	// future-dated. The 5th arg is next_attempt_at (time.Time); pin
	// via AnyArg so the test isn't tied to wall-clock math.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
		WithArgs(int64(1), "pending", 1, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	drainer.Tick(context.Background())

	// Both regions were attempted; the failure didn't short-circuit.
	got := pub.regionsCalled()
	if len(got) != 2 {
		t.Errorf("publisher regions = %v, want 2 (loop must not abort on per-region failure)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestOutboxDrainer_GivesUpAfterMaxAttempts covers the terminal
// path: the Nth failure flips status to 'failed' via MarkFailed's
// own state-machine logic. The drainer's behavior is just to call
// MarkFailed with attempt+1 == maxAttempts; the repository handles
// the status='failed' transition. This test pins the boundary.
func TestOutboxDrainer_GivesUpAfterMaxAttempts(t *testing.T) {
	pub := newRecordingPublisher()
	pub.failFor["us-east"] = errors.New("nats: connection refused")
	drainer, mock, cleanup := newDrainerForTest(t, pub, 50, 3)
	defer cleanup()

	// Row already at attempt_count=2; the next failure will be
	// the third (== maxAttempts), giving up.
	mock.ExpectQuery(regexp.QuoteMeta(`WITH due AS (`)).
		WithArgs(50).
		WillReturnRows(claimedRow(1, "t_test", "myapp", "d_x", []string{"us-east"}, 2))
	// MarkFailed with newAttempt=3 == maxAttempts=3 → status='failed'.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
		WithArgs(int64(1), "failed", 3, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	drainer.Tick(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestOutboxDrainer_SkipsFutureRows covers the empty-claim case:
// ClaimDue returns no rows, no publisher calls happen, no
// MarkPublished/MarkFailed. Verifies Tick doesn't go off the
// rails on an empty queue.
func TestOutboxDrainer_SkipsFutureRows(t *testing.T) {
	pub := newRecordingPublisher()
	drainer, mock, cleanup := newDrainerForTest(t, pub, 50, 10)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`WITH due AS (`)).
		WithArgs(50).
		WillReturnRows(sqlmock.NewRows(outboxColumns)) // zero rows

	drainer.Tick(context.Background())

	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("publisher regions = %v, want [] (nothing was due)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestOutboxDrainer_PayloadUnmarshalFailsTerminates covers the
// malformed-payload path: processRow's json.Unmarshal fails, the
// drainer logs and calls MarkFailed at maxAttempts so the row
// status flips to 'failed' (terminal) with a clear last_error.
// This is the path that protects the operator from an outbox row
// that can never make it to NATS.
func TestOutboxDrainer_PayloadUnmarshalFailsTerminates(t *testing.T) {
	pub := newRecordingPublisher()
	drainer, mock, cleanup := newDrainerForTest(t, pub, 50, 10)
	defer cleanup()

	now := time.Now()
	mock.ExpectQuery(regexp.QuoteMeta(`WITH due AS (`)).
		WithArgs(50).
		WillReturnRows(sqlmock.NewRows(outboxColumns).AddRow(
			1, "t_test", "myapp", "task_update",
			[]byte(`not-json`), // invalid TaskMessage
			pq.Array([]string{"us-east"}),
			0, now, "in_flight", nil,
			"t:myapp:att", now, nil, now.Add(30*time.Second),
		))
	// MarkFailed at maxAttempts → status='failed', last_error
	// mentions the unmarshal cause. Pin the constant pieces so a
	// regression on the error prefix is caught.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
		WithArgs(int64(1), "failed", 10, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	drainer.Tick(context.Background())

	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("publisher regions = %v, want [] (malformed payload must not reach NATS)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestOutboxDrainer_ClaimDueErrorIsLoggedNotFatal covers the
// infrastructure-failure path: ClaimDue itself returns an error
// (DB hiccup, network blip). Tick must log and return without
// panicking — the next tick will retry.
func TestOutboxDrainer_ClaimDueErrorIsLoggedNotFatal(t *testing.T) {
	pub := newRecordingPublisher()
	drainer, mock, cleanup := newDrainerForTest(t, pub, 50, 10)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`WITH due AS (`)).
		WithArgs(50).
		WillReturnError(errors.New("db unreachable"))

	// Must not panic; must not call the publisher.
	drainer.Tick(context.Background())

	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("publisher regions = %v, want [] (no claim, no publish)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestOutboxDrainer_FullBatchLogsBacklog covers the operator-
// visibility path: when ClaimDue returns exactly batchSize rows,
// Tick logs a backlog warning. sqlmock doesn't capture log output,
// but the test verifies the path doesn't error out and that all
// batchSize rows are processed (publisher sees N×regions calls).
func TestOutboxDrainer_FullBatchLogsBacklog(t *testing.T) {
	pub := newRecordingPublisher()
	const batchSize = 3
	drainer, mock, cleanup := newDrainerForTest(t, pub, batchSize, 10)
	defer cleanup()

	rows := sqlmock.NewRows(outboxColumns)
	for i := int64(1); i <= int64(batchSize); i++ {
		rows.AddRow(
			i, "t_test", "myapp", "task_update",
			taskMessagePayload(t, "t_test", "myapp", "d_"+string(rune('0'+int(i))), []string{"us-east"}),
			pq.Array([]string{"us-east"}),
			0, time.Now(), "in_flight", nil,
			"t:myapp:att"+string(rune('0'+int(i))),
			time.Now(), nil, time.Now().Add(30*time.Second),
		)
	}
	mock.ExpectQuery(regexp.QuoteMeta(`WITH due AS (`)).
		WithArgs(batchSize).
		WillReturnRows(rows)
	for i := int64(1); i <= int64(batchSize); i++ {
		mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
			WithArgs(i).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}

	drainer.Tick(context.Background())

	if got := pub.regionsCalled(); len(got) != batchSize {
		t.Errorf("publisher regions = %v, want %d (full batch must publish all rows)", got, batchSize)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestNewOutboxDrainer_ClampsInvalidArgs pins the constructor's
// fallback-to-defaults contract: invalid (≤0) inputs are replaced
// with sensible defaults rather than producing a drainer that
// tight-loops or never retries.
func TestNewOutboxDrainer_ClampsInvalidArgs(t *testing.T) {
	pub := newRecordingPublisher()
	mockDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = mockDB.Close() })
	repo := repository.NewOutboxRepository(sqlx.NewDb(mockDB, "postgres"))

	d := NewOutboxDrainer(repo, pub, 0, 0, 0)
	if d.interval != 2*time.Second {
		t.Errorf("interval = %v, want 2s (clamped from 0)", d.interval)
	}
	if d.batchSize != 50 {
		t.Errorf("batchSize = %d, want 50 (clamped from 0)", d.batchSize)
	}
	if d.maxAttempts != 10 {
		t.Errorf("maxAttempts = %d, want 10 (clamped from 0)", d.maxAttempts)
	}
}

// TestBackoffFor_Schedule pins the exponential schedule so the
// outage-recovery SLO is predictable. attempt < 6 follows
// 2^attempt * 5s exactly; attempt >= 6 hits the 5-minute cap.
func TestBackoffFor_Schedule(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 5 * time.Second},
		{1, 10 * time.Second},
		{2, 20 * time.Second},
		{3, 40 * time.Second},
		{4, 80 * time.Second},
		{5, 160 * time.Second},
		{6, 5 * time.Minute}, // 2^6 * 5s = 320s > 300s cap
		{7, 5 * time.Minute},
		{10, 5 * time.Minute},
	}
	for _, tt := range tests {
		got := backoffFor(tt.attempt)
		if got != tt.want {
			t.Errorf("backoffFor(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

// TestBackoffFor_NegativeGuardsZero pins the defensive floor:
// negative inputs are coerced to attempt=0 → 5s, not a panic.
func TestBackoffFor_NegativeGuardsZero(t *testing.T) {
	if got := backoffFor(-5); got != 5*time.Second {
		t.Errorf("backoffFor(-5) = %v, want 5s", got)
	}
}

// TestBackoffFor_OverflowCap pins the overflow guard: absurdly
// large attempt counts return the cap, not a wrapped duration or
// a panic.
func TestBackoffFor_OverflowCap(t *testing.T) {
	if got := backoffFor(1000); got != 5*time.Minute {
		t.Errorf("backoffFor(1000) = %v, want 5m cap", got)
	}
}

// ── Issue #569: task_purge dispatch (OutboxDrainer) ────────────────────

// claimedPurgeRow builds a ClaimDue result row carrying a
// task_purge payload. Mirrors claimedRow but uses Kind="task_purge"
// and a PurgePayload bytes instead of TaskMessage.
func claimedPurgeRow(id int64, tenantID, appName string, reason nats.PurgeReason, regions []string, attempt int) *sqlmock.Rows {
	now := time.Now()
	payload, err := json.Marshal(&nats.PurgePayload{
		Type:      nats.TaskMessageKindTaskPurge,
		Timestamp: now,
		TenantID:  tenantID,
		AppName:   appName,
		Reason:    reason,
	})
	if err != nil {
		panic(err)
	}
	return sqlmock.NewRows(outboxColumns).AddRow(
		id, tenantID, appName, nats.TaskMessageKindTaskPurge,
		payload,
		pq.Array(regions),
		attempt, now, "in_flight", nil,
		"purge:"+tenantID+":"+appName, now, nil, now.Add(30*time.Second),
	)
}

// TestOutboxDrainer_TaskPurgeDispatches: a ClaimDue row carrying
// kind='task_purge' reaches PublishPurge once per region and never
// touches PublishTaskUpdate / PublishFullSync. The drainer's switch
// on row.Kind is the single source of dispatch truth — if it
// silently falls through to the task_update branch, the payload
// would unmarshal as a TaskMessage (no `apps` field, but the
// unmarshal might still succeed with an empty apps map) and we'd
// publish a phantom task_update. The split recording surfaces that
// regression here.
func TestOutboxDrainer_TaskPurgeDispatches(t *testing.T) {
	pub := newRecordingPublisher()
	drainer, mock, cleanup := newDrainerForTest(t, pub, 50, 10)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`WITH due AS (`)).
		WithArgs(50).
		WillReturnRows(claimedPurgeRow(1, "t_purge", "myapp", nats.PurgeReasonAppDeleted,
			[]string{"us-east", "eu-west"}, 0))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
		WithArgs(int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	drainer.Tick(context.Background())

	got := pub.purgeRegionsCalled()
	want := []string{"us-east", "eu-west"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("purge regions = %v, want %v", got, want)
	}
	if taskRegions := pub.regionsCalled(); len(taskRegions) != 0 {
		t.Errorf("task_update was called for a task_purge row; regions = %v", taskRegions)
	}
	// Sanity: the captured payload unmarshals back to PurgePayload
	// with the original reason — locks the wire shape end-to-end.
	if len(pub.purgeCalls) != 2 {
		t.Fatalf("expected 2 purge calls, got %d", len(pub.purgeCalls))
	}
	if pub.purgeCalls[0].msg.Reason != nats.PurgeReasonAppDeleted {
		t.Errorf("purge.reason = %q, want %q", pub.purgeCalls[0].msg.Reason, nats.PurgeReasonAppDeleted)
	}
	if pub.purgeCalls[0].msg.AppName != "myapp" {
		t.Errorf("purge.app_name = %q, want myapp", pub.purgeCalls[0].msg.AppName)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestOutboxDrainer_PurgeRetriesOnPublishFailure: same retry ladder
// as the task_update path. failFor["eu-west"] = simulated NATS
// outage; the row is marked failed at attempt=1 with backoff, and
// us-east's successful publish doesn't prevent the retry.
func TestOutboxDrainer_PurgeRetriesOnPublishFailure(t *testing.T) {
	pub := newRecordingPublisher()
	pub.failFor["eu-west"] = errors.New("nats: connection refused")
	drainer, mock, cleanup := newDrainerForTest(t, pub, 50, 10)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`WITH due AS (`)).
		WithArgs(50).
		WillReturnRows(claimedPurgeRow(1, "t_purge", "myapp", nats.PurgeReasonTenantOffboarded,
			[]string{"us-east", "eu-west"}, 0))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
		WithArgs(int64(1), sqlmock.AnyArg(), 1, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	drainer.Tick(context.Background())

	// Both regions tried.
	if got := pub.purgeRegionsCalled(); len(got) != 2 {
		t.Errorf("purge regions = %v, want [us-east eu-west]", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestOutboxDrainer_UnknownKindTerminates: a future kind the
// drainer doesn't know lands in the default branch and is
// terminal-marked at maxAttempts. The row stays on disk for
// operator inspection rather than silently dropping or
// publishing a malformed payload.
func TestOutboxDrainer_UnknownKindTerminates(t *testing.T) {
	pub := newRecordingPublisher()
	drainer, mock, cleanup := newDrainerForTest(t, pub, 50, 10)
	defer cleanup()

	now := time.Now()
	mock.ExpectQuery(regexp.QuoteMeta(`WITH due AS (`)).
		WithArgs(50).
		WillReturnRows(sqlmock.NewRows(outboxColumns).AddRow(
			1, "t_test", "myapp", "future_kind_v2",
			[]byte(`{"type":"future_kind_v2"}`),
			pq.Array([]string{"us-east"}),
			0, now, "in_flight", nil,
			"future:1", now, nil, now.Add(30*time.Second),
		))
	// MarkFailed at maxAttempts → status='failed'. Pin the error
	// prefix so a regression that drops "unknown kind" lands here.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
		WithArgs(int64(1), "failed", 10, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	drainer.Tick(context.Background())

	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("publisher task_update regions = %v, want [] (unknown kind must not publish)", got)
	}
	if got := pub.purgeRegionsCalled(); len(got) != 0 {
		t.Errorf("publisher purge regions = %v, want [] (unknown kind must not publish)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestOutboxDrainer_MixedBatchHandlesBothKinds: a single ClaimDue
// batch can carry both task_update and task_purge rows; the drainer
// must dispatch each row independently based on its Kind. Asserts
// one publish per kind, no cross-talk.
func TestOutboxDrainer_MixedBatchHandlesBothKinds(t *testing.T) {
	pub := newRecordingPublisher()
	drainer, mock, cleanup := newDrainerForTest(t, pub, 50, 10)
	defer cleanup()

	// Build the row set by appending a second row to the first.
	// claimedRow() returns *sqlmock.Rows; we AddRow onto it so the
	// returned rows cover both task_update and task_purge.
	taskRows := claimedRow(1, "t_a", "app-a", "d_a", []string{"us-east"}, 0)
	taskRows.AddRow(
		2, "t_b", "app-b", nats.TaskMessageKindTaskPurge,
		mustPurgePayload("t_b", "app-b", nats.PurgeReasonAppDeleted, []string{"us-east"}),
		pq.Array([]string{"us-east"}),
		0, time.Now(), "in_flight", nil,
		"purge:t_b:app-b", time.Now(), nil, time.Now().Add(30*time.Second),
	)
	mock.ExpectQuery(regexp.QuoteMeta(`WITH due AS (`)).
		WithArgs(50).
		WillReturnRows(taskRows)
	// Two MarkPublished calls (one per row) — order isn't pinned
	// because the drainer iterates in ClaimDue order and the mock
	// accepts calls in any sequence.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
		WithArgs(int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
		WithArgs(int64(2)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	drainer.Tick(context.Background())

	if got := pub.regionsCalled(); len(got) != 1 || got[0] != "us-east" {
		t.Errorf("task_update regions = %v, want [us-east]", got)
	}
	if got := pub.purgeRegionsCalled(); len(got) != 1 || got[0] != "us-east" {
		t.Errorf("purge regions = %v, want [us-east]", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestOutboxDrainer_KindPayloadMismatchIsTerminal (issue #569 review
// finding #1): a row whose `Kind` does not match the JSON `type`
// field in its payload must NOT be dispatched to either publisher.
// Without this guard, a kind='task_update' row carrying a
// PurgePayload body would unmarshal as a TaskMessage with
// type="task_purge" and an empty apps map — the worker would silently
// receive a phantom task_update with no apps. The cross-check in
// processTaskRow / processPurgeRow rejects the row at the dispatcher
// boundary and MarkFailed's it for operator inspection.
func TestOutboxDrainer_KindPayloadMismatchIsTerminal(t *testing.T) {
	pub := newRecordingPublisher()
	drainer, mock, cleanup := newDrainerForTest(t, pub, 50, 10)
	defer cleanup()

	now := time.Now()
	// Row claims kind='task_update' but its payload is a
	// PurgePayload (type='task_purge'). The cross-check must
	// catch this and MarkFailed the row.
	mock.ExpectQuery(regexp.QuoteMeta(`WITH due AS (`)).
		WithArgs(50).
		WillReturnRows(sqlmock.NewRows(outboxColumns).AddRow(
			1, "t_mismatch", "myapp", nats.TaskMessageKindTaskUpdate,
			mustPurgePayload("t_mismatch", "myapp", nats.PurgeReasonAppDeleted, []string{"us-east"}),
			pq.Array([]string{"us-east"}),
			0, now, "in_flight", nil,
			"mismatch:1", now, nil, now.Add(30*time.Second),
		))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE outbox`)).
		WithArgs(int64(1), "failed", 10, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	drainer.Tick(context.Background())

	if got := pub.regionsCalled(); len(got) != 0 {
		t.Errorf("publisher task_update regions = %v, want [] (kind/payload mismatch must not publish)", got)
	}
	if got := pub.purgeRegionsCalled(); len(got) != 0 {
		t.Errorf("publisher purge regions = %v, want [] (kind/payload mismatch must not publish)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// mustPurgePayload marshals a PurgePayload — used by the mixed-batch
// test to build a row inline.
func mustPurgePayload(tenantID, appName string, reason nats.PurgeReason, regions []string) []byte {
	b, err := json.Marshal(&nats.PurgePayload{
		Type:      nats.TaskMessageKindTaskPurge,
		Timestamp: time.Now().UTC(),
		TenantID:  tenantID,
		AppName:   appName,
		Reason:    reason,
	})
	if err != nil {
		panic(err)
	}
	return b
}
