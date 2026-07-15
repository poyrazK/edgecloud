package service

import (
	"context"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// fakeWebhookSvc captures every PublishEvent call so tests can assert
// that RollbackDeployment fires the right event + payload under the
// right trigger (issue #84 ask 7: emit `auto_rollback` when triggered
// by the worker, `rollback` when triggered by the operator).
//
// The production WebhookService implements WebhookServiceInterface, so
// the production wiring at app.go:325 is unchanged. This fake only
// implements PublishEvent because RollbackDeployment is the only
// service consumer and that's the only method it calls. The
// compile-time assertion at the bottom of this file catches a future
// drift where the interface gains a method or the fake is forgotten.
type fakeWebhookSvc struct {
	// Embed the production interface so the fake satisfies all
	// current methods (Create, ListByTenant, GetByID, Update,
	// Delete, ListDeliveriesByWebhook) without per-method stubs.
	// RollbackDeployment only calls PublishEvent; if a future
	// service starts using another method, the embedded nil
	// will panic with a clear "called on nil receiver" trace
	// rather than failing at compile time.
	WebhookServiceInterface
	mu    sync.Mutex
	calls []fakeWebhookCall
}

type fakeWebhookCall struct {
	tenantID  string
	appName   string
	eventType string
	payload   map[string]string
}

func (f *fakeWebhookSvc) PublishEvent(_ context.Context, tenantID, appName, eventType string, payload interface{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, _ := payload.(map[string]string)
	f.calls = append(f.calls, fakeWebhookCall{
		tenantID:  tenantID,
		appName:   appName,
		eventType: eventType,
		payload:   p,
	})
}

// Compile-time check that fakeWebhookSvc satisfies the production
// interface. Catches a future drift where the interface gains a
// method and the fake is forgotten.
var _ WebhookServiceInterface = (*fakeWebhookSvc)(nil)

func (f *fakeWebhookSvc) last() (fakeWebhookCall, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return fakeWebhookCall{}, false
	}
	return f.calls[len(f.calls)-1], true
}

func (f *fakeWebhookSvc) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// TestRollbackDeployment_AutoTriggeredEmitsAutoRollbackEvent pins
// issue #84 ask 7: when the rollback path is triggered by the worker
// (POST /api/internal/apps/{appName}/auto-rollback → handler passes
// isAutoTriggered=true), the service must emit the `auto_rollback`
// webhook with trigger=worker_crash and canary=false.
//
// The integration coverage of the SQL tx is in
// TestRollbackDeployment_NormalTenant_Proceeds
// (deployment_regions_test.go:1495); this test focuses on the
// post-commit webhook emit by running the same happy-path SQL and
// asserting on the fake sink. The test is parameterized over
// isAutoTriggered so the manual rollback contract is pinned in the
// same file.
func TestRollbackDeployment_AutoTriggeredEmitsAutoRollbackEvent(t *testing.T) {
	for _, tc := range []struct {
		name            string
		isAutoTriggered bool
		wantEvent       string
		wantTrigger     string
		wantCanary      string // "" means the key must be ABSENT
	}{
		{
			name:            "auto_triggered_emits_auto_rollback",
			isAutoTriggered: true,
			wantEvent:       "auto_rollback",
			wantTrigger:     "worker_crash",
			wantCanary:      "false",
		},
		{
			name:            "manual_triggered_emits_rollback",
			isAutoTriggered: false,
			wantEvent:       "rollback",
			wantTrigger:     "operator",
			wantCanary:      "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeWebhookSvc{}
			pub := newRecordingPublisher()
			svc, drainer, mock, cleanup := activateSvcForTest(t, pub, "global")
			defer cleanup()
			svc.SetWebhookService(fake)

			const (
				tenantID           = "t_rollback_webhook"
				appName            = "myapp"
				activeDeploymentID = "d_active"
				lastGoodID         = "d_last_good"
				deploymentHash     = "hash_last_good"
			)
			now := time.Now()

			// SQL trace mirrors
			// TestRollbackDeployment_NormalTenant_Proceeds
			// (deployment_regions_test.go:1495) — the happy
			// path that reaches the post-tx webhook emit block.
			mock.ExpectBegin()
			expectTenantForUpdateOK(mock, tenantID)
			mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
				WithArgs(tenantID, appName).
				WillReturnRows(sqlmock.NewRows([]string{
					"tenant_id", "app_name", "deployment_id",
					"last_good_deployment_id", "auto_rollback_enabled", "stable_since",
					"regions_published", "regions_failed", "regions_cached",
					"regions_cache_failed", "last_publish_at", "last_publish_attempt_id",
					"preview_id", "preview_pr_number",
				}).AddRow(
					tenantID, appName, activeDeploymentID, lastGoodID, false, nil,
					"{}", "{}", "{}", "{}",
					nil, nil, nil, nil,
				))
			mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
				WithArgs(lastGoodID).
				WillReturnRows(sqlmock.NewRows([]string{
					"id", "tenant_id", "app_name", "status", "hash", "regions",
					"created_at", "auto_rollback_enabled", "signature", "signing_key_id",
					"build_attestation", "desired_replicas", "preview_id",
					"preview_pr_number", "preview_expires_at",
				}).AddRow(
					lastGoodID, tenantID, appName, domain.StatusDeployed, deploymentHash,
					`{"us-east"}`, now, false, "", "", []byte("null"), 0, nil, nil, nil,
				))
			mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, max_resident_seconds_per_month, max_compute_ms_per_month, used_outbound_bytes, used_request_count, used_memory_mb, used_resident_seconds, used_compute_ms, quota_period_start, quota_lock_grace_until`) + `.*FROM quotas WHERE tenant_id =`).
				WithArgs(tenantID).
				WillReturnRows(sqlmock.NewRows([]string{
					"tenant_id", "max_deployments", "max_apps", "max_workers",
					"max_memory_mb", "max_outbound_mb", "max_requests_per_month",
					"max_resident_seconds_per_month", "max_compute_ms_per_month", "used_outbound_bytes",
					"used_request_count", "used_memory_mb", "used_resident_seconds", "used_compute_ms",
					"quota_period_start", "quota_lock_grace_until",
				}).AddRow(tenantID, 100, 50, 10, 256, 1024, 100_000, 0, 0, 0, 0, 0, 0, 0, now, nil))
			mock.ExpectExec(`INSERT INTO active_deployments`).
				WithArgs(tenantID, appName, lastGoodID, nil,
					sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
					sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
					sqlmock.AnyArg(), sqlmock.AnyArg(), 0).
				WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
				WithArgs(tenantID, appName).
				WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
				WithArgs(tenantID, appName).
				WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
			expectInTxOutboxInsert(mock, tenantID, appName)
			mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_memory_mb = used_memory_mb + $2 WHERE tenant_id = $1`)).
				WithArgs(tenantID, int64(256)).
				WillReturnRows(sqlmock.NewRows([]string{
					"tenant_id", "max_deployments", "max_apps", "max_workers",
					"max_memory_mb", "max_outbound_mb", "max_requests_per_month",
					"max_resident_seconds_per_month", "max_compute_ms_per_month", "used_outbound_bytes",
					"used_request_count", "used_memory_mb", "used_resident_seconds", "used_compute_ms",
					"quota_period_start", "quota_lock_grace_until",
				}).AddRow(tenantID, 100, 50, 10, 256, 1024, 100_000, 0, 0, 0, 0, 256, 0, 0, now, nil))
			mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_memory_mb = used_memory_mb + $2 WHERE tenant_id = $1`)).
				WithArgs(tenantID, int64(-256)).
				WillReturnRows(sqlmock.NewRows([]string{
					"tenant_id", "max_deployments", "max_apps", "max_workers",
					"max_memory_mb", "max_outbound_mb", "max_requests_per_month",
					"max_resident_seconds_per_month", "max_compute_ms_per_month", "used_outbound_bytes",
					"used_request_count", "used_memory_mb", "used_resident_seconds", "used_compute_ms",
					"quota_period_start", "quota_lock_grace_until",
				}).AddRow(tenantID, 100, 50, 10, 256, 1024, 100_000, 0, 0, 0, 0, 0, 0, 0, now, nil))
			mock.ExpectCommit()

			// Drive the rollback. publishSwap is a no-op without
			// cachePusher; the webhook emit happens immediately
			// after publishSwap, before publishBuilder runs.
			if _, err := svc.RollbackDeployment(context.Background(), tenantID, appName, "", tc.isAutoTriggered); err != nil {
				t.Fatalf("RollbackDeployment: %v", err)
			}

			// Drainer tick to clear the outbox so sqlmock has no
			// pending expectations.
			expectDrainerTickSuccess(t, mock, tenantID, appName, lastGoodID,
				[]string{"us-east"}, 256)
			drainer.Tick(context.Background())

			// Assertion: webhook emit happened exactly once with
			// the right event type + payload.
			if got := fake.count(); got != 1 {
				t.Fatalf("webhook PublishEvent calls = %d, want 1", got)
			}
			call, ok := fake.last()
			if !ok {
				t.Fatal("fake.last returned false despite count==1")
			}
			if call.tenantID != tenantID {
				t.Errorf("webhook tenantID = %q, want %q", call.tenantID, tenantID)
			}
			if call.appName != appName {
				t.Errorf("webhook appName = %q, want %q", call.appName, appName)
			}
			if call.eventType != tc.wantEvent {
				t.Errorf("webhook eventType = %q, want %q (isAutoTriggered=%v)",
					call.eventType, tc.wantEvent, tc.isAutoTriggered)
			}
			if got := call.payload["deployment_id"]; got != lastGoodID {
				t.Errorf("webhook payload.deployment_id = %q, want %q", got, lastGoodID)
			}
			if got := call.payload["trigger"]; got != tc.wantTrigger {
				t.Errorf("webhook payload.trigger = %q, want %q (isAutoTriggered=%v)",
					got, tc.wantTrigger, tc.isAutoTriggered)
			}
			gotCanary, hasCanary := call.payload["canary"]
			if tc.wantCanary == "" {
				if hasCanary {
					t.Errorf("webhook payload.canary = %q, want field absent (manual rollback)",
						gotCanary)
				}
			} else {
				if !hasCanary {
					t.Errorf("webhook payload.canary absent, want %q", tc.wantCanary)
				} else if gotCanary != tc.wantCanary {
					t.Errorf("webhook payload.canary = %q, want %q", gotCanary, tc.wantCanary)
				}
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("sqlmock expectations: %v", err)
			}
		})
	}
}
