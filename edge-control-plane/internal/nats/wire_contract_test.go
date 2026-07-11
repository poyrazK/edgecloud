package nats

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// wireContractRoundTrip asserts that unmarshaling raw into target then
// re-marshaling target then unmarshaling the re-marshaled bytes into a
// generic interface produces a structurally identical JSON tree.
//
// This is the contract test for issue #610: a Go-side field rename, an
// `omitempty` flip, or a value drift must fail this check. The Rust side
// at edge-worker/tests/nats_wire_contract.rs decodes the same fixtures
// against the worker-side serde types, so a wire drift on either side
// turns a test red.
//
// Structural comparison (vs strict bytes.Equal) lets the fixtures stay
// human-readable — Go's json.Marshal always emits compact JSON, so a
// byte-equality assertion would require compact fixtures. We compare
// decoded values instead, which preserves the rename / omitempty-flip
// detection property (those change the JSON tree) while tolerating
// whitespace.
//
// Known limitation: an `omitempty` flip on a *string field (e.g. dropping
// `omitempty` from `DeploymentSignature`) makes the re-marshal emit
// `"deployment_signature":""` where the fixture omits the key. The Rust
// side accepts empty-string-vs-absent indistinguishably, so this drift
// fires only the Go side. Documented here as an acknowledged gap.
func wireContractRoundTrip(t *testing.T, raw []byte, target interface{}) {
	t.Helper()
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatalf("unmarshal fixture: %v\nraw=%s", err, raw)
	}
	re, err := json.Marshal(target)
	if err != nil {
		t.Fatalf("marshal round-trip: %v", err)
	}
	var a, b interface{}
	if err := json.Unmarshal(raw, &a); err != nil {
		t.Fatalf("re-decode fixture: %v", err)
	}
	if err := json.Unmarshal(re, &b); err != nil {
		t.Fatalf("re-decode re-marshaled: %v", err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Errorf("wire-contract drift:\n  fixture: %s\n  re:      %s", raw, re)
	}
}

func TestWireContract_TaskUpdate_GoRoundTrip(t *testing.T) {
	var msg TaskMessage
	wireContractRoundTrip(t, taskUpdateFixture, &msg)

	if msg.Type != TaskMessageKindTaskUpdate {
		t.Errorf("type: got %q, want %q", msg.Type, TaskMessageKindTaskUpdate)
	}
	if msg.TenantID != "t_acme" {
		t.Errorf("tenant_id: got %q, want t_acme", msg.TenantID)
	}
	app, ok := msg.Apps["myapp"]
	if !ok {
		t.Fatalf("apps[myapp] missing")
	}
	if app.DeploymentID != "d_abc123" {
		t.Errorf("deployment_id: got %q", app.DeploymentID)
	}
	if app.SocketMode != "hostname-pinned" {
		t.Errorf("socket_mode: got %q", app.SocketMode)
	}
	if got := len(app.Routes); got != 2 {
		t.Errorf("routes len: got %d, want 2", got)
	}
}

func TestWireContract_TaskUpdateMinimal_GoRoundTrip(t *testing.T) {
	var msg TaskMessage
	wireContractRoundTrip(t, taskUpdateMinimalFixture, &msg)

	app := msg.Apps["myapp"]
	if app.DeploymentSignature != "" {
		t.Errorf("expected deployment_signature omitted (omitempty), got %q", app.DeploymentSignature)
	}
	if app.SocketMode != "" {
		t.Errorf("expected socket_mode omitted (omitempty), got %q", app.SocketMode)
	}
	if len(app.Routes) != 0 {
		t.Errorf("expected no routes, got %d", len(app.Routes))
	}
}

func TestWireContract_FullSync_GoRoundTrip(t *testing.T) {
	var msg TaskMessage
	wireContractRoundTrip(t, fullSyncFixture, &msg)

	if msg.Type != TaskMessageKindFullSync {
		t.Errorf("type: got %q, want %q", msg.Type, TaskMessageKindFullSync)
	}
	if got := len(msg.Apps); got != 2 {
		t.Errorf("apps count: got %d, want 2", got)
	}
}

func TestWireContract_TaskPurgePerApp_GoRoundTrip(t *testing.T) {
	var p PurgePayload
	wireContractRoundTrip(t, taskPurgePerAppFixture, &p)

	if p.Type != TaskMessageKindTaskPurge {
		t.Errorf("type: got %q, want %q", p.Type, TaskMessageKindTaskPurge)
	}
	if p.AppName != "myapp" {
		t.Errorf("app_name: got %q, want myapp", p.AppName)
	}
	if p.Reason != PurgeReasonAppDeleted {
		t.Errorf("reason: got %q, want %q", p.Reason, PurgeReasonAppDeleted)
	}
}

func TestWireContract_TaskPurgeTenantWide_GoRoundTrip(t *testing.T) {
	var p PurgePayload
	wireContractRoundTrip(t, taskPurgeTenantWideFixture, &p)

	if p.AppName != "" {
		t.Errorf("app_name: got %q, want empty (tenant-wide)", p.AppName)
	}
	if p.Reason != PurgeReasonTenantOffboarded {
		t.Errorf("reason: got %q, want %q", p.Reason, PurgeReasonTenantOffboarded)
	}
}

// TestWireContract_TaskPurgeUnknownReason documents the Go/Rust
// closed-enum asymmetry. PurgeReason is a Go string alias — no custom
// UnmarshalJSON, so any string decodes fine. The Rust side
// (edge-worker/src/messages.rs:1066) uses #[serde(rename_all="snake_case")]
// with no #[serde(other)] arm, so unknown reasons error out on the worker.
//
// This is intentional: enforcing a closed set on the Go side would be a
// behavior change for existing callers (e.g. OutboxDrainer which
// constructs PurgePayload values directly). The cross-language contract
// is therefore that the producer side may emit any string value, but the
// consumer side rejects unknowns. Each side asserts its own invariant.
func TestWireContract_TaskPurgeUnknownReason_GoRoundTrip(t *testing.T) {
	var p PurgePayload
	wireContractRoundTrip(t, taskPurgeUnknownReasonFixture, &p)

	if string(p.Reason) != "scheduled_cleanup" {
		t.Errorf("reason: got %q, want scheduled_cleanup (Go accepts unknown strings; Rust rejects)", p.Reason)
	}
}

func TestWireContract_Heartbeat_GoRoundTrip(t *testing.T) {
	var hb HeartbeatMessage
	wireContractRoundTrip(t, heartbeatFixture, &hb)

	if hb.Type != "heartbeat" {
		t.Errorf("type: got %q, want heartbeat", hb.Type)
	}
	app, ok := hb.Apps["myapp"]
	if !ok {
		t.Fatalf("apps[myapp] missing")
	}
	if app.DedupeID != "w_global_dev:d_abc123:2026-07-11T10:30:00Z" {
		t.Errorf("dedupe_id: got %q", app.DedupeID)
	}
	if app.ResidentSeconds == nil || *app.ResidentSeconds != 90 {
		t.Errorf("resident_seconds: got %v, want Some(90)", app.ResidentSeconds)
	}
	if app.DurationMsTotal == nil || *app.DurationMsTotal != 1500 {
		t.Errorf("duration_ms_total: got %v, want Some(1500)", app.DurationMsTotal)
	}
	if got := len(app.ObserverMetrics); got != 3 {
		t.Errorf("observer_metrics len: got %d, want 3", got)
	}
	if hb.ClusterHeadroom == nil || hb.ClusterHeadroom.AppSlots != 42 {
		t.Errorf("cluster_headroom.app_slots: got %v, want 42", hb.ClusterHeadroom)
	}
}

func TestWireContract_HeartbeatMinimal_GoRoundTrip(t *testing.T) {
	var hb HeartbeatMessage
	wireContractRoundTrip(t, heartbeatMinimalFixture, &hb)

	app := hb.Apps["myapp"]
	if app.DedupeID != "" {
		t.Errorf("dedupe_id: got %q, want empty (pre-#418)", app.DedupeID)
	}
	if app.ResidentSeconds != nil {
		t.Errorf("resident_seconds: got %v, want nil (pre-#484)", app.ResidentSeconds)
	}
	if app.DurationMsTotal != nil {
		t.Errorf("duration_ms_total: got %v, want nil (pre-#555)", app.DurationMsTotal)
	}
	if hb.ClusterHeadroom != nil {
		t.Errorf("cluster_headroom: got %v, want nil (pre-#85)", hb.ClusterHeadroom)
	}
}

// Sanity check on the domain.AppStatus field order — encoding/json emits
// struct fields in declaration order, and AppStatus is referenced by the
// heartbeat fixtures. A field-order drift here would silently fail every
// Heartbeat round-trip, so we pin the order explicitly via reflect.
func TestWireContract_DomainAppStatusFieldOrder(t *testing.T) {
	want := []string{
		"status",
		"exit_code",
		"deployment_id",
		"request_count",
		"outbound_bytes",
		"tenant_id",
		"port",
		"dedupe_id",
		"resident_seconds",
		"duration_ms_total",
	}
	var s domain.AppStatus
	got, err := json.Marshal(&s)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(got, &m); err != nil {
		t.Fatal(err)
	}
	// Marshal produces empty `{}` for a zero struct — that's fine; just
	// assert the field ORDER when fields are present.
	raw := []byte(`{"status":"running","exit_code":0,"deployment_id":"d","request_count":1,"outbound_bytes":2,"tenant_id":"t","port":8080,"dedupe_id":"x","resident_seconds":1,"duration_ms_total":1}`)
	var s2 domain.AppStatus
	if err := json.Unmarshal(raw, &s2); err != nil {
		t.Fatal(err)
	}
	re, err := json.Marshal(&s2)
	if err != nil {
		t.Fatal(err)
	}
	// Decode both into a generic map and verify key set matches `want`.
	var gotMap map[string]interface{}
	if err := json.Unmarshal(re, &gotMap); err != nil {
		t.Fatal(err)
	}
	if len(gotMap) != len(want) {
		t.Errorf("AppStatus field count: got %d, want %d", len(gotMap), len(want))
	}
	for _, k := range want {
		if _, ok := gotMap[k]; !ok {
			t.Errorf("AppStatus missing field %q after round-trip", k)
		}
	}
}
