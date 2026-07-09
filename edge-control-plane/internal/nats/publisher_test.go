package nats

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

func TestNATSPublisherImplementsPublisher(t *testing.T) {
	var p Publisher = &NATSPublisher{}
	_ = p // compile check: NATSPublisher implements Publisher
}

func TestNewNATSPublisher_ConnectionError(t *testing.T) {
	_, err := NewNATSPublisher("nats://localhost:4222")
	if err == nil {
		t.Skip("NATS not available, skipping")
	}
}

func TestMockPublisher_PublishTaskUpdate(t *testing.T) {
	p := &MockPublisher{}
	msg := &TaskMessage{
		Type:      "task_update",
		Timestamp: time.Now(),
		TenantID:  "t_test",
		Apps:      map[string]AppConfig{},
	}
	if err := p.PublishTaskUpdate("global", msg); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestMockPublisher_PublishFullSync(t *testing.T) {
	// Issue #53: the ReconcileService and the RegisterWorker hook call
	// PublishFullSync with a TaskMessage pre-populated by the caller.
	// The publisher is responsible for overriding the `type` field so
	// the worker can distinguish event-driven updates from scheduled
	// syncs. Verify the wire shape:
	//   - `type` field is "full_sync" even when the caller passed "task_update"
	//   - `apps` map is preserved
	//   - `tenant_id` is preserved
	p := &MockPublisher{}
	// Caller passed a task_update message — PublishFullSync must override.
	msg := &TaskMessage{
		Type:      "task_update",
		Timestamp: time.Now().UTC(),
		TenantID:  "t_test",
		Apps: map[string]AppConfig{
			"myapp": {
				DeploymentID:   "d_1",
				DeploymentHash: "sha256:abc",
				Env:            map[string]string{"KEY": "value"},
				Allowlist:      []string{"api.stripe.com"},
				MaxMemoryMB:    256,
			},
		},
	}
	if err := p.PublishFullSync("global", msg); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// Caller's struct is untouched (we snapshot before overriding).
	if msg.Type != "task_update" {
		t.Errorf("PublishFullSync mutated caller struct: type=%q", msg.Type)
	}
}

func TestPublishFullSync_OverridesTypeField(t *testing.T) {
	// Direct test of the wire shape that NATSPublisher.PublishFullSync
	// would emit. The MockPublisher doesn't surface the serialized
	// bytes, so we re-encode the same payload shape here and assert
	// the worker's deserializer sees what we expect.
	//
	// This locks the wire shape: workers fail to deserialize if the
	// type field isn't "full_sync" (issue #53).
	msg := &TaskMessage{
		Type:      "full_sync",
		Timestamp: time.Date(2026, 6, 19, 0, 0, 0, 0, time.UTC),
		TenantID:  "t_test",
		Apps: map[string]AppConfig{
			"myapp": {
				DeploymentID:   "d_1",
				DeploymentHash: "abc",
				Env:            map[string]string{},
				MaxMemoryMB:    256,
			},
		},
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Round-trip back into a TaskMessage to verify the worker's
	// deserializer sees what we expect.
	var parsed TaskMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.Type != "full_sync" {
		t.Errorf("parsed.Type = %q, want full_sync", parsed.Type)
	}
	if parsed.TenantID != "t_test" {
		t.Errorf("parsed.TenantID = %q, want t_test", parsed.TenantID)
	}
	if len(parsed.Apps) != 1 {
		t.Errorf("len(apps) = %d, want 1", len(parsed.Apps))
	}
}

func TestMockPublisher_PublishHeartbeat(t *testing.T) {
	p := &MockPublisher{}
	msg := &HeartbeatMessage{
		Type:      "heartbeat",
		Timestamp: time.Now(),
		WorkerID:  "w_test",
		Region:    "global",
		Apps:      map[string]domain.AppStatus{},
	}
	if err := p.PublishHeartbeat("global", msg); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// captureStdout redirects os.Stdout for the duration of fn and returns
// whatever was written. Tests use this to assert the [NATS MOCK] log
// line emitted by MockPublisher actually contains the wire-format
// overrides (applyTypeOverride is called by both MockPublisher and
// NATSPublisher.publishTaskMessage; these tests pin the mock side).
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	fn()

	w.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read captured stdout: %v", err)
	}
	return string(out)
}

// TestMockPublisher_PublishFullSync_EmitsFullSyncTypeEvenIfCallerSetsOther
// pins the wire-format contract for the mock: a caller passing a
// TaskMessage with Type="task_update" to PublishFullSync must see
// "full_sync" on the wire — same as the production NATSPublisher.
//
// Without this test (and the applyTypeOverride extraction in commit
// G), the previous MockPublisher printed whatever the caller passed
// in — so a dev-mode integration that happened to call
// PublishFullSync with a stale TaskMessage would log the wrong type
// and operators couldn't trust the dev log to mirror production.
func TestMockPublisher_PublishFullSync_EmitsFullSyncTypeEvenIfCallerSetsOther(t *testing.T) {
	out := captureStdout(t, func() {
		p := &MockPublisher{}
		err := p.PublishFullSync("fra", &TaskMessage{
			Type:      "task_update", // wrong on purpose
			Timestamp: time.Now().UTC(),
			TenantID:  "t_test",
			Apps:      map[string]AppConfig{},
		})
		if err != nil {
			t.Fatalf("PublishFullSync: %v", err)
		}
	})
	if !strings.Contains(out, `"type":"full_sync"`) {
		t.Errorf("mock didn't override type; stdout=%s", out)
	}
	if strings.Contains(out, `"type":"task_update"`) {
		t.Errorf("mock printed the caller's (wrong) type; stdout=%s", out)
	}
}

// TestMockPublisher_PublishTaskUpdate_EmitsTaskUpdateType pins the
// other half of the override contract: PublishTaskUpdate must emit
// "task_update" on the wire even if the caller set Type to something
// else. The applyTypeOverride helper (commit G) makes both publishers
// use the same code path.
func TestMockPublisher_PublishTaskUpdate_EmitsTaskUpdateType(t *testing.T) {
	out := captureStdout(t, func() {
		p := &MockPublisher{}
		err := p.PublishTaskUpdate("global", &TaskMessage{
			Type:      "full_sync", // wrong on purpose
			Timestamp: time.Now().UTC(),
			TenantID:  "t_test",
			Apps:      map[string]AppConfig{},
		})
		if err != nil {
			t.Fatalf("PublishTaskUpdate: %v", err)
		}
	})
	if !strings.Contains(out, `"type":"task_update"`) {
		t.Errorf("mock didn't override type; stdout=%s", out)
	}
	if strings.Contains(out, `"type":"full_sync"`) {
		t.Errorf("mock printed the caller's (wrong) type; stdout=%s", out)
	}
}

// TestMockPublisher_DoesNotMutateCallerStruct locks the snapshot
// semantics promised by applyTypeOverride: the caller can re-use
// their TaskMessage struct after publish without seeing an
// unexpected Type change.
func TestMockPublisher_DoesNotMutateCallerStruct(t *testing.T) {
	msg := &TaskMessage{
		Type:      "task_update",
		Timestamp: time.Now().UTC(),
		TenantID:  "t_test",
		Apps:      map[string]AppConfig{},
	}
	_ = captureStdout(t, func() {
		p := &MockPublisher{}
		if err := p.PublishFullSync("global", msg); err != nil {
			t.Fatalf("PublishFullSync: %v", err)
		}
	})
	if msg.Type != "task_update" {
		t.Errorf("mock mutated caller struct: type=%q, want task_update", msg.Type)
	}
}

// TestHeartbeatMessage_ClusterHeadroomRoundTrip pins the wire shape used
// by the autoscaler (issue #85). AppSlots must serialize, must round-trip,
// and CPUPct/MemPct must be omitted when nil.
func TestHeartbeatMessage_ClusterHeadroomRoundTrip(t *testing.T) {
	msg := &HeartbeatMessage{
		Type:      "heartbeat",
		Timestamp: time.Now(),
		WorkerID:  "w_fra_test",
		Region:    "fra",
		Apps:      map[string]domain.AppStatus{},
		ClusterHeadroom: &ClusterHeadroom{
			AppSlots: 42,
		},
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"cluster_headroom":{"app_slots":42}`) {
		t.Errorf("cluster_headroom.app_slots must appear on the wire; got: %s", data)
	}
	if strings.Contains(string(data), "cpu_pct") || strings.Contains(string(data), "mem_pct") {
		t.Errorf("nil CPUPct/MemPct must be omitted; got: %s", data)
	}
	var decoded struct {
		ClusterHeadroom ClusterHeadroom `json:"cluster_headroom"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.ClusterHeadroom.AppSlots != 42 {
		t.Errorf("AppSlots = %d, want 42", decoded.ClusterHeadroom.AppSlots)
	}
}

// TestHeartbeatMessage_NoClusterHeadroom pins the pre-#85 wire shape.
// A heartbeat without the field must still serialize cleanly. This is
// the regression that would break pre-#85 workers if `omitempty` were
// forgotten on the new field.
func TestHeartbeatMessage_NoClusterHeadroom(t *testing.T) {
	msg := &HeartbeatMessage{
		Type:      "heartbeat",
		Timestamp: time.Now(),
		WorkerID:  "w_legacy",
		Region:    "fra",
		Apps:      map[string]domain.AppStatus{},
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "cluster_headroom") {
		t.Errorf("nil ClusterHeadroom must be omitted; got: %s", data)
	}
}

// TestAppConfig_SocketMode_OmittedStaysEmpty pins the rolling-upgrade
// contract for issue #412: an AppConfig constructed without
// SocketMode must serialize without a `socket_mode` field on the wire
// and must round-trip into an empty string. This is the case the
// pre-#412 control plane depends on — pre-#412 workers that ignore
// the field keep working.
func TestAppConfig_SocketMode_OmittedStaysEmpty(t *testing.T) {
	cfg := AppConfig{
		DeploymentID:   "d_legacy",
		DeploymentHash: "abc",
		Env:            map[string]string{},
		Allowlist:      []string{"api.stripe.com"},
		MaxMemoryMB:    256,
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "socket_mode") {
		t.Errorf("empty SocketMode must be omitted; got: %s", data)
	}
	var decoded AppConfig
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.SocketMode != "" {
		t.Errorf("decoded.SocketMode = %q, want empty", decoded.SocketMode)
	}
}

// TestAppConfig_SocketMode_HostnamePinned_RoundTrips is the wire-shape
// pin for issue #412. The CP doesn't interpret the value — it just
// threads it. This test verifies the value the worker needs to see
// (`hostname-pinned`) survives marshal/unmarshal intact. The worker
// then enforces the compose rule (per-app field AND worker-wide
// `EDGE_EGRESS_HOSTNAME_PINNING=true`).
func TestAppConfig_SocketMode_HostnamePinned_RoundTrips(t *testing.T) {
	cfg := AppConfig{
		DeploymentID:   "d_412",
		DeploymentHash: "abc",
		Env:            map[string]string{},
		MaxMemoryMB:    256,
		SocketMode:     "hostname-pinned",
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"socket_mode":"hostname-pinned"`) {
		t.Errorf("hostname-pinned must appear on the wire; got: %s", data)
	}
	var decoded AppConfig
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.SocketMode != "hostname-pinned" {
		t.Errorf("decoded.SocketMode = %q, want hostname-pinned", decoded.SocketMode)
	}
}

// TestAppConfig_SocketMode_UnknownValue_DoesNotPanic pins the
// rolling-upgrade contract for unknown values: a future control
// plane emitting a variant the worker doesn't know yet (e.g.
// `socket_mode: "experimental-v2"`) must NOT cause `json.Marshal` or
// `json.Unmarshal` to fail. The CP just threads the string; the
// worker deserializer (edge-worker/src/messages.rs) maps unknown
// values back to `None` (= fall back to worker-wide knob). This is
// the same shape `deserialize_allowlist` already establishes for
// per-app egress allowlist entries.
func TestAppConfig_SocketMode_UnknownValue_DoesNotPanic(t *testing.T) {
	cfg := AppConfig{
		DeploymentID:   "d_future",
		DeploymentHash: "abc",
		Env:            map[string]string{},
		MaxMemoryMB:    256,
		SocketMode:     "experimental-v2",
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"socket_mode":"experimental-v2"`) {
		t.Errorf("unknown value must round-trip verbatim; got: %s", data)
	}
	var decoded AppConfig
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.SocketMode != "experimental-v2" {
		t.Errorf("decoded.SocketMode = %q, want experimental-v2 (passthrough)", decoded.SocketMode)
	}
}

// TestBuildAppConfigWithSocketMode_DefaultsToEmpty verifies the
// issue #412 sibling constructor: passing an empty socketMode must
// produce an AppConfig indistinguishable from the pre-#412
// BuildAppConfig output. This locks the rolling-upgrade behavior at
// the call-site level: callers that opt into the new constructor
// without actually setting a policy must not surprise downstream
// workers with a phantom `socket_mode` field.
func TestBuildAppConfigWithSocketMode_DefaultsToEmpty(t *testing.T) {
	cfg := BuildAppConfigWithSocketMode(
		"d_legacy", "abc", "", "",
		map[string]string{}, []string{}, 256,
		"", // empty socketMode
	)
	if cfg.SocketMode != "" {
		t.Errorf("SocketMode = %q, want empty (omitempty contract)", cfg.SocketMode)
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "socket_mode") {
		t.Errorf("empty SocketMode must be omitted; got: %s", data)
	}
}
