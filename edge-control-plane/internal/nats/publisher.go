package nats

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/nats-io/nats.go"
)

// Stream name used by both the publisher and the worker for `TaskMessage`s.
// Exposed here so the worker can verify it's subscribing to the same stream.
const TaskStreamName = "edgecloud-tasks"

// Publisher defines the interface for NATS publishing.
type Publisher interface {
	PublishTaskUpdate(region string, msg *TaskMessage) error
	// PublishFullSync publishes the full desired-state snapshot for a
	// (tenant, region). Workers treat it as authoritative: stop any app
	// not in the set, start any missing, restart any whose deployment_id
	// doesn't match. Published periodically by ReconcileService (issue #53)
	// and on worker registration so cold-start is instant.
	PublishFullSync(region string, msg *TaskMessage) error
	PublishHeartbeat(region string, msg *HeartbeatMessage) error
	// PublishPurge issues a per-tenant (and optionally per-app) tombstone
	// that instructs the worker to remove its KV / cache / scheduler
	// persistent state (issue #569). The signal is explicit — stop /
	// crash / rebalance must NOT delete state — so it travels as a
	// distinct wire variant (`task_purge`) on the same
	// edgecloud.tasks.<region> subject. The OutboxDrainer
	// (internal/service/outbox_drainer.go) dispatches this method when
	// the outbox row carries Kind="task_purge".
	PublishPurge(region string, msg *PurgePayload) error
	EnsureStream(cfg StreamConfig) error
}

// TaskMessageKind names the wire `type` discriminator. Centralized as
// constants so the publisher, the OutboxRepository, and the OutboxDrainer
// share one source of truth (issue #569).
const (
	TaskMessageKindTaskUpdate = "task_update"
	TaskMessageKindFullSync   = "full_sync"
	TaskMessageKindTaskPurge  = "task_purge"
)

// TaskMessage is published to edgecloud.tasks.<region> when app set changes.
type TaskMessage struct {
	Type      string               `json:"type"`
	Timestamp time.Time            `json:"timestamp"`
	TenantID  string               `json:"tenant_id"`
	Apps      map[string]AppConfig `json:"apps"`
}

// PurgePayload is the issue #569 tombstone. Carried on the same
// edgecloud.tasks.<region> subject as TaskMessage but with a
// distinctly-shaped payload (no `apps` field — the worker's purge
// handler derives the set of apps to stop from its own in-memory
// state at receipt time). Subject reuse avoids a new NATS
// subscription; payload shape divergence keeps the worker's
// deserialize-or-error invariant (see unknown_type_field_fails_to_deserialize
// in edge-worker/src/messages.rs) safe.
//
// AppName semantics:
//   - non-empty: per-app purge — stop `AppName` if running, then purge
//     the per-tenant dirs. Idempotent if the app isn't running.
//   - empty ("" or omitted via omitempty): tenant-wide — the worker
//     enumerates its current apps for TenantID and stops each before
//     purging. Today the CP enqueues per-app rows from
//     `AppService.Delete` and from `TenantService.DeleteTenant` (one
//     row per app); the empty form is kept for forward-compat with
//     a future "single tenant-wide publish" optimization.
//
// Reason is an audit-only discriminator; the worker logs it but
// doesn't change behavior between the two cases.
type PurgePayload struct {
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	TenantID  string      `json:"tenant_id"`
	AppName   string      `json:"app_name,omitempty"`
	Reason    PurgeReason `json:"reason"`
}

// PurgeReason is the typed `reason` field on PurgePayload. Backed
// by a string so the JSON wire shape stays "app_deleted" /
// "tenant_offboarded" (matching the worker-side `PurgeReason`
// `#[serde(rename_all = "snake_case")]` in
// edge-worker/src/messages.rs), but the typed wrapper gives
// compile-time safety at the CP call sites — AppService.Delete
// and TenantService.DeleteTenant cannot accidentally pass
// `Reason: "wrong_value"` to the field.
type PurgeReason string

// Purge reason constants (issue #569). The string values are the
// wire format; do not rename without also updating the worker.
const (
	PurgeReasonAppDeleted       PurgeReason = "app_deleted"
	PurgeReasonTenantOffboarded PurgeReason = "tenant_offboarded"
)

// AppConfig describes an app's deployment configuration.
type AppConfig struct {
	DeploymentID   string `json:"deployment_id"`
	DeploymentHash string `json:"deployment_hash"`
	// DeploymentSignature is the base64url(no-pad) Ed25519 signature
	// over `sha256(artifact_bytes) || deployment_id` (issue #307).
	// Workers verify the signature against the configured public key
	// before instantiation. Empty for pre-#307 deployments (legacy
	// mode); the worker's `EDGE_REQUIRE_SIGNATURE` flag gates
	// whether empty signatures are accepted. Each DeploymentRoute
	// carries its own signature so canary splits can verify
	// independently.
	DeploymentSignature string `json:"deployment_signature,omitempty"`
	// SigningKeyID is the logical key id (`EDGE_SIGNING_KEY_ID`
	// on the CP) the signature above was produced with (issue #307
	// follow-up PR1). Workers look this id up in their configured
	// keyring to pick the right public key for verification; empty
	// falls back to the default key. Omitted from the wire when
	// empty so pre-PR1 workers silently ignore it (the field is
	// additive and backward-compatible).
	SigningKeyID string `json:"signing_key_id,omitempty"`
	// PreviewID is the hex suffix the CLI (or server, as a fallback)
	// minted when this deployment was uploaded as a preview
	// (issue #308). The worker reads it and forwards it to
	// edge-runtime::RuntimeState so the per-tenant persistent stores
	// (KV/cache/scheduling) get a `/preview-{id}/` subdirectory —
	// preventing two concurrent previews of the same app from
	// trampling each other's keys. Empty for non-preview deploys;
	// omitted from the wire when empty so pre-#308 workers
	// silently ignore it.
	PreviewID string `json:"preview_id,omitempty"`
	// PreviewPRNumber is the integer GitHub PR number the composite
	// action forwards via ?preview-pr-number=. The worker reads it
	// and stamps `EDGE_PREVIEW_PR_NUMBER=<n>` into the guest env
	// so the guest can render PR-aware UI. Zero for non-preview
	// deploys or for `edge deploy --preview` from a laptop
	// (no PR linkage); omitted from the wire when zero so
	// pre-#308 workers ignore it.
	PreviewPRNumber int               `json:"preview_pr_number,omitempty"`
	Routes          []DeploymentRoute `json:"routes,omitempty"` // populated when canary splits are active
	Env             map[string]string `json:"env"`
	Allowlist       []string          `json:"allowlist"`
	MaxMemoryMB     int               `json:"max_memory_mb"`
	CpuBudgetMS     int               `json:"cpu_budget_ms"`
	// SocketMode is the per-app selector for the worker-side
	// `SocketEgressPolicy` (issue #412). The CP doesn't interpret the
	// value — it threads the string through and the worker decides
	// what to do. Recognised values on the worker: "block-all",
	// "allowlist", "allow-all", "hostname-pinned". Unknown / absent
	// values cause the worker to fall back to the worker-wide
	// `EDGE_EGRESS_SOCKET_MODE` knob (rolling-upgrade contract, see
	// `deserialize_socket_mode` in edge-worker/src/messages.rs).
	SocketMode string `json:"socket_mode,omitempty"`
}

// DeploymentRoute describes one deployment's weight in a canary traffic split.
// Workers use this to run multiple deployments of the same app concurrently.
type DeploymentRoute struct {
	DeploymentID        string `json:"deployment_id"`
	DeploymentHash      string `json:"deployment_hash"`
	DeploymentSignature string `json:"deployment_signature,omitempty"`
	// SigningKeyID mirrors AppConfig.SigningKeyID for canary splits;
	// each route is independently signed and may have been produced
	// by a different key during key rotation.
	SigningKeyID string `json:"signing_key_id,omitempty"`
	Weight       int    `json:"weight"`
}

// HeartbeatMessage is published by workers to edgecloud.heartbeats.<region>.
//
// This type is publish-only — no code in the repo deserializes into it
// (the consumer in service/worker.go uses an anonymous inline struct
// so it can pass the apps blob through as json.RawMessage to the JSONB
// upsert path). New wire fields should be added here AND mirrored in
// the consumer's anonymous struct.
type HeartbeatMessage struct {
	Type      string                      `json:"type"`
	Timestamp time.Time                   `json:"timestamp"`
	WorkerID  string                      `json:"worker_id"`
	Region    string                      `json:"region"`
	Apps      map[string]domain.AppStatus `json:"apps"`
	// ClusterHeadroom carries capacity info for the autoscaler (issue #85).
	// Optional on the wire so pre-#85 workers (no field) still serialize
	// cleanly through this struct, and a new worker talking to an old
	// control plane has the field silently dropped by the consumer's
	// partial unmarshal — both directions safe.
	//
	// The autoscaler reads `AppSlots` from this block; CPUPct / MemPct are
	// observability-only for now (no sysinfo on the worker yet).
	ClusterHeadroom *ClusterHeadroom `json:"cluster_headroom,omitempty"`
}

// ClusterHeadroom mirrors the Rust `ClusterHeadroom` struct in
// edge-worker/src/messages.rs. AppSlots is the only field the autoscaler
// acts on; the rest are pass-through for future PRs that add
// system-introspection.
type ClusterHeadroom struct {
	CPUPct   *float64 `json:"cpu_pct,omitempty"`
	MemPct   *float64 `json:"mem_pct,omitempty"`
	AppSlots uint32   `json:"app_slots"`
}

// StreamConfig describes a JetStream stream to be created/verified.
type StreamConfig struct {
	Name      string
	Subjects  []string
	Retention nats.RetentionPolicy
	MaxAge    time.Duration
	Replicas  int
}

// applyTypeOverride returns a *TaskMessage with the given `type` field
// set, preserving every other field from the input. Both the real
// NATSPublisher and the MockPublisher call this so the wire shape is
// guaranteed identical regardless of which publisher the operator
// configured — and so the wire-format invariant has a single source of
// truth (the override logic was previously only in
// NATSPublisher.publishTaskMessage, and the mock printed whatever the
// caller passed in; the two would diverge if a caller accidentally set
// `Type: "task_update"` and called PublishFullSync through the mock).
//
// We snapshot rather than mutate so callers who hold a TaskMessage
// pointer don't see their struct modified by the publish call.
func applyTypeOverride(msg *TaskMessage, typeField string) *TaskMessage {
	return &TaskMessage{
		Type:      typeField,
		Timestamp: msg.Timestamp,
		TenantID:  msg.TenantID,
		Apps:      msg.Apps,
	}
}

// BuildAppConfig is the single source of truth for constructing an
// AppConfig. The previous implementation had this literal duplicated at
// 7 sites across internal/service/{deployment,reconcile,traffic}.go
// — exactly how the TaskUpdate / FullSync wire shape drifted apart
// before PR #166. Use this everywhere; new fields on AppConfig get
// the default for free.
//
// `routes` is variadic for ergonomics: omit it for single-deployment
// publishes; pass a non-empty slice to activate canary splits. The
// `omitempty` JSON tag on AppConfig.Routes means nil and missing
// produce identical wire output.
//
// `signingKeyID` is the `EDGE_SIGNING_KEY_ID` the artifact was
// signed with (issue #307 follow-up PR1). Empty string means "use
// the worker's default key" — see `verifier::Keyring` on the
// worker side. The `omitempty` JSON tag means an empty string is
// dropped from the wire so pre-PR1 workers ignore it.
func BuildAppConfig(
	deploymentID, deploymentHash, deploymentSignature, signingKeyID string,
	previewID string,
	previewPRNumber int,
	env map[string]string,
	allowlist []string,
	maxMemoryMB int,
	routes ...DeploymentRoute,
) AppConfig {
	cfg := AppConfig{
		DeploymentID:        deploymentID,
		DeploymentHash:      deploymentHash,
		DeploymentSignature: deploymentSignature,
		SigningKeyID:        signingKeyID,
		PreviewID:           previewID,
		PreviewPRNumber:     previewPRNumber,
		Env:                 env,
		Allowlist:           allowlist,
		MaxMemoryMB:         maxMemoryMB,
		CpuBudgetMS:         1000,
	}
	if len(routes) > 0 {
		cfg.Routes = routes
	}
	return cfg
}

// BuildAppConfigWithSocketMode is the issue #412 sibling of
// BuildAppConfig — same shape, plus a `socketMode` per-app selector
// for the worker's `SocketEgressPolicy`. The CP doesn't interpret the
// value (worker owns the policy); it threads the string through.
// Empty string is dropped from the wire (omitempty), preserving the
// pre-#412 rolling-upgrade contract.
func BuildAppConfigWithSocketMode(
	deploymentID, deploymentHash, deploymentSignature, signingKeyID string,
	env map[string]string,
	allowlist []string,
	maxMemoryMB int,
	socketMode string,
	routes ...DeploymentRoute,
) AppConfig {
	cfg := BuildAppConfig(
		deploymentID, deploymentHash, deploymentSignature, signingKeyID,
		"", // preview_id — socket-mode sibling does not carry preview metadata;
		//     preview-aware callers must use BuildAppConfig directly so they
		//     pass preview_id + preview_pr_number explicitly.
		0, // preview_pr_number — same reasoning as preview_id above.
		env, allowlist, maxMemoryMB, routes...,
	)
	cfg.SocketMode = socketMode
	return cfg
}

// MockPublisher is a no-op publisher for development.
type MockPublisher struct{}

func (p *MockPublisher) PublishTaskUpdate(region string, msg *TaskMessage) error {
	data, _ := json.Marshal(applyTypeOverride(msg, "task_update"))
	fmt.Printf("[NATS MOCK] Publish to edgecloud.tasks.%s: %s\n", region, string(data))
	return nil
}

func (p *MockPublisher) PublishFullSync(region string, msg *TaskMessage) error {
	data, _ := json.Marshal(applyTypeOverride(msg, "full_sync"))
	fmt.Printf("[NATS MOCK] Publish to edgecloud.tasks.%s: %s\n", region, string(data))
	return nil
}

func (p *MockPublisher) PublishHeartbeat(region string, msg *HeartbeatMessage) error {
	data, _ := json.Marshal(msg)
	fmt.Printf("[NATS MOCK] Publish to edgecloud.heartbeats.%s: %s\n", region, string(data))
	return nil
}

// PublishPurge emits a task_purge tombstone. The MockPublisher prints
// the marshaled payload rather than going through applyTypeOverride —
// the purge payload already carries `type:"task_purge"` because the
// caller set it when building PurgePayload, and applyTypeOverride
// only operates on *TaskMessage (which has no purge equivalent).
func (p *MockPublisher) PublishPurge(region string, msg *PurgePayload) error {
	data, _ := json.Marshal(msg)
	fmt.Printf("[NATS MOCK] Publish to edgecloud.tasks.%s (purge): %s\n", region, string(data))
	return nil
}

func (p *MockPublisher) EnsureStream(_ StreamConfig) error {
	return nil
}

// NATSPublisher is a real NATS JetStream publisher.
type NATSPublisher struct {
	nc *nats.Conn
	js nats.JetStreamContext
}

// NewNATSPublisher connects to NATS and returns a JetStream publisher.
func NewNATSPublisher(url string) (*NATSPublisher, error) {
	nc, err := nats.Connect(url,
		nats.Name("edge-cloud-control-plane"),
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS at %s: %w", url, err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("creating JetStream context: %w", err)
	}
	return &NATSPublisher{nc: nc, js: js}, nil
}

// EnsureStream idempotently creates the given JetStream stream. If the
// stream already exists with the same shape (subjects, retention, MaxAge,
// replicas), it's a no-op.
//
// Retention and replica-count changes require delete+recreate — NATS does
// not allow changing those on an existing stream. The reconcile loop (every
// 5 minutes) re-publishes desired state, bounding the window of missed
// messages after the delete.
func (p *NATSPublisher) EnsureStream(cfg StreamConfig) error {
	info, err := p.js.StreamInfo(cfg.Name)
	if errors.Is(err, nats.ErrStreamNotFound) {
		return p.addStream(cfg)
	}
	if err != nil {
		return fmt.Errorf("checking stream %s: %w", cfg.Name, err)
	}

	// Stream exists — check if we need to delete+recreate for changes
	// that NATS doesn't allow in-place (retention, replica count).
	if info.Config.Retention != cfg.Retention || info.Config.Replicas != cfg.Replicas {
		log.Printf(
			"stream %s has retention=%v/replicas=%d, want retention=%v/replicas=%d — deleting and recreating",
			cfg.Name, info.Config.Retention, info.Config.Replicas, cfg.Retention, cfg.Replicas,
		)
		if err := p.js.DeleteStream(cfg.Name); err != nil {
			return fmt.Errorf("deleting stream %s for migration: %w", cfg.Name, err)
		}
		return p.addStream(cfg)
	}

	if !equalSubjects(info.Config.Subjects, cfg.Subjects) {
		return fmt.Errorf("stream %s already exists with subjects=%v, want %v", cfg.Name, info.Config.Subjects, cfg.Subjects)
	}
	if info.Config.MaxAge != cfg.MaxAge {
		return fmt.Errorf("stream %s already exists with MaxAge=%v, want %v", cfg.Name, info.Config.MaxAge, cfg.MaxAge)
	}
	return nil
}

// addStream is a small helper that creates a stream with the given config.
func (p *NATSPublisher) addStream(cfg StreamConfig) error {
	_, err := p.js.AddStream(&nats.StreamConfig{
		Name:      cfg.Name,
		Subjects:  cfg.Subjects,
		Retention: cfg.Retention,
		MaxAge:    cfg.MaxAge,
		Replicas:  cfg.Replicas,
	})
	if err != nil {
		return fmt.Errorf("adding stream %s: %w", cfg.Name, err)
	}
	return nil
}

func equalSubjects(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// PublishTaskUpdate publishes a task update to edgecloud.tasks.<region>.
func (p *NATSPublisher) PublishTaskUpdate(region string, msg *TaskMessage) error {
	subject := "edgecloud.tasks." + region
	return p.publishTaskMessage(subject, msg, "task_update")
}

// PublishFullSync publishes a full-state sync to edgecloud.tasks.<region>.
// Wire format is identical to PublishTaskUpdate except the `type` field is
// "full_sync" so the worker can distinguish a scheduled reconcile from an
// event-driven update in metrics/logs. Used by:
//   - ReconcileService.Run — periodic safety net (RECONCILE_INTERVAL, default 5min)
//   - ReconcileService.RequestSync — on worker registration
//   - InternalHandler.Sync — HTTP fallback when NATS is silent > N seconds
func (p *NATSPublisher) PublishFullSync(region string, msg *TaskMessage) error {
	subject := "edgecloud.tasks." + region
	return p.publishTaskMessage(subject, msg, "full_sync")
}

// PublishPurge issues a task_purge tombstone to edgecloud.tasks.<region>
// (issue #569). The wire shape is a PurgePayload (NOT a TaskMessage)
// because the worker's purge handler doesn't carry per-app config — it
// derives the apps-to-stop set from its own in-memory state. Subject
// reuse (vs a new edgecloud.purges.<region>) avoids adding a NATS
// subscription the worker would need to bind.
//
// We deliberately bypass applyTypeOverride here — that helper only
// knows how to snapshot a *TaskMessage and force its `type` field, and
// PurgePayload is a different shape. The caller (OutboxDrainer) sets
// PurgePayload.Type to TaskMessageKindTaskPurge at construction time,
// so the marshaled payload already carries the right discriminator.
func (p *NATSPublisher) PublishPurge(region string, msg *PurgePayload) error {
	subject := "edgecloud.tasks." + region
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshaling purge payload: %w", err)
	}
	if _, err := p.js.Publish(subject, data); err != nil {
		return fmt.Errorf("publishing purge to %s: %w", subject, err)
	}
	return nil
}

// publishTaskMessage marshals and publishes a TaskMessage, overriding the
// `type` field via applyTypeOverride (shared with MockPublisher so the
// wire shape is identical regardless of which publisher the operator
// configured).
func (p *NATSPublisher) publishTaskMessage(subject string, msg *TaskMessage, typeField string) error {
	data, err := json.Marshal(applyTypeOverride(msg, typeField))
	if err != nil {
		return fmt.Errorf("marshaling task message: %w", err)
	}
	_, err = p.js.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("publishing to %s: %w", subject, err)
	}
	return nil
}

// PublishHeartbeat publishes a heartbeat to edgecloud.heartbeats.<region>.
func (p *NATSPublisher) PublishHeartbeat(region string, msg *HeartbeatMessage) error {
	subject := "edgecloud.heartbeats." + region
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshaling heartbeat message: %w", err)
	}
	_, err = p.js.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("publishing to %s: %w", subject, err)
	}
	return nil
}

// Close closes the NATS connection.
func (p *NATSPublisher) Close() {
	p.nc.Close()
}

// Conn returns the underlying NATS connection for subscriber use.
func (p *NATSPublisher) Conn() *nats.Conn {
	return p.nc
}
