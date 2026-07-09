package domain

import (
	"encoding/json"
	"time"

	"github.com/lib/pq"
)

// Deployment represents a deployed Wasm artifact.
type Deployment struct {
	ID       string `db:"id"`
	TenantID string `db:"tenant_id"`
	AppName  string `db:"app_name"`
	Status   string `db:"status"`
	Hash     string `db:"hash"` // SHA-256 of Wasm payload
	// Signature is the base64url(no-pad) Ed25519 signature over
	// `sha256(artifact_bytes) || deployment_id` (issue #307). Empty
	// for rows created before the signing code shipped; the worker
	// treats empty as "unsigned legacy artifact" and the rollout
	// flag EDGE_REQUIRE_SIGNATURE gates whether such rows are
	// accepted. Stamped at Deploy / Migrate / MigrateTree time by
	// `signing.Signer.Sign`.
	Signature string `db:"signature"`
	// SigningKeyID is the logical key id used to sign this row
	// (env EDGE_SIGNING_KEY_ID on the CP). Future rotation work
	// will check `signing_key_id = <current key id>` to refuse
	// artifacts signed with a retired key without a DB lookup per
	// request. Empty for legacy rows.
	SigningKeyID string `db:"signing_key_id"`
	// Regions is the list of regions this deployment is replicated to.
	// The activate path loops over this list and publishes one
	// `TaskMessage` per region to `edgecloud.tasks.<region>`. An empty
	// slice (e.g. for rows created before migration 008) means
	// "use the control plane's default region" — the service layer
	// resolves the fallback. See `service.ActivateDeployment`.
	//
	// Typed as pq.StringArray (which is `[]string` underneath) so the
	// `TEXT[]` column scans correctly via lib/pq's Scanner — a bare
	// `[]string` does NOT implement `sql.Scanner` and would fail on
	// SELECT. The JSON wire format is unchanged because
	// pq.StringArray marshals identically to []string.
	//
	// No `omitempty`: an empty slice serializes as `[]`, which is
	// more useful for clients than `null` and matches the codebase
	// convention of not using `omitempty` on domain structs.
	Regions   pq.StringArray `db:"regions" json:"regions"`
	CreatedAt time.Time      `db:"created_at"`
	// AutoRollbackEnabled is the tenant opt-in set by
	// `edge deploy --auto-rollback`. At activate time this flag is
	// copied onto the active_deployments row; it controls whether
	// the worker-driven auto-rollback (handler.AutoRollback) and the
	// heartbeat-driven stability window (service.worker.evaluateStability)
	// are allowed to mutate last_good_deployment_id for this app.
	// Defaults to false on the wire (legacy deployments pre-migration-009
	// are not affected). Stored on the deployments row too so operators
	// can audit "which deployments opted in" via the list endpoint.
	AutoRollbackEnabled bool `db:"auto_rollback_enabled" json:"auto_rollback_enabled"`
	// DesiredReplicas is the number of workers that should run this
	// deployment in each region (issue #316). 0 means "no threshold"
	// — the reconcile loop won't warn about under-replication.
	DesiredReplicas int `db:"desired_replicas" json:"desired_replicas"`
	// BuildAttestation is the DSSE-wrapped, signed in-toto Statement
	// v0.1 envelope for this deployment (issue #307 PR2). Stored as
	// JSONB so downstream audit pipelines can query structured fields
	// without a wire round-trip. Empty bytes (= nil JSONB in SQL) for
	// pre-PR2 rows and for deployments where the operator hasn't
	// supplied build metadata; the EDGE_PROVENANCE_REQUIRED env var
	// (default false) gates whether absence is a 4xx. Typed as
	// json.RawMessage so we marshal the envelope verbatim — the CP
	// never re-parses or rewrites it, preserving the canonical bytes
	// the verifier reconstructs.
	//
	// `omitempty` on the wire so pre-PR2 API consumers don't see a
	// new field in list / status responses.
	BuildAttestation json.RawMessage `db:"build_attestation" json:"build_attestation,omitempty"`

	// PreviewID, PreviewPRNumber, PreviewExpiresAt (issue #308)
	// mark a deployments row as a preview and drive auto-cleanup.
	//
	// All three are nullable on purpose: a non-preview deployment
	// has NULL for all three, mirroring the LastGoodDeploymentID
	// *string pattern on ActiveDeployment (line 97). The pointer
	// type lets sqlx distinguish NULL from a typed zero value
	// (e.g., PreviewPRNumber == 0 — a real PR number — must not be
	// confused with "no PR"). On the wire `omitempty` keeps the
	// JSON output clean for non-preview rows.
	//
	// PreviewID is the hex suffix the CLI mints (or the server
	// overrides) when ?preview-id=... is set. Used as the
	// store-scope key by edge-runtime (`<EDGE_KV_STORE_PATH>/{tenant_id}/preview-{id}/`)
	// and as the SWEEP marker for PreviewGCService. Free-form TEXT
	// (not a foreign key to anything); the GC sweep uses the
	// expiry column, not this one.
	PreviewID *string `db:"preview_id" json:"preview_id,omitempty"`
	// PreviewPRNumber is the integer GitHub PR number the composite
	// action forwards via ?preview-pr-number=. Stored so an operator
	// can correlate a deployment row with the PR that produced it
	// without parsing the app_name suffix. Optional: non-CI users
	// (`edge deploy --preview` on a laptop) don't have a PR number.
	PreviewPRNumber *int `db:"preview_pr_number" json:"preview_pr_number,omitempty"`
	// PreviewExpiresAt is the TIMESTAMPTZ the PreviewGCService
	// compares against NOW() on each sweep. Indexed (partial,
	// migration 021) so the sweep stays cheap. Set to
	// NOW() + 7d on upload by default; per-deploy overridable via
	// ?preview-ttl=24h. NULL for non-preview rows.
	PreviewExpiresAt *time.Time `db:"preview_expires_at" json:"preview_expires_at,omitempty"`
}

// Deployment status constants.
const (
	StatusDeployed = "deployed"
	StatusActive   = "active"
	StatusFailed   = "failed"
	StatusMigrated = "migrated"
)

// ActiveDeployment maps an app name to its active deployment for a tenant.
//
// LastGoodDeploymentID is the prior deployment that was active before the
// most recent Activate. Used by RollbackDeployment to swap back to it
// without requiring the tenant to remember the id. Nullable: pre-existing
// rows (no history) read back as nil; rollback on such a row returns 409.
type ActiveDeployment struct {
	TenantID             string  `db:"tenant_id"`
	AppName              string  `db:"app_name"`
	DeploymentID         string  `db:"deployment_id"`
	LastGoodDeploymentID *string `db:"last_good_deployment_id"`
	// AutoRollbackEnabled mirrors the flag from the deployments
	// row, copied at activate time. Read by the worker-driven
	// auto-rollback endpoint and by the heartbeat-driven stability
	// window. Defaults to false on disk (migration 009).
	AutoRollbackEnabled bool `db:"auto_rollback_enabled"`
	// StableSince is the first-heartbeat timestamp for the
	// currently-active deployment. NULL means "not yet observed
	// running" or "rolled back; clock reset". The heartbeat
	// handler sets this to NOW() the first time it sees
	// status="running" for this active row; the stability window
	// promotes deployment_id → last_good_deployment_id once
	// stable_since is older than STABLE_WINDOW_SECONDS. Reset to
	// NULL on every activate / rollback / auto-rollback (see
	// service.ActivateDeployment / RollbackDeployment and
	// repository.ResetStableSinceForRollback).
	StableSince *time.Time `db:"stable_since"`

	// RegionsPublished is the deduped set of regions that have been
	// successfully notified for the CURRENT activation. Written by
	// service.ActivateDeployment after each per-region publish
	// succeeds; read at the start of the next activate call so a
	// retry doesn't republish to regions that already received the
	// TaskMessage (NATS JetStream workqueue dedupes by message id,
	// but a redundant publish still wastes a round trip and a worker
	// reconciliation pass).
	//
	// The DO UPDATE branch of ActiveDeploymentRepository.Set
	// overwrites the four publish-state columns, so a re-activation
	// starts with an empty history — matches the operator's mental
	// model ("I just activated, so no regions have been notified
	// yet for THIS activation").
	//
	// pq.StringArray for the same reason as Deployment.Regions:
	// lib/pq's Scanner requires it for TEXT[].
	RegionsPublished pq.StringArray `db:"regions_published"`
	// RegionsFailed is the set of regions whose publish failed on
	// the most recent activation attempt. Distinct from
	// RegionsPublished because a publish may partially succeed. The
	// service layer always re-publishes to RegionsFailed on the
	// next activate (see issue #127 Risk 3 — a stale
	// RegionsPublished must not mask a real failure).
	RegionsFailed pq.StringArray `db:"regions_failed"`
	// RegionsCached (issue #332, Layer 3) is the deduped set of
	// regions whose edge-artifact-cache binary currently holds the
	// activation artifact bytes. On a re-activation, publishSwap
	// subtracts this from the cache-push loop so already-cached
	// regions are not re-pushed over the network. The NATS
	// TaskMessage still fires for these regions — only the cache
	// PUT is skipped, since workers may not have received the prior
	// publish (e.g. due to NATS workqueue delay).
	//
	// The DO UPDATE branch of ActiveDeploymentRepository.Set wipes
	// the publish-state columns (RegionsPublished, RegionsFailed,
	// LastPublishAt, LastPublishAttemptID) on re-activation, AND
	// RegionsCached — so a re-activation starts fresh and the next
	// activation retries the cache push from scratch. This matches
	// the "I just activated, so no regions have been processed yet
	// for THIS activation" mental model documented on
	// RegionsPublished above.
	//
	// pq.StringArray for the same reason as Deployment.Regions:
	// lib/pq's Scanner requires it for TEXT[].
	RegionsCached pq.StringArray `db:"regions_cached"`
	// RegionsCacheFailed (issue #332, PR 2 follow-up) is the
	// deduped set of regions whose last cache-push attempt failed.
	// Persisted so an operator can query the row and see which
	// regions are currently failing, and so a future retry that
	// preserves the active row knows which regions to re-push.
	// Mirrors the asymmetry between regions_published (successes)
	// and regions_failed (failures) on the NATS publish side.
	//
	// The same Set wipe contract applies: wiped to '{}' on
	// re-activation of a different deployment_id. PR 2 follow-up
	// makes the wipe conditional on the deployment_id changing so
	// the cache-skip actually engages on canary+promote cycles.
	//
	// pq.StringArray for the same reason as Deployment.Regions.
	RegionsCacheFailed pq.StringArray `db:"regions_cache_failed"`
	// LastPublishAt is the wall-clock timestamp of the most recent
	// (per-region) publish attempt, regardless of outcome. Useful
	// for the operator escape hatch `SELECT last_publish_at FROM
	// active_deployments WHERE ...` to see when the last attempt
	// fired. Nullable because pre-migration-010 rows have no value.
	LastPublishAt *time.Time `db:"last_publish_at"`
	// LastPublishAttemptID is the UUID stamped onto the most recent
	// publish. Lets the operator correlate an active_deployments
	// row to a specific NATS message id when debugging "did the
	// TaskMessage for this activation reach the workers?" — the
	// NATS stream UI surfaces message ids, but the CP-side table
	// needs a copy for the join to be useful.
	LastPublishAttemptID *string `db:"last_publish_attempt_id"`
	// DesiredReplicas is the number of workers that should run this
	// deployment in each region (issue #316). 0 means "no threshold".
	// Copied from the deployments row at activate time.
	DesiredReplicas int `db:"desired_replicas"`
	// PreviewID + PreviewPRNumber (issue #308) are copied from the
	// deployments row at activate time and propagated onto the
	// published TaskMessage so the worker can scope per-preview
	// persistent stores (`<EDGE_KV_STORE_PATH>/{tenant_id}/preview-{id}/`)
	// and stamp `EDGE_PREVIEW_PR_NUMBER` into the guest env. No
	// expiry column here — the active row is short-lived; expiry
	// lives on the deployments row and is the GC's sweep key.
	PreviewID       *string `db:"preview_id"`
	PreviewPRNumber *int    `db:"preview_pr_number"`
}

// AppEnv stores environment variables for an app.
type AppEnv struct {
	TenantID string `db:"tenant_id"`
	AppName  string `db:"app_name"`
	EnvKey   string `db:"env_key"`
	EnvValue string `db:"env_value"`
}
