package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
)

// publishBuilder centralizes the marshaling of the wire-format
// TaskMessage that accompanies an `active_deployments` mutation.
//
// It used to be a private method on (*DeploymentService) (issue
// #440's pre-#42 path). It is now reused by both the
// activate/rollback paths in deployment.go AND the env-write path
// in env.go (issue #560). The helper is shaped this way — taking
// pre-resolved typed inputs rather than reading from a *sqlx.Tx
// itself — to keep the call sites' tx-snapshot reads in one
// place (the caller, which owns the tx) and the marshaling logic
// in one place (here). Both callers read env / tenant / quota /
// active-dep under their own *sqlx.Tx for atomicity; sharing
// those reads through this helper would re-introduce the
// lock-order hazard issue #440's belt-and-braces check was
// designed to prevent.
//
// The receiver is kept for forward-compatibility — a future
// refactor may move the tx-bound reads into this struct once
// both call sites agree on a single lock-ordering discipline.
// Today it only does marshaling.
type publishBuilder struct{}

// NewPublishBuilder constructs the helper. No deps today.
// Exposed at package level so app.go can wire a single instance
// shared between DeploymentService and EnvService.
func NewPublishBuilder() *publishBuilder {
	return &publishBuilder{}
}

// buildPublishPayload assembles the marshaled TaskMessage that
// accompanies the caller's active_deployments mutation. All
// inputs are pre-resolved under the caller's *sqlx.Tx at the
// call site, so every read participates in the same transaction;
// this function only does the marshaling. Returns the marshaled
// JSON payload ready to be stored on the outbox row.
//
//   - envMap is the freshly-decrypted env var map. Decryption
//     happens at the call site, where the encryptor is reachable,
//     to keep this helper free of dependencies on EnvService's
//     optional nil-safe state. Decrypt errors are fatal at the
//     call site before this function is invoked.
//   - The tenant-disabled gate is the caller's responsibility
//     (typically via lockTenantForUpdate). Both the activate
//     and env-write paths hold tenants FOR UPDATE before
//     reaching here so a racing disable can never sneak in.
//   - max_memory_mb is derived from the quota row via
//     perAppMemoryMB; the constant default (256 MB) matches the
//     pre-#44 behavior the runtime expects.
//
// `deployment` carries the artifact hash, signature,
// signing_key_id, and preview linkage that BuildAppConfig threads
// into AppConfig.
//
// `tenant` carries the per-tenant allowlist (passed through to
// AppConfig.Allowlist). Required — a nil tenant is a programming
// error here and is rejected so we never publish to a worker
// without the allowlist enforcement context.
//
// `regions` is the pre-resolved fan-out target set (activate's
// default-region fallback OR the env-write's deployment-region
// read). This function does NOT re-resolve regions — the caller's
// job, because each path has its own region-selection policy.
// regions is also intentionally NOT propagated onto the wire —
// TaskMessage carries a tenant-scoped `task_update` and the
// publisher in nats/publisher.go fans it out to every region in
// the outbound stream. Including it here would be redundant
// with the publisher's stream-config region set.
//
// Issue #548: `deployment.Protocol` is read by `nats.BuildAppConfig`
// to derive the per-app `socket_mode` override (`"allow-all"` for
// tcp, empty for http). Previously this was a separate `socketMode`
// parameter that every caller forgot to pass — adding the
// `protocol` parameter to `BuildAppConfig` itself forces every
// call site to think about the protocol, eliminating the
// forgotten-override bug.
func (b *publishBuilder) buildPublishPayload(
	_ context.Context,
	tenantID, appName, deploymentID string,
	deployment *domain.Deployment,
	tenant *domain.Tenant,
	regions []string,
	quota *domain.Quota,
	envMap map[string]string,
) ([]byte, error) {
	if tenantID == "" || appName == "" || deploymentID == "" {
		return nil, fmt.Errorf("buildPublishPayload: tenantID, appName, deploymentID must be non-empty (got %q,%q,%q)", tenantID, appName, deploymentID)
	}
	if deployment == nil {
		return nil, fmt.Errorf("buildPublishPayload: deployment is nil")
	}
	if tenant == nil {
		return nil, fmt.Errorf("buildPublishPayload: tenant is nil")
	}

	maxMemoryMB := int(perAppMemoryMB(quota))

	cfg := nats.BuildAppConfig(
		deploymentID,
		deployment.Hash,
		deployment.Signature,
		deployment.SigningKeyID, // issue #307 PR1: per-key kid
		previewIDFromDeployment(deployment),
		previewPRNumberFromDeployment(deployment),
		envMap,
		tenant.AllowlistedDestinations,
		maxMemoryMB,
	)

	msg := &nats.TaskMessage{
		Type:      "task_update",
		Timestamp: time.Now().UTC(),
		TenantID:  tenantID,
		Apps: map[string]nats.AppConfig{
			appName: cfg,
		},
	}

	return json.Marshal(msg)
}

// perAppMemoryMB picks the per-app memory cap from a quota row.
// Returns 256 MB (the pre-#44 default) when the quota is nil or
// the row carries MaxMemoryMB <= 0. Promoted from
// (*DeploymentService).perAppMemoryMB when buildPublishPayload
// was extracted to publishBuilder in issue #560; the function has
// no receiver state and is now shared by both DeploymentService
// and EnvService.
func perAppMemoryMB(quota *domain.Quota) int64 {
	if quota != nil && quota.MaxMemoryMB > 0 {
		return int64(quota.MaxMemoryMB)
	}
	return 256
}
