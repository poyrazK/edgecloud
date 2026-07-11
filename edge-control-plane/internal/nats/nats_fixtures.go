package nats

import _ "embed"

// Wire-contract fixtures for issue #610. These JSON blobs are the
// cross-language source of truth for the NATS payloads that flow between
// the control plane (Go) and the worker (Rust). They are decoded from
// both languages — see internal/nats/wire_contract_test.go on the Go side
// and edge-worker/tests/nats_wire_contract.rs on the Rust side.
//
// ⚠ The fixture values are PLACEHOLDERS. The allowlist hosts
// (api.stripe.com, *.sendgrid.net) in task_update.json are NOT a real
// egress allowlist. The deployment hashes are `0123…` and `fedcba…`
// hex strings. The deployment_signature value happens to be valid
// base64url but is NOT a valid Ed25519 signature over the listed hash.
// Do not copy these values into production code or docs.
//
// ⚠ When adding a new `TaskMessage` / `HeartbeatMessage` variant or a new
// `PurgeReason`:
//  1. Add the JSON fixture here AND extend BOTH test files.
//  2. The Go side decodes any string value for `PurgeReason` (string alias,
//     no UnmarshalJSON); the Rust side is a closed enum
//     (`#[serde(rename_all = "snake_case")]`, no `#[serde(other)]`). The
//     `task_purge_unknown_reason.json` fixture exercises this asymmetry
//     and BOTH tests assert their own behavior. Adding a new reason on
//     one side without the other will silently leave the asymmetry
//     test green for the side that didn't change — re-read the inline
//     comments at `task_purge_unknown_reason` in both test files before
//     shipping a new variant.
//
//go:embed testdata/task_update.json
var taskUpdateFixture []byte

//go:embed testdata/task_update_minimal.json
var taskUpdateMinimalFixture []byte

//go:embed testdata/full_sync.json
var fullSyncFixture []byte

//go:embed testdata/task_purge_per_app.json
var taskPurgePerAppFixture []byte

//go:embed testdata/task_purge_tenant_wide.json
var taskPurgeTenantWideFixture []byte

//go:embed testdata/task_purge_unknown_reason.json
var taskPurgeUnknownReasonFixture []byte

//go:embed testdata/heartbeat.json
var heartbeatFixture []byte

//go:embed testdata/heartbeat_minimal.json
var heartbeatMinimalFixture []byte
