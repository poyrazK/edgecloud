package nats

import _ "embed"

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
