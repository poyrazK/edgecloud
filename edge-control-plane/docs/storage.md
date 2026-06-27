# Artifact Storage

edgeCloud stores every tenant's compiled WASM artifact somewhere. The
control plane offers three backends, selected at startup via
`storage.artifact_backend` (env `STORAGE_ARTIFACT_BACKEND`). Pick the
one that matches your deployment topology; the wrong choice costs you
either latency (every request crosses the ocean) or a single-region
SPOF (the blob lives on one disk).

## When to pick which

| Backend | Use when |
|---|---|
| `fs` (default) | Single-region control plane. Simplest possible ops — the artifact is a file on the local filesystem. No new infrastructure to run. |
| `s3` | Multi-region control plane with a shared object store (AWS S3, minio, R2, LocalStack). Every CP can read every artifact without a peer relationship. Use this when you already operate S3 or are willing to. |
| `remote` | Multi-region control plane without a shared object store. Each CP caches locally and pulls on miss from a designated "origin" peer CP. Cheaper than S3 for low-volume regions; first request after activation pays cross-region latency once, then every subsequent request hits the local cache. |

## Configuration matrix

| YAML key | Env var | Required for | Notes |
|---|---|---|---|
| `storage.artifact_backend` | `STORAGE_ARTIFACT_BACKEND` | all | `""` or `"fs"` or `"s3"` or `"remote"`. Empty defaults to `fs`. Unknown value fails startup. |
| `storage.artifact_path` | `STORAGE_ARTIFACT_PATH` | `fs`, `remote` | FS root for `fs`; local cache dir for `remote`. Ignored for `s3`. |
| `storage.s3_bucket` | `STORAGE_S3_BUCKET` | `s3` | |
| `storage.s3_region` | `STORAGE_S3_REGION` | `s3` | |
| `storage.s3_endpoint` | `STORAGE_S3_ENDPOINT` | optional | For minio / R2 / LocalStack. Real AWS S3 leaves this empty. |
| `storage.s3_path_style` | `STORAGE_S3_PATH_STYLE` | optional | `true` for minio; `false` for AWS. Default false. |
| `storage.s3_key_prefix` | `STORAGE_S3_KEY_PREFIX` | optional | Namespace a shared bucket across multiple edgeCloud deployments. |
| `storage.peer_control_plane_url` | `STORAGE_PEER_CONTROL_PLANE_URL` | `remote` | Must be `https://`. http:// is rejected at startup. |
| `storage.peer_control_plane_internal_token` | `STORAGE_PEER_CONTROL_PLANE_INTERNAL_TOKEN` | `remote` | Must match the peer's `internal_token` config. Empty is rejected at startup. |

## How the three backends work

### FS (`fs`)

Writes `<artifact_path>/<tenant_id>/<app_name>/<deployment_id>.wasm`.
Same shape the platform has used since v0.1. The migration from `fs`
to `s3` or `remote` requires re-uploading existing artifacts; there is
no in-place migrator today.

### S3 (`s3`)

Writes `s3://<bucket>/[<key_prefix>/]<tenant_id>/<app_name>/<deployment_id>.wasm`
with `Content-Type: application/wasm`. Uses PUT/GET/DELETE against the
S3 REST API with manual SigV4 signing (no AWS SDK dependency — the
signer is ~80 lines of stdlib `crypto/hmac` + `crypto/sha256`).

Credentials come from the standard `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` environment variables at startup. IAM role /
IRSA / instance-profile support is a follow-up; today you must use
static creds (or run minio without auth in dev).

### Remote (`remote`)

A pull-through cache. Each control plane keeps a local FS cache rooted
at `artifact_path`. On `Open`:

1. Look in the local cache. Hit → return.
2. Miss → `GET <peer_url>/api/internal/download/<deployment_id>` with
   `X-Internal-Token: <shared_secret>`.
3. Stream the response body to a staging file, fsync, atomic-rename
   into the cache, then return the cached file.

`Save` writes only to the local cache (the peer CP pulls on first
miss — first-request latency cost paid once, then served locally
forever after). `Delete` removes only the local cache entry; cross-CP
GC is a separate concern (a peer with a stale cached blob is harmless
because the worker re-verifies the artifact hash on every download).

**Threat model.** The shared secret protects the pull-through lane. It
must travel over `https://` — the constructor rejects `http://` peer
URLs at startup so an operator can't accidentally expose it on the
wire. On 4xx/5xx peer responses the body is drained but not returned
to the caller, so a peer that includes diagnostic details (e.g. a
stack trace containing headers) doesn't leak them upward.

**Cold-cache race.** Two concurrent pull-throughs for the same key
both write to the same staging path (`<artifact_path>/.staging/<deployment_id>.tmp`).
Second writer's `os.Create` truncates the first stream; second
`os.Rename` overwrites the first. The end state is consistent
regardless of which writer wins — the file is the same bytes either
way. Cheaper than `singleflight.Group` and survives process restarts.

**Operator escape hatch.** If a region needs to be re-published
because a peer cached a stale blob that the worker's hash check
rejected:

```sql
UPDATE active_deployments
SET regions_published = '{}', regions_failed = '{}'
WHERE tenant_id = $1 AND app_name = $2;
```

Then re-activate. The next publish will include every region in
`deployment.Regions` (the publish-set computation is
`(deployment.Regions ∪ regions_failed) − regions_published`).

## Migrating between backends

`fs` → `s3`: copy every `<artifact_path>/<tenant>/<app>/<d>.wasm`
file to `s3://<bucket>/<key_prefix><tenant>/<app>/<d>.wasm`. Update
config and restart. The control plane does NOT have a built-in
migrator — this is a one-time data motion.

`fs` → `remote`: stand up the peer CP first; copy the existing
artifacts to it. Then point this CP at the peer via
`storage.peer_control_plane_url`. The local cache will populate on
first miss per artifact.

`s3` → `remote`: pull every blob from S3 into the local cache dir
before flipping the backend, OR accept the cold-cache penalty on the
first request per artifact after the switch.

## Per-region publish state

Issue #127 added four columns to `active_deployments` (migration 010):

- `regions_published TEXT[]` — regions the latest activate has
  successfully published to. Default `'{}'`.
- `regions_failed TEXT[]` — regions the latest activate failed to
  publish to. Default `'{}'`.
- `last_publish_at TIMESTAMPTZ` — when the latest activate was
  attempted. `NULL` if never.
- `last_publish_attempt_id UUID` — uniquely identifies the attempt
  (for operator log correlation). `NULL` if never.

The activation idempotency contract is:
```
toPublish = (deployment.Regions ∪ regions_failed) − regions_published
```
i.e. always re-publish anything that failed previously, skip regions
that already succeeded, and if `toPublish` is empty the activation is
treated as a no-op (the row already flipped, which is the correct
semantic).

The 502 response when at least one region failed includes the
per-region breakdown:

```json
{
  "error": "activation committed but worker notification failed; please retry",
  "regions_published": ["us-east"],
  "regions_failed": ["eu-west"]
}
```

Same envelope on the rollback path. Both surfaces use the same
`*service.PublishError` type and the `writePublishFailureEnvelope`
helper.
