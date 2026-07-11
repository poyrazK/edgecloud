# JWT Bootstrap Handshake

edgeCloud workers authenticate with the control plane via HMAC-SHA256 JWTs
for all `/api/internal/*` calls (downloads, logs, registration, auto-rollback).
The JWT secret (`JWT_SECRET`) must match between the control plane and every
worker.

This document describes how to bootstrap workers that don't yet have the
JWT secret, using a shared bootstrap secret.

## Quick Start

### 1. Generate a bootstrap secret

On the control plane host:

```bash
openssl rand -hex 32
# → e.g. 7f3b... (64 hex chars = 32 bytes)
```

### 2. Set on the control plane

```yaml
# config.yaml
bootstrap:
  secret: "7f3b..."
```

Or via env var:

```bash
export BOOTSTRAP_SECRET="7f3b..."
```

### 3. Set on workers

```bash
export WORKER_BOOTSTRAP_SECRET="7f3b..."
# Do NOT set WORKER_JWT_SECRET — the bootstrap handshake will fetch it
```

### 4. Start the worker

The worker detects that `WORKER_JWT_SECRET` is empty and
`WORKER_BOOTSTRAP_SECRET` is set, then performs the bootstrap handshake:

```
Worker                          CP
  │                               │
  │ POST /api/internal/bootstrap  │  HMAC-SHA256(worker_id:region:tenant_id:ts:nonce)
  │──────────────────────────────►│  signed with BOOTSTRAP_SECRET
  │                               │  verify HMAC, issue 5min bootstrap JWT
  │ 200 { token: "<bootstrap_jwt>"}│
  │◄──────────────────────────────│
  │                               │
  │ GET /api/internal/worker-secret│  Authorization: Bearer <bootstrap_jwt>
  │──────────────────────────────►│  verify bootstrap JWT (separate key)
  │ 200 { secret: "<JWT_SECRET>" }│
  │◄──────────────────────────────│
  │                               │
  │ cache secret in memory        │
  │ proceed as normal             │
```

If the bootstrap fails (wrong secret, network issue) the worker exits with
a non-zero status — the supervisor (systemd, k8s) will restart it.

## How It Works

### Phase 1: Sign and Verify

The worker signs a payload with the shared bootstrap secret using
HMAC-SHA256:

```
payload = worker_id + ":" + region + ":" + tenant_id + ":" + timestamp + ":" + nonce
signature = HMAC-SHA256(payload, BOOTSTRAP_SECRET)
```

The control plane verifies the signature and returns a short-lived bootstrap
JWT (5-minute TTL) that authorizes only `GET /api/internal/worker-secret`.

### Phase 2: Fetch the Real Secret

The worker presents the bootstrap JWT as a Bearer token to
`GET /api/internal/worker-secret`. The control plane verifies the bootstrap
JWT (using the same `BOOTSTRAP_SECRET`) and returns the real `JWT_SECRET`.

### Security Properties

| Property | Mechanism |
|----------|-----------|
| Replay protection | Timestamp (+/- 5min window) + random nonce per request |
| Short-lived bootstrap JWT | 5-minute TTL |
| Bootstrap secret != JWT secret | Separate keys, rotate independently |
| Rate limiting | Bootstrap endpoint: 5 req/min per IP |
| Audit trail | Every bootstrap and secret-fetch is logged |

## Secret Rotation

### Rotating the Bootstrap Secret

The bootstrap secret is only used for the initial handshake — workers cache
the JWT secret after bootstrapping. You can rotate `BOOTSTRAP_SECRET` at any
time without affecting running workers.

1. Update `BOOTSTRAP_SECRET` on the control plane
2. Update `WORKER_BOOTSTRAP_SECRET` on new workers
3. Restart existing workers if they need to re-bootstrap

### Rotating the JWT Secret

The JWT secret is consumed by every worker for authentication. Follow the
keyring approach (see `config.yaml` for `jwt.keys`):

1. Add the new secret as a new key in `jwt.keys` with a unique `kid`
2. Set `jwt.active_kid` to the new key — new tokens use this key
3. Old tokens signed with the previous key are still accepted
4. Restart workers (or run the bootstrap handshake) so they pick up the
   new active key
5. Remove the old key once all tokens have expired (24h default)

## Environment Reference

| Env Var | Required | Description |
|---------|----------|-------------|
| `BOOTSTRAP_SECRET` | Optional | Shared HMAC secret (≥32 bytes) for the bootstrap handshake |
| `WORKER_BOOTSTRAP_SECRET` | Optional | Must match `BOOTSTRAP_SECRET` on the CP |
| `WORKER_JWT_SECRET` | Optional* | Direct JWT secret — falls back to bootstrap when empty |

\* One of `WORKER_JWT_SECRET` or `WORKER_BOOTSTRAP_SECRET` must be set,
or `/api/internal/*` calls will return 401.

## Troubleshooting

### Worker exits with "bootstrap handshake failed"

Check that `BOOTSTRAP_SECRET` on the CP matches `WORKER_BOOTSTRAP_SECRET`
on the worker. The bootstrap endpoint logs on the CP:

```
bootstrap: invalid signature for worker w_fra_abc (tenant=t_test, region=fra)
```

### Worker starts but registration fails with 401

The bootstrap handshake succeeded but the fetched JWT secret doesn't match
the CP's `JWT_SECRET`. Verify `JWT_SECRET` is the same on both sides.

### No logs from the worker

If the bootstrap succeeded but logs still don't arrive, the worker's JWT
signer may have a cached token signed with an empty secret from before the
bootstrap. Restart the worker — the fix in `main.rs` now resolves the
secret before creating the signer.

## Related

- [`jwt-secret-rotation.md`](jwt-secret-rotation.md) — rotating the
  cluster `JWT_SECRET` after every worker has been upgraded to the
  per-worker derived-secret model (issue #430). The `BOOTSTRAP_SECRET`
  rotation procedure described above is unchanged; the new runbook
  covers the per-worker `JWT_KEY_<kid>` / `JWT_ACTIVE_KID` flow.
