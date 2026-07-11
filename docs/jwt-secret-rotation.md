# JWT Secret Rotation

The cluster `JWT_SECRET` is the HKDF input for **every** per-worker
derived secret (issue #430). A single rotation therefore invalidates
every worker's HS256 secret in one shot. This runbook covers the
two-stage rollout the safe rotation path requires.

For the **bootstrap handshake** (the `BOOTSTRAP_SECRET` that lets a
fresh worker exchange for a per-worker derived secret), see
[`jwt-bootstrap.md`](jwt-bootstrap.md). That runbook still applies
unchanged; this document is the per-worker-key rotation path.

## Threat model

| Scenario | Outcome under pre-#430 | Outcome under #430 |
|---|---|---|
| `BOOTSTRAP_SECRET` leaks | Attacker can mint bootstrap JWTs → `GET /api/internal/worker-secret` returns cluster `JWT_SECRET` → cluster-wide forgery (CRITICAL). | Attacker can mint bootstrap JWTs but cannot complete enrollment without the worker's Ed25519 private key. The bootstrap JWT alone is useless. |
| One worker is compromised | Compromised worker exfiltrates `JWT_SECRET` and can forge JWTs for **every** worker cluster-wide (CRITICAL). | Compromised worker only has its own derived secret. Forging a JWT for worker B requires either the CP's `JWT_SECRET` (HKDF input) or worker B's `ED25519` private key. Neither is exposed by the CP to worker A. |
| Cluster `JWT_SECRET` rotates | Every worker 401s until restart with the new value. | Same — HKDF is a pure function of `JWT_SECRET`, so a rotation invalidates every derived secret at once. |

The remaining failure mode (rotating the cluster `JWT_SECRET` invalidates every worker) is the topic of this runbook.

## The keyring rotation pattern

`JWT_SECRET` is now an HKDF input, not a directly-shared verification secret. Rotating it without downtime requires that the CP accept **two** active inputs for the verification window: the old one (still trusted by not-yet-rotated workers) and the new one (used by workers that have already re-enrolled).

The control plane already supports multi-kid verification via the `JWT_KEY_<kid>` / `JWT_ACTIVE_KID` env vars (`internal/signing/keyring.go`). The rotation procedure is therefore:

1. **Add the new kid** — set `JWT_KEY_<newkid>` on the CP alongside the existing keyring. The CP keeps the existing `JWT_ACTIVE_KID` so existing tokens still verify.
2. **Drain the worker fleet** — every worker restarts with `EDGE_WORKER_REENROLL_ON_BOOT=true`. Each worker re-enrolls and the CP re-derives the per-worker secret from the **new** `JWT_SECRET` (since the keyring's HKDF input is whichever key the active kid points to — see step 3). The CP stores the new `workers.public_key` enrollment alongside the new derived secret.
3. **Flip the active kid** — set `JWT_ACTIVE_KID=<newkid>`. The CP now signs new cluster-wide tokens (used by `mintIngressToken` and any pre-rotation JWTs) with the new key, and verifies tokens whose `kid` is the new one against the new key.
4. **Clean up** — once the fleet is fully on the new kid, unset `JWT_KEY_<oldkid>` from the CP. Tokens still using the old kid will now 401 — this is the recovery signal that rotation is complete and an old-kid window is no longer required.

### Why three steps, not one

Setting `JWT_ACTIVE_KID=<newkid>` without first adding the new key to the keyring is a fail-fast (the CP refuses to start). Setting `JWT_KEY_<newkid>` alone (without flipping the active kid) is a no-op for verification (the active kid still routes to the old key). The order — add → drain → flip → cleanup — guarantees the CP never serves a token signed by a key it can't verify, and never refuses a token signed by a key it does have.

### Per-worker re-enrollment during rotation

`EDGE_WORKER_REENROLL_ON_BOOT=true` (edge-worker config) forces the worker to skip its persisted `identity.key` and run the bootstrap enrollment handshake fresh. The CP re-derives the worker's HS256 secret from the (possibly-new) cluster `JWT_SECRET` and returns it under the same `wkr_<hex>` kid. The worker's Ed25519 identity keypair is reused — only the derived HS256 secret rotates.

After all workers have restarted with the flag, the fleet is fully on the new kid and the operator can remove the old `JWT_KEY_<oldkid>`.

## Migration: existing clusters upgrading from pre-#430

Pre-#430 workers authenticate with a static `WORKER_JWT_SECRET` that **is** the cluster `JWT_SECRET`. Post-#430, workers run the bootstrap handshake and never see the cluster secret directly. The migration sequence is:

1. **CP upgrade first.** Operators deploy the new control plane image; it accepts both the legacy `/worker-secret` mount (handled by `EnrollWorker`'s sibling) AND the new `/worker-bootstrap/enroll`. The legacy path is removed in the same release, so any operator who hasn't drained their fleet by then has workers stuck on a 5-min bootstrap JWT loop. This window is intentional — see the PR description for the rollout strategy.
2. **Worker upgrade.** Each worker is redeployed with the new binary. On first boot it generates an Ed25519 identity keypair (`.worker-cache/identity.key`, mode 0600) and runs the three-phase handshake. The CP stores the worker's `public_key` on `workers.public_key` (migration 032) and returns the derived secret + `kid`.
3. **Steady state.** Subsequent restarts load the persisted identity and skip the handshake. The cluster `JWT_SECRET` is now an HKDF input that never leaves the CP.

Operators who can't redeploy workers within the upgrade window should set `WORKER_JWT_SECRET` directly on each worker (the legacy escape hatch in `main.rs::resolve_jwt_secret`). This keeps the worker operational but reopens the pre-#430 trust model — **only use this as a temporary bridge during the migration**, and remove it as soon as the worker is upgraded.

## Operator checklist

- [ ] **Plan the rotation.** Pick a maintenance window long enough that every worker in the fleet can restart with `EDGE_WORKER_REENROLL_ON_BOOT=true`. For a 100-worker fleet with rolling restarts, budget ~30 minutes.
- [ ] **Add the new key.** Set `JWT_KEY_<newkid>=<hex>` on the CP. The CP does NOT require the new key to be the active kid — adding it to the keyring is enough to prevent fail-fast on flip.
- [ ] **Restart every worker with the flag.** Use your existing rolling-restart orchestration (`kubectl rollout restart deployment/edge-worker`, etc.) with `EDGE_WORKER_REENROLL_ON_BOOT=true` injected. Each worker logs `[bootstrap] running bootstrap enrollment handshake with control plane` then `[bootstrap] persisted worker identity; subsequent restarts skip bootstrap`.
- [ ] **Flip the active kid.** Set `JWT_ACTIVE_KID=<newkid>` on the CP and restart it. Tokens signed under the new kid now verify; tokens still using the old kid verify only as long as `JWT_KEY_<oldkid>` is in the keyring.
- [ ] **Wait for the fleet to drain.** The rollout completes when no worker has been observed authenticating with the old kid for at least one reconcile cycle (5 min default). The CP's audit log (`workers.last_enrolled_at`) is the load-bearing source of truth here.
- [ ] **Remove the old key.** Unset `JWT_KEY_<oldkid>` from the CP and restart it. Any worker still on the old kid now 401s — fix by re-running step 3 (set `EDGE_WORKER_REENROLL_ON_BOOT=true` on the offending worker).
- [ ] **Unset the flag.** Once the fleet is stable, unset `EDGE_WORKER_REENROLL_ON_BOOT` from your orchestration so a future unrelated restart doesn't trigger an unnecessary re-enrollment.

## Failure modes

| Symptom | Cause | Recovery |
|---|---|---|
| `EDGE_REQUIRE_SIGNATURE=true but no keyring configured` at worker startup | Worker started with no `EDGE_SIGNING_KEYRING` / `EDGE_SIGNING_KEYRING_PATH`. Pre-existing issue, not #430-specific — see `docs/jwt-bootstrap.md`. | Set the keyring env vars. |
| `enrollment public_key mismatch` at worker | The CP stored a different public_key than the worker sent (man-in-the-middle or operator error). | Abort the rotation; investigate the network path between the worker and the CP. The mismatch is logged in `workers.public_key` — diff against the worker's `identity.key`. |
| `bootstrap handshake failed: 401` at worker startup | The cluster's `BOOTSTRAP_SECRET` no longer matches `WORKER_BOOTSTRAP_SECRET`. | Re-sync the bootstrap secret on both sides. |
| Worker 401s on `/api/internal/*` after rotation | Worker's persisted `identity.key` is from before the rotation and the old kid is no longer in the keyring. | Set `EDGE_WORKER_REENROLL_ON_BOOT=true` on the offending worker and restart. |

## Related docs

- [`jwt-bootstrap.md`](jwt-bootstrap.md) — the pre-existing `BOOTSTRAP_SECRET` runbook. Still required; this document does not replace it.
- [`CLAUDE.md`](../CLAUDE.md) — the signing keyring + per-worker `wkr_` kid namespace summary.
- `edge-control-plane/internal/signing/worker_key.go` — `DeriveWorkerSecret` (the HKDF call referenced above).
- `edge-control-plane/internal/middleware/worker.go::resolveKey` — the per-worker verification branch that consults `WorkerKeyCache`.
- `edge-worker/src/bootstrap.rs` — the worker's three-phase handshake client.
- `edge-worker/src/worker_key.rs` — the worker's Ed25519 identity loader.