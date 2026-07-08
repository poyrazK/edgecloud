#!/usr/bin/env bash
# scripts/lib/verify.sh — post-launch smoke checks. Prints [verify] lines
# to stderr; exits 0 only if every check passes.
#
# Checks:
#   1. docker compose ps: postgres + nats healthy
#   2. CP /health returns 200
#   3. Caddy admin returns 200 on :2019 with admin.listen = 0.0.0.0:2019
#   4. NATS /jsz?streams=true shows >= 1 stream
#   5. Edge status shows the sample deployment as running
#   6. Direct worker hit on the seeded worker_port returns 200
#
# Globals expected:
#   REPO_ROOT, EDGECLOUD_HOME, SEED_FILE (path to seed.json)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SEED_FILE="${SEED_FILE:-$EDGECLOUD_HOME/state/seed.json}"

PASS=0
FAIL=0

ok()   { echo "[verify] OK   $*" >&2; PASS=$((PASS+1)); }
bad()  { echo "[verify] FAIL $*" >&2; FAIL=$((FAIL+1)); }

# ── 1. Postgres + nats healthy ──────────────────────────────────────────

if ( cd "$REPO_ROOT" && docker compose ps --format json 2>/dev/null \
     | jq -r '.[] | select(.Name=="edgecloud-postgres" or .Name=="edgecloud-nats") | .Health' \
     | sort -u ) | grep -q '^healthy$' \
   && ( cd "$REPO_ROOT" && docker compose ps --format json 2>/dev/null \
        | jq -r '.[] | select(.Name=="edgecloud-postgres" or .Name=="edgecloud-nats") | .Health' \
        | wc -l | tr -d ' ' ) | grep -q '^2$'; then
  ok "postgres + nats healthy"
else
  bad "postgres + nats not both healthy (docker compose ps)"
fi

# ── 2. CP health ────────────────────────────────────────────────────────

if curl -fsS --max-time 5 http://127.0.0.1:8080/health >/dev/null 2>&1; then
  ok "control plane /health on :8080"
else
  bad "control plane /health unreachable"
fi

# ── 3. Caddy admin ──────────────────────────────────────────────────────

CADDY_ADMIN_LISTEN="$(curl -fsS --max-time 5 http://127.0.0.1:2019/config/ 2>/dev/null \
  | jq -r '.admin.listen' 2>/dev/null || true)"
if [[ "$CADDY_ADMIN_LISTEN" == "0.0.0.0:2019" ]]; then
  ok "caddy admin on :2019 (admin.listen=$CADDY_ADMIN_LISTEN)"
else
  bad "caddy admin not on 0.0.0.0:2019 (got '$CADDY_ADMIN_LISTEN')"
fi

# ── 4. NATS JetStream ───────────────────────────────────────────────────

NATS_STREAMS="$(curl -fsS --max-time 5 'http://127.0.0.1:8222/jsz?streams=true' 2>/dev/null \
  | jq -r '.streams | length' 2>/dev/null || echo 0)"
if [[ "${NATS_STREAMS:-0}" -ge 1 ]]; then
  ok "nats JetStream streams active ($NATS_STREAMS)"
else
  bad "nats JetStream no streams (CP may not have started its consumer yet)"
fi

# ── 5. Edge status ──────────────────────────────────────────────────────

if [[ -f "$SEED_FILE" ]]; then
  DEPLOY_ID="$(jq -r '.deployment_id' "$SEED_FILE")"
  EDGE_BIN="$REPO_ROOT/target/release/edge"
  if [[ -x "$EDGE_BIN" && -n "$DEPLOY_ID" && "$DEPLOY_ID" != "null" ]]; then
    if EDGE_API_URL="http://127.0.0.1:8080" "$EDGE_BIN" status \
         --deployment "$DEPLOY_ID" 2>&1 | grep -qE 'running|active'; then
      ok "edge status: deployment $DEPLOY_ID running"
    else
      bad "edge status: deployment $DEPLOY_ID not running"
    fi
  else
    bad "edge CLI or deployment_id missing; skipping status check"
  fi
else
  bad "seed.json missing; run seed-sample.sh first"
fi

# ── 6. Direct worker hit ────────────────────────────────────────────────

if [[ -f "$SEED_FILE" ]]; then
  WORKER_PORT="$(jq -r '.worker_port' "$SEED_FILE")"
  APP_NAME="$(jq -r '.app_name' "$SEED_FILE")"
  if [[ -n "$WORKER_PORT" && "$WORKER_PORT" != "null" && -n "$APP_NAME" ]]; then
    # Wait up to 30s for the worker to start serving.
    DEADLINE=$(( $(date +%s) + 30 ))
    HIT=false
    while [[ $(date +%s) -lt $DEADLINE ]]; do
      if curl -fsS --max-time 3 "http://127.0.0.1:${WORKER_PORT}/hello" >/dev/null 2>&1 \
         || curl -fsS --max-time 3 "http://127.0.0.1:${WORKER_PORT}/" >/dev/null 2>&1; then
        HIT=true
        break
      fi
      sleep 2
    done
    if $HIT; then
      ok "worker responds on :${WORKER_PORT}"
    else
      bad "worker not responding on :${WORKER_PORT} (may need up to 30s post-activate)"
    fi
  fi
fi

# ── Summary ─────────────────────────────────────────────────────────────

echo "[verify] ${PASS} passed, ${FAIL} failed" >&2
if [[ $FAIL -gt 0 ]]; then
  exit 1
fi
exit 0