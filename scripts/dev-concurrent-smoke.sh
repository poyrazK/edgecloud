#!/usr/bin/env bash
# scripts/dev-concurrent-smoke.sh — end-to-end smoke for issue #663
# (per-tenant concurrent-request cap, sub-feature #2 of #305).
#
# Walks the full concurrent-cap path against a local dev stack
# (`scripts/dev-up.sh` must be running):
#   1. Build the xcaddy `edgecloud/caddy-concurrent` image (or reuse it
#      if present — the same build-if-missing pattern as
#      `scripts/dev-l4-smoke.sh`).
#   2. Bring Caddy up against the plugin-enabled image
#      (idempotent — `docker compose up -d caddy` is a no-op if
#      already running).
#   3. Set `concurrent_limit=5` on the seeded dev tenant via
#      `PUT /api/v1/admin/tenants/{tenant_id}/rate-limit`. The
#      seeded key is owner-role (per `internal/service/tenant.go:261`
#      `mintAPIKey(..., RoleOwner)`) so the admin endpoint accepts
#      the write.
#   4. Wait for the ingress cache to refresh (default
#      `TENANT_RATE_LIMIT_FETCH_INTERVAL=30s`, mirrored by
#      `edge-ingress::config::tenant_rate_limit_fetch_interval`).
#      We poll Caddy's admin API for the `tenant-concurrent:<id>`
#      route to appear so the test does not sleep for the full
#      default 30s when the operator has shortened the interval.
#   5. Fire 6 concurrent slow-read requests against the seeded
#      `<tenant>-hello.edgecloud.dev` host via Caddy. We use
#      `curl --limit-rate 1 --max-time 30` so curl holds the
#      connection open for the full 30s, draining the response at
#      1 byte/sec — that's the Caddy-side in-flight window the
#      concurrent cap counts. With `concurrent_limit=5` the
#      first 5 must reach the worker (200) and the 6th must be
#      rejected with 429 + `Retry-After: 1` by the
#      `tenant_concurrent` handler.
#   6. Reset the cap to 0 so a re-run starts from a clean slate.
#      Best-effort — a cleanup failure logs a warning but does
#      not fail the smoke (the test still proved the cap fired).
#
# Pre-reqs: docker, the edge CLI on PATH (or built at
# `target/release/edge`), the control plane reachable at $EDGE_API_URL
# (default http://localhost:8080), and a NATS server the workers can
# reach. `scripts/dev-up.sh` stands all of that up; this script only
# exercises the concurrent-cap enforcement on top.
#
# Usage:
#   EDGE_API_KEY=dev-key ./scripts/dev-concurrent-smoke.sh
#
# On success the script prints
# `PASS: <app> concurrent cap (5 in-flight → (cap+1)th gets 429 + Retry-After: 1)`
# and exits 0. On any failure it prints a
# `FAIL: <reason>` line and exits 1.

set -euo pipefail

# Resolve repo root from the script's location so the script can be
# run from any cwd.
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

# Configurable bits. EDGE_API_KEY and EDGE_API_URL override via env.
: "${EDGE_API_KEY:=dev-key}"
: "${EDGE_API_URL:=http://localhost:8080}"
: "${EDGE_CADDY_ADMIN_URL:=http://localhost:2019}"
: "${CADDY_HOST:=127.0.0.1}"
: "${CONCURRENT_LIMIT:=5}"   # how many in-flight requests the cap allows
: "${SAMPLE_APP:=hello}"
: "${HOLD_SECONDS:=30}"      # how long each in-flight request is held
: "${ROUTE_WAIT_SECS:=35}"   # how long we wait for Caddy to receive the new route

SEED_FILE="${EDGECLOUD_HOME:-$HOME/.edgecloud}/state/seed.json"

fail() {
    echo "FAIL: $1" >&2
    exit 1
}

# ── 1. Build the xcaddy image ──────────────────────────────────────────
# Reuse scripts/dev-l4-smoke.sh's build-if-missing pattern. The
# concurrent image is a strict superset of `edgecloud/caddy-l4:latest`
# (it includes the mholt/caddy-l4 plugin AND the first-party
# tenant_concurrent HTTP middleware), so building this also satisfies
# the L4 smoke's prerequisite if it runs after.
if ! docker image inspect edgecloud/caddy-concurrent:latest >/dev/null 2>&1; then
    docker build \
        -t edgecloud/caddy-concurrent:latest \
        -f "$REPO_ROOT/edge-ingress/Dockerfile.caddy-concurrent" \
        "$REPO_ROOT" \
        || fail "xcaddy build (edgecloud/caddy-concurrent) failed"
fi

# ── 2. Bring Caddy up against the plugin-enabled image ────────────────
# `scripts/dev-up.sh` already runs Caddy with this image (it builds
# if missing at boot). `docker compose up -d caddy` is a no-op if
# the container is already running with the right image, and a fresh
# start if the operator restarted `dev-up.sh` after a host reboot.
( cd "$REPO_ROOT" && docker compose up -d caddy ) \
    || fail "docker compose up -d caddy"

# ── 3. Read seeded tenant_id + verify the API key authenticates ───────
[[ -f "$SEED_FILE" ]] \
    || fail "seed file not found at $SEED_FILE; run scripts/dev-up.sh first"

TENANT_ID=$(jq -r '.tenant_id' "$SEED_FILE")
APP_NAME=$(jq -r '.app_name' "$SEED_FILE")
[[ -n "$TENANT_ID" && "$TENANT_ID" != "null" ]] \
    || fail "could not extract tenant_id from $SEED_FILE"
[[ -n "$APP_NAME" && "$APP_NAME" != "null" ]] \
    || fail "could not extract app_name from $SEED_FILE"

# Use the persisted api_key (the default in $EDGE_API_KEY is the
# placeholder `dev-key`, not the real one). Fall back to $EDGE_API_KEY
# only if the seed file doesn't have one.
PERSISTED_KEY=$(jq -r '.api_key // empty' "$SEED_FILE")
if [[ -n "$PERSISTED_KEY" ]]; then
    EDGE_API_KEY="$PERSISTED_KEY"
fi

# whoami probe — surface a 401 here with a clear message rather
# than the 401 deep in the rate-limit PUT below.
if ! curl -fsS -H "Authorization: Bearer $EDGE_API_KEY" \
        "$EDGE_API_URL/api/auth/whoami" >/dev/null 2>&1; then
    fail "api key $EDGE_API_KEY did not authenticate against $EDGE_API_URL"
fi

# ── 4. Set concurrent_limit via the owner-only admin endpoint ─────────
# The renderer's `concurrent_caps()` iterator filters on
# `concurrent_limit > 0`, so a 0-value clears the cap and a
# positive value enables it. We pin the cap to $CONCURRENT_LIMIT
# (default 5) for the duration of the test.
echo "[smoke] setting $TENANT_ID concurrent_limit=$CONCURRENT_LIMIT"
curl -fsS -X PUT \
    -H "Authorization: Bearer $EDGE_API_KEY" \
    -H "Content-Type: application/json" \
    -d "{\"concurrent_limit\": $CONCURRENT_LIMIT}" \
    "$EDGE_API_URL/api/v1/admin/tenants/$TENANT_ID/rate-limit" \
    >/dev/null \
    || fail "PUT rate-limit returned non-2xx"

# Cleanup on exit — best-effort reset to concurrent_limit=0 so a
# re-run starts clean. We trap on EXIT so a mid-script failure
# still resets; the reset itself is best-effort and logs but
# does not fail the test (the operator can re-run it).
reset_cap() {
    curl -fsS -X PUT \
        -H "Authorization: Bearer $EDGE_API_KEY" \
        -H "Content-Type: application/json" \
        -d '{"concurrent_limit": 0}' \
        "$EDGE_API_URL/api/v1/admin/tenants/$TENANT_ID/rate-limit" \
        >/dev/null 2>&1 \
        || echo "[smoke] WARN: failed to reset concurrent_limit=0" >&2
}
trap reset_cap EXIT

# ── 5. Wait for Caddy to receive the new tenant_concurrent route ──────
# The ingress polls the CP every
# `TENANT_RATE_LIMIT_FETCH_INTERVAL` (default 30s, see
# `edge-ingress/src/config.rs::tenant_rate_limit_fetch_interval`),
# then re-renders and POSTs `/load` to Caddy. We poll Caddy's admin
# API for the `tenant-concurrent:<tenant_id>` route to appear so the
# test does not sleep for the full default 30s when the operator
# has shortened the interval.
ROUTE_ID="tenant-concurrent:$TENANT_ID"
DEADLINE=$(( $(date +%s) + ROUTE_WAIT_SECS ))
while [[ $(date +%s) -lt $DEADLINE ]]; do
    if curl -fsS "$EDGE_CADDY_ADMIN_URL/config/" 2>/dev/null \
        | jq -e --arg id "$ROUTE_ID" '.apps.http.servers.edge_https.routes // [] | map(select(.["@id"] == $id)) | length > 0' \
        >/dev/null; then
        echo "[smoke] caddy route $ROUTE_ID present"
        break
    fi
    sleep 1
done
if ! curl -fsS "$EDGE_CADDY_ADMIN_URL/config/" 2>/dev/null \
    | jq -e --arg id "$ROUTE_ID" '.apps.http.servers.edge_https.routes // [] | map(select(.["@id"] == $id)) | length > 0' \
    >/dev/null; then
    fail "caddy route $ROUTE_ID did not appear within $ROUTE_WAIT_SECS s; the ingress cache tick may be longer than ROUTE_WAIT_SECS or the rate-limit write did not propagate"
fi

# ── 6. Fire N+1 concurrent slow-read requests ─────────────────────────
# The `<tenant>-<app>.edgecloud.dev` host is what Caddy's
# `host_regexp` matches. We pass the host via `-H Host: ...` and
# connect to 127.0.0.1 over plain HTTP (`scripts/dev-up.sh` runs
# Caddy in plain-HTTP mode for dev; production uses TLS via the
# `tls.on_demand: {}` block). The dev cert is self-signed — we use
# `-k` to skip the verify.
#
# Mechanics: the first N requests use `--limit-rate 1 --max-time
# $HOLD_SECONDS` so curl drains the response at 1 byte/sec. The
# response body is small (~100 bytes for the `hello` sample) so the
# `--max-time` will fire before the body fully drains, but the
# *connection* stays open until curl exits, which is what the
# Caddy-side in-flight counter cares about. The (N+1)th request
# uses `--max-time 10` (no limit-rate) — Caddy will reject it
# synchronously with 429 + Retry-After: 1 within milliseconds, and
# curl reports the http_code immediately.
#
# We only assert on the (N+1)th's status code. The first N
# background curls will eventually report `28` (CURLE_OPERATION_
# TIMEDOUT) when `--max-time` fires — that's the *curl* exit code,
# not the HTTP status. We deliberately do NOT read those status
# files; the slot-held behavior is what we need from them, not
# their reported status.
HOST="${TENANT_ID}-${APP_NAME}.edgecloud.dev"

echo "[smoke] firing $CONCURRENT_LIMIT in-flight + 1 over-cap request against $HOST (cap=$CONCURRENT_LIMIT)"
HOLD_PIDS=()
for i in $(seq 1 "$CONCURRENT_LIMIT"); do
    # Hold a slot for HOLD_SECONDS. We don't capture or assert
    # on the curl exit code; what matters is the TCP connection
    # stays open for the Caddy-side in-flight counter.
    curl -sS -o /dev/null \
        -k \
        --limit-rate 1 \
        --max-time "$HOLD_SECONDS" \
        -H "Host: $HOST" \
        "http://$CADDY_HOST/" >/dev/null 2>&1 &
    HOLD_PIDS+=($!)
done

# Give the holds a moment to actually acquire their slots before
# firing the (N+1)th. 100ms is plenty — the `tenant_concurrent`
# handler is a buffered-channel send, microseconds.
sleep 0.2

# Fire the (N+1)th. Caddy's `tenant_concurrent` handler rejects
# synchronously with 429 + Retry-After: 1 because the bucket is
# full (the N hold curls are still draining).
OVER_CAP_STATUS_FILE=$(mktemp)
echo "[smoke] firing (cap+1)th request; expect 429 + Retry-After: 1"
OVER_CAP_HEADERS_FILE=$(mktemp)
curl -sS -D "$OVER_CAP_HEADERS_FILE" \
    -o /dev/null \
    -w "%{http_code}" \
    -k \
    --max-time 10 \
    -H "Host: $HOST" \
    "http://$CADDY_HOST/" > "$OVER_CAP_STATUS_FILE" 2>&1 || true

OVER_CAP_STATUS=$(cat "$OVER_CAP_STATUS_FILE")
if [[ "$OVER_CAP_STATUS" != "429" ]]; then
    kill "${HOLD_PIDS[@]}" 2>/dev/null || true
    rm -f "$OVER_CAP_STATUS_FILE" "$OVER_CAP_HEADERS_FILE"
    fail "(cap+1)th request got status '$OVER_CAP_STATUS', want 429"
fi
echo "[smoke] (cap+1)th request got 429 as expected"

# Verify the 429 also carried `Retry-After: 1` (Caddy's
# `tenant_concurrent` handler stamps the header explicitly at
# `main.go:180`).
if ! grep -iE '^retry-after:[[:space:]]*1' "$OVER_CAP_HEADERS_FILE" >/dev/null; then
    echo "----- response headers -----" >&2
    cat "$OVER_CAP_HEADERS_FILE" >&2
    echo "----------------------------" >&2
    kill "${HOLD_PIDS[@]}" 2>/dev/null || true
    rm -f "$OVER_CAP_STATUS_FILE" "$OVER_CAP_HEADERS_FILE"
    fail "429 response missing 'Retry-After: 1' header (issue #663 spec)"
fi
echo "[smoke] 429 carries Retry-After: 1"

# Release the held slots so the trap-on-EXIT cap reset and the
# worker can drain normally. Best-effort.
kill "${HOLD_PIDS[@]}" 2>/dev/null || true
wait "${HOLD_PIDS[@]}" 2>/dev/null || true
rm -f "$OVER_CAP_STATUS_FILE" "$OVER_CAP_HEADERS_FILE"

echo "PASS: $APP_NAME concurrent cap ($CONCURRENT_LIMIT in-flight → (cap+1)th gets 429 + Retry-After: 1)"
