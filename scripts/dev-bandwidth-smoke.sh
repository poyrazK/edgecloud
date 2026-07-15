#!/usr/bin/env bash
# scripts/dev-bandwidth-smoke.sh — end-to-end smoke for issue #664
# (per-tenant bandwidth cap on response payload, sub-feature #3 of
# #305).
#
# Walks the full bandwidth-cap path against a local dev stack
# (`scripts/dev-up.sh` must be running):
#   1. Build the xcaddy `edgecloud/caddy-concurrent` image (or reuse
#      it if present — the same build-if-missing pattern as
#      `scripts/dev-concurrent-smoke.sh`). The image now bundles
#      THREE plugins (L4 + tenant_concurrent + tenant_bandwidth); if
#      it already exists from a previous smoke run we still rebuild
#      so a stale image (pre-bandwidth) can't mask a regression.
#      Rebuild is a no-op when the source is unchanged (layer
#      caching).
#   2. Bring Caddy up against the plugin-enabled image
#      (idempotent — `docker compose up -d caddy` is a no-op if
#      already running).
#   3. Set `bandwidth_bps=2000` on the seeded dev tenant via
#      `PUT /api/v1/admin/tenants/{tenant_id}/rate-limit`. The
#      seeded key is owner-role (per `internal/service/tenant.go:261`
#      `mintAPIKey(..., RoleOwner)`) so the admin endpoint accepts
#      the write.
#   4. Wait for the ingress cache to refresh (default
#      `TENANT_RATE_LIMIT_FETCH_INTERVAL=30s`, mirrored by
#      `edge-ingress::config::tenant_rate_limit_fetch_interval`).
#      We poll Caddy's admin API for the `tenant-bandwidth:<id>`
#      route to appear so the test does not sleep for the full
#      default 30s when the operator has shortened the interval.
#   5. Fetch `?size=10000` against the seeded
#      `<tenant>-hello.edgecloud.dev` host. At 2000 B/s, a 10000-
#      byte body should take ~5 s of pacing (the first 2000 bytes
#      drain from the burst instantly, then the remaining 8000
#      pace at the cap rate). We assert `time_total ≥ 4 s` (CI
#      scheduler jitter margin) AND `size_download == 10000` (the
#      bytes must actually be delivered — pacing is response-side,
#      not a hard deny). Without the cap, the same request
#      completes in well under 100 ms; a regression that disables
#      the cap would fail the time-total assertion.
#   6. Reset the cap to 0 so a re-run starts from a clean slate.
#      Best-effort — a cleanup failure logs a warning but does not
#      fail the smoke (the test still proved the cap fired).
#
# Pre-reqs: docker, the edge CLI on PATH (or built at
# `target/release/edge`), the control plane reachable at
# $EDGE_API_URL (default http://localhost:8080), and a NATS server
# the workers can reach. `scripts/dev-up.sh` stands all of that up;
# this script only exercises the bandwidth-cap enforcement on top.
# The `samples/hello` artifact must support the `?size=N` query
# parameter (added in commit 7 of PR #664).
#
# Usage:
#   EDGE_API_KEY=dev-key ./scripts/dev-bandwidth-smoke.sh
#
# On success the script prints
# `PASS: <app> bandwidth cap (10KB body at 2KB/s → ~5s pacing)`
# and exits 0. On any failure it prints a `FAIL: <reason>` line
# and exits 1.

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
: "${BANDWIDTH_BPS:=2000}"    # 2 KB/s cap
: "${BODY_SIZE:=10000}"       # 10 KB body → ~5s pacing at 2 KB/s
: "${SAMPLE_APP:=hello}"
: "${ROUTE_WAIT_SECS:=35}"    # how long we wait for Caddy to receive the new route
: "${MIN_PACED_SECS:=4}"      # lower bound on time_total for the paced request

SEED_FILE="${EDGECLOUD_HOME:-$HOME/.edgecloud}/state/seed.json"

fail() {
    echo "FAIL: $1" >&2
    exit 1
}

# ── 1. Build the xcaddy image ─────────────────────────────────────────
# Mirror scripts/dev-concurrent-smoke.sh's build-if-missing pattern
# but always rebuild — a stale image from before the bandwidth
# module landed would not have the tenant_bandwidth plugin
# compiled in, and `caddy list-modules` smoke (CI commit 5) would
# fail on the rebuilt image but the stale local cache would still
# serve requests without the cap.
docker build \
    -t edgecloud/caddy-concurrent:latest \
    -f "$REPO_ROOT/edge-ingress/Dockerfile.caddy-concurrent" \
    "$REPO_ROOT" \
    || fail "xcaddy build (edgecloud/caddy-concurrent) failed"

# ── 2. Bring Caddy up against the plugin-enabled image ────────────────
# `scripts/dev-up.sh` already runs Caddy with this image. `docker
# compose up -d caddy` is a no-op if the container is already
# running with the right image, and a fresh start if the operator
# restarted `dev-up.sh` after a host reboot. The container restart
# picks up the freshly-built image.
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
# placeholder `dev-key`, not the real one).
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

# ── 4. Set bandwidth_bps via the owner-only admin endpoint ───────────
# The renderer's `bandwidth_caps()` iterator filters on
# `bandwidth_bps > 0`, so a 0-value clears the cap and a positive
# value enables it. We pin the cap to $BANDWIDTH_BPS (default
# 2000 = 2 KB/s) for the duration of the test.
echo "[smoke] setting $TENANT_ID bandwidth_bps=$BANDWIDTH_BPS"
curl -fsS -X PUT \
    -H "Authorization: Bearer $EDGE_API_KEY" \
    -H "Content-Type: application/json" \
    -d "{\"bandwidth_bps\": $BANDWIDTH_BPS}" \
    "$EDGE_API_URL/api/v1/admin/tenants/$TENANT_ID/rate-limit" \
    >/dev/null \
    || fail "PUT rate-limit returned non-2xx"

# Cleanup on exit — best-effort reset to bandwidth_bps=0 so a
# re-run starts clean. Best-effort; failure logs but does not
# fail the test.
reset_cap() {
    curl -fsS -X PUT \
        -H "Authorization: Bearer $EDGE_API_KEY" \
        -H "Content-Type: application/json" \
        -d '{"bandwidth_bps": 0}' \
        "$EDGE_API_URL/api/v1/admin/tenants/$TENANT_ID/rate-limit" \
        >/dev/null 2>&1 \
        || echo "[smoke] WARN: failed to reset bandwidth_bps=0" >&2
}
trap reset_cap EXIT

# ── 5. Wait for Caddy to receive the new tenant_bandwidth route ──────
# The ingress polls the CP every
# `TENANT_RATE_LIMIT_FETCH_INTERVAL` (default 30s, see
# `edge-ingress/src/config.rs::tenant_rate_limit_fetch_interval`),
# then re-renders and POSTs `/load` to Caddy. We poll Caddy's admin
# API for the `tenant-bandwidth:<tenant_id>` route to appear so the
# test does not sleep for the full default 30s when the operator
# has shortened the interval.
ROUTE_ID="tenant-bandwidth:$TENANT_ID"
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

# ── 6. Fetch a 10 KB body and assert pacing fires ────────────────────
# The `<tenant>-<app>.edgecloud.dev` host is what Caddy's
# `host_regexp` matches. We pass the host via `-H Host: ...` and
# connect to 127.0.0.1 over plain HTTP (`scripts/dev-up.sh` runs
# Caddy in plain-HTTP mode for dev; production uses TLS via the
# `tls.on_demand: {}` block). The dev cert is self-signed — we use
# `-k` to skip the verify.
#
# At $BANDWIDTH_BPS=2000 (2 KB/s) and $BODY_SIZE=10000 (10 KB), the
# request should take ~5 s of pacing (first 2000 bytes drain from
# the burst instantly, then the remaining 8000 pace at the rate).
# We assert time_total ≥ $MIN_PACED_SECS (4 s gives CI scheduler
# jitter a comfortable margin — a slow runner finishes at ~5.5 s,
# a fast one at ~4.5 s; either passes) AND size_download == 10000
# (the bytes must actually be delivered — pacing is response-side,
# not a hard deny). Without the cap, the same request completes in
# well under 100 ms; a regression that disables the cap would fail
# the time-total assertion.
#
# We use `--max-time 30` so a hung request doesn't block the
# script forever; a stuck request would time out and curl would
# report `time_total = 30.000` which would also satisfy the lower
# bound but fail the size-download check, so the size assertion
# catches the hung case too.
HOST="${TENANT_ID}-${APP_NAME}.edgecloud.dev"
SIZE_OUTPUT=$(mktemp)
echo "[smoke] fetching $HOST/?size=$BODY_SIZE at cap=$BANDWIDTH_BPS B/s; expect ~5s pacing"

CURL_START=$(date +%s.%N)
curl -sS -o /dev/null \
    -w 'time_total=%{time_total} size_download=%{size_download}\n' \
    -k \
    --max-time 30 \
    -H "Host: $HOST" \
    "http://$CADDY_HOST/?size=$BODY_SIZE" \
    | tee "$SIZE_OUTPUT"

TIME_TOTAL=$(awk -F'[= ]' '/time_total/ {print $2}' "$SIZE_OUTPUT")
SIZE_DOWNLOAD=$(awk -F'[= ]' '/size_download/ {print $4}' "$SIZE_OUTPUT")
rm -f "$SIZE_OUTPUT"

# Compare via awk for fractional seconds (bash arithmetic can't
# handle 4.875 reliably).
if ! awk -v t="$TIME_TOTAL" -v min="$MIN_PACED_SECS" 'BEGIN { exit !(t+0 >= min+0) }'; then
    fail "time_total=$TIME_TOTAL, expected >= $MIN_PACED_SECS (cap not pacing or tenant_bandwidth plugin missing from Caddy)"
fi
echo "[smoke] time_total=$TIME_TOTAL (>= $MIN_PACED_SECS confirms pacing fired)"

if [[ "$SIZE_DOWNLOAD" != "$BODY_SIZE" ]]; then
    fail "size_download=$SIZE_DOWNLOAD, expected $BODY_SIZE (the body must be fully delivered — pacing is response-side, not a hard deny)"
fi
echo "[smoke] size_download=$SIZE_DOWNLOAD (full body delivered — pacing is response-side, not deny)"

echo "PASS: $APP_NAME bandwidth cap (${BODY_SIZE}B body at ${BANDWIDTH_BPS}B/s → ${TIME_TOTAL}s pacing, full body delivered)"
