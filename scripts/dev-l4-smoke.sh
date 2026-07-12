#!/usr/bin/env bash
# scripts/dev-l4-smoke.sh — end-to-end smoke for issue #548 (L4 ingress)
#
# Walks the whole L4 path against a local docker-compose:
#   1. Build the xcaddy caddy-l4 image (or reuse it if present).
#   2. Bring caddy up against the plugin-enabled binary.
#   3. Build the samples/hello-tcp RESP echo-server (cargo +
#      wasm-tools wrap).
#   4. Deploy via the CLI; the CLI calls `POST /api/v1/apps/{appName}/l4-port`
#      on the CP, which atomically allocates a public port from
#      `L4_PORT_RANGE_START..=L4_PORT_RANGE_END` (default 31000..=31999).
#   5. Discover the public port via `GET /api/v1/apps/hello-tcp/l4-port`.
#   6. `nc localhost <port>` → assert the RESP echo replies `+PONG`.
#
# Pre-reqs: docker, the edge CLI on PATH (or built at
# `target/release/edge`), the control plane reachable at $EDGE_API_URL
# (default http://localhost:8080), and a NATS server the workers can
# reach. `scripts/dev-up.sh` stands all of that up; this script only
# exercises the L4 path on top.
#
# Usage:
#   EDGE_API_KEY=dev-key ./scripts/dev-l4-smoke.sh
#
# On success the script prints `PASS: hello-tcp reachable at
# tcp://localhost:<port>` and exits 0. On any failure it prints a
# `FAIL: <reason>` line and exits 1.

set -euo pipefail

# Resolve repo root from the script's location so the script can be
# run from any cwd.
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

# Configurable bits. EDGE_API_KEY and EDGE_API_URL override via env.
: "${EDGE_API_KEY:=dev-key}"
: "${EDGE_API_URL:=http://localhost:8080}"
: "${SAMPLE_APP:=hello-tcp}"
: "${SAMPLE_DIR:=$REPO_ROOT/samples/$SAMPLE_APP}"

CLI_BIN="${EDGE_BIN:-$REPO_ROOT/target/release/edge}"

fail() {
    echo "FAIL: $1" >&2
    exit 1
}

# 1. xcaddy image. Build once per machine; the `docker image inspect`
#    short-circuits subsequent runs the same way `scripts/dev-up.sh`
#    does.
if ! docker image inspect edgecloud/caddy-l4:latest >/dev/null 2>&1; then
    docker build \
        -t edgecloud/caddy-l4:latest \
        -f "$REPO_ROOT/edge-ingress/Dockerfile.caddy-l4" \
        "$REPO_ROOT" \
        || fail "xcaddy build (edgecloud/caddy-l4) failed"
fi

# 2. Caddy up against the plugin-enabled image. `docker compose up -d caddy`
#    is idempotent — if caddy is already running from scripts/dev-up.sh
#    (which uses the same `edgecloud/caddy-l4:latest` image), this is a
#    no-op.
( cd "$REPO_ROOT" && docker compose up -d caddy ) \
    || fail "docker compose up -d caddy"

# 3. Build samples/hello-tcp. The sample's `edge.toml` declares
#    `protocol = "tcp"`; the CLI's `validate_protocol_combo` enforces
#    `world = "edge-runtime"`.
( cd "$SAMPLE_DIR" && "$CLI_BIN" build ) \
    || fail "edge build failed for $SAMPLE_DIR"

# 4. Deploy. `edge deploy --manifest edge.toml` posts the artifact +
#    asks the CP to allocate the L4 port.
( cd "$SAMPLE_DIR" && \
  EDGE_API_KEY="$EDGE_API_KEY" EDGE_API_URL="$EDGE_API_URL" \
  "$CLI_BIN" deploy --manifest edge.toml ) \
    || fail "edge deploy --manifest failed"

# 5. Wait for the heartbeat pipeline to settle — the worker has to
#    start, mark the app Running, publish a heartbeat, and the ingress
#    has to render the L4 route. 30s is the heartbeat tick.
echo "[smoke] waiting 5s for heartbeat → render..."
sleep 5

# 6. Discover the public port.
public_port=$(curl -s \
    -H "Authorization: Bearer $EDGE_API_KEY" \
    "$EDGE_API_URL/api/v1/apps/$SAMPLE_APP/l4-port" \
    | jq -r .public_port 2>/dev/null || true)
[[ -z "$public_port" || "$public_port" == "null" ]] \
    && fail "GET /api/v1/apps/$SAMPLE_APP/l4-port returned no public_port (expected 4-digit number)"
[[ "$public_port" -lt 31000 || "$public_port" -gt 31999 ]] \
    && fail "public_port=$public_port outside L4 default range 31000..=31999"
echo "[smoke] public_port=$public_port"

# 7. Reach the app via raw TCP. `nc -w 2` closes after 2s of idle.
#    `printf` emits the literal RESP `PING\r\n`; `nc` strips the local
#    EOF after -w 2s elapses.
reply=$(printf 'PING\r\n' | nc -w 2 localhost "$public_port" || true)
expected="+PONG"
if [[ "$reply" != *"$expected"* ]]; then
    echo "FAIL: got '$reply' from tcp://localhost:$public_port, expected '$expected'" >&2
    exit 1
fi

echo "PASS: $SAMPLE_APP reachable at tcp://localhost:$public_port"
