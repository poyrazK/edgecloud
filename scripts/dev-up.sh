#!/usr/bin/env bash
# scripts/dev-up.sh — bring up the full edgeCloud dev stack in the
# foreground with prefixed log interleaving and Ctrl+C cleanup.
#
# Phases:
#   0  banner + signal trap
#   1  preflight (prereqs)
#   2-3 dirs, secrets, dummy cert
#   4-5 env.sh + config.local.yaml
#   6  bring up postgres + nats containers
#   7  run DB migrations
#   8  launch Caddy (in Docker, host networking)
#   9  launch CP + worker + ingress in foreground, prefixed
#   10 seed sample app (see scripts/lib/seed-sample.sh)
#   11 verify + hold
#
# Re-running this script is safe and idempotent: secrets, signing key,
# dummy cert, and Caddy container are reused/preserved; only env.sh and
# config.local.yaml are regenerated.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=lib/preflight.sh
source "$SCRIPT_DIR/lib/preflight.sh"

# ── Argument parsing ─────────────────────────────────────────────────────

CHECK_ONLY=0
WRITE_CONFIG_ONLY=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --check-only) CHECK_ONLY=1; shift ;;
    --write-config) WRITE_CONFIG_ONLY=1; shift ;;
    -h|--help)
      sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//' | head -n -1
      exit 0
      ;;
    *) echo "[dev-up] unknown arg: $1" >&2; exit 2 ;;
  esac
done

# ── Globals populated by preflight::all ──────────────────────────────────
EDGECLOUD_HOME="${HOME}/.edgecloud"
EDGECLOUD_ENV_FILE=""
EDGECLOUD_CP_LOCAL_CONFIG=""
EDGECLOUD_SIGNING_SEED=""
EDGECLOUD_WASI_SDK_PATH=""
EDGECLOUD_WORKER_PORT=8081
EDGECLOUD_TENANT_NAME="dev"

# ── Phase 0: banner + signal trap ───────────────────────────────────────

echo "[dev-up] edgeCloud dev stack (plain HTTP only — dev use)" >&2
echo "[dev-up] repo:    $REPO_ROOT" >&2
echo "[dev-up] state:   $EDGECLOUD_HOME" >&2
echo "[dev-up] Ctrl+C to stop; 'make dev-down' to stop containers; 'make dev-clean' to wipe artifacts." >&2

# PIDs of the foreground service processes, for targeted kill.
SERVICE_PIDS=()

cleanup() {
  local exit_code=$?
  # Disable re-entry on second Ctrl+C.
  trap '' INT TERM EXIT
  echo "" >&2
  echo "[dev-up] shutting down..." >&2
  # Kill the script's process group (CP, worker, ingress children).
  if [[ ${#SERVICE_PIDS[@]} -gt 0 ]]; then
    for pid in "${SERVICE_PIDS[@]}"; do
      kill "$pid" 2>/dev/null || true
    done
    sleep 0.5
    kill -KILL "${SERVICE_PIDS[@]}" 2>/dev/null || true
  fi
  # Stop Caddy container if we started one.
  if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -q '^edgecloud-caddy$'; then
    docker stop edgecloud-caddy >/dev/null 2>&1 || true
    docker rm edgecloud-caddy >/dev/null 2>&1 || true
  fi
  exit "$exit_code"
}
trap cleanup INT TERM EXIT

# ── Phase 1-5: preflight + dirs + secrets + env ─────────────────────────

if [[ $WRITE_CONFIG_ONLY -eq 1 ]]; then
  # Write-config-only: skip the Docker daemon check; just generate files.
  if ! preflight::all "$REPO_ROOT" --skip-docker; then
    echo "[dev-up] preflight failed; see messages above." >&2
    trap '' INT TERM EXIT
    exit 1
  fi
else
  if ! preflight::all "$REPO_ROOT"; then
    echo "[dev-up] preflight failed; see messages above." >&2
    trap '' INT TERM EXIT
    exit 1
  fi
fi

if [[ $CHECK_ONLY -eq 1 ]]; then
  echo "[dev-up] --check-only: prereqs OK; env written to $EDGECLOUD_ENV_FILE" >&2
  trap '' INT TERM EXIT
  exit 0
fi

if [[ $WRITE_CONFIG_ONLY -eq 1 ]]; then
  echo "[dev-up] --write-config: env written to $EDGECLOUD_ENV_FILE; cp config at $EDGECLOUD_CP_LOCAL_CONFIG" >&2
  trap '' INT TERM EXIT
  exit 0
fi

# Source the env file so all subsequent commands inherit the values.
# shellcheck source=/dev/null
source "$EDGECLOUD_ENV_FILE"

# ── Phase 6: bring up postgres + nats containers ────────────────────────

echo "[dev-up] starting postgres + nats..." >&2
( cd "$REPO_ROOT" && docker compose up -d postgres nats )

# Poll healthchecks. Total timeout 60s.
DEADLINE=$(( $(date +%s) + 60 ))
while [[ $(date +%s) -lt $DEADLINE ]]; do
  if ( cd "$REPO_ROOT" && docker compose ps --format json | jq -r '.[] | select(.Name=="edgecloud-postgres" or .Name=="edgecloud-nats") | .Health' 2>/dev/null ) | grep -q '^healthy$' && \
     ( cd "$REPO_ROOT" && docker compose ps --format json | jq -r '.[] | select(.Name=="edgecloud-postgres" or .Name=="edgecloud-nats") | .Health' 2>/dev/null ) | grep -vc '^healthy$' | grep -q '^0$'; then
    break
  fi
  sleep 2
done

POSTGRES_HEALTH=$(cd "$REPO_ROOT" && docker compose ps --format json | jq -r '.[] | select(.Name=="edgecloud-postgres") | .Health' 2>/dev/null || echo "unknown")
NATS_HEALTH=$(cd "$REPO_ROOT" && docker compose ps --format json | jq -r '.[] | select(.Name=="edgecloud-nats") | .Health' 2>/dev/null || echo "unknown")
if [[ "$POSTGRES_HEALTH" != "healthy" || "$NATS_HEALTH" != "healthy" ]]; then
  echo "[dev-up] ERROR: postgres=$POSTGRES_HEALTH nats=$NATS_HEALTH (timeout)" >&2
  ( cd "$REPO_ROOT" && docker compose logs --tail=50 ) >&2
  trap '' INT TERM EXIT
  exit 1
fi
echo "[dev-up] postgres + nats healthy" >&2

# ── Phase 7: run migrations ─────────────────────────────────────────────

echo "[dev-up] applying migrations..." >&2
( cd "$REPO_ROOT/edge-control-plane" && go run ./cmd/migrate --up )

# ── Phase 8: launch Caddy ───────────────────────────────────────────────

# Remove a stale container if one exists from a previous unclean exit.
if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -q '^edgecloud-caddy$'; then
  docker rm -f edgecloud-caddy >/dev/null 2>&1 || true
fi

echo "[dev-up] starting caddy..." >&2
docker run -d \
  --name edgecloud-caddy \
  --network host \
  -v "$SCRIPT_DIR/lib/caddy.json:/etc/caddy/caddy.json:ro" \
  -v "$EDGECLOUD_HOME/caddy/data:/data" \
  -v "$EDGECLOUD_HOME/caddy/config:/config" \
  caddy:2 \
  caddy run --config /etc/caddy/caddy.json --adapter "" \
  >/dev/null

# Wait for Caddy admin to respond.
DEADLINE=$(( $(date +%s) + 30 ))
until curl -fsS http://127.0.0.1:2019/config/ >/dev/null 2>&1; do
  if [[ $(date +%s) -ge $DEADLINE ]]; then
    echo "[dev-up] ERROR: caddy admin did not come up on :2019" >&2
    docker logs edgecloud-caddy --tail=50 >&2 || true
    trap '' INT TERM EXIT
    exit 1
  fi
  sleep 1
done
echo "[dev-up] caddy admin reachable on :2019" >&2

# ── Phase 9: launch CP + worker + ingress in foreground ────────────────

echo "[dev-up] starting control plane, worker, ingress (foreground; Ctrl+C to stop)..." >&2

# Launch a service in the background with each line of its stderr/stdout
# prefixed by [name]. Returns the PID of the `sed` (which exits when the
# service exits). The service child PID lives inside that pipeline.
run_foreground() {
  local name=$1; shift
  # We wrap in `bash -lc` so the env file is sourced fresh each launch
  # (cheap; ~10ms) and so $PATH includes anything Cargo/Rustup set up.
  ( bash -lc "$* 2>&1 | sed -u \"s/^/[${name}] /\"" ) &
  SERVICE_PIDS+=($!)
}

run_foreground api "cd '$REPO_ROOT/edge-control-plane' && source '$EDGECLOUD_ENV_FILE' && exec go run ./cmd/api -config '$EDGECLOUD_CP_LOCAL_CONFIG'"
run_foreground worker "cd '$REPO_ROOT' && source '$EDGECLOUD_ENV_FILE' && exec cargo run --quiet --release --bin edge-worker"
run_foreground ingress "cd '$REPO_ROOT' && source '$EDGECLOUD_ENV_FILE' && exec cargo run --quiet --release --bin edge-ingress"

# Wait for the CP health endpoint before proceeding to seed.
echo "[dev-up] waiting for control plane health on :8080..." >&2
DEADLINE=$(( $(date +%s) + 120 ))
until curl -fsS http://127.0.0.1:8080/health >/dev/null 2>&1; do
  if [[ $(date +%s) -ge $DEADLINE ]]; then
    echo "[dev-up] ERROR: control plane did not become healthy on :8080/health" >&2
    trap '' INT TERM EXIT
    exit 1
  fi
  sleep 2
done
echo "[dev-up] control plane healthy" >&2

# ── Phase 10: seed the samples/hello FaaS handler ───────────────────────

echo "[dev-up] seeding samples/hello (build CLI, signup tenant, deploy + activate)..." >&2
if ! SEED_FILE="$EDGECLOUD_HOME/state/seed.json" \
     "$SCRIPT_DIR/lib/seed-sample.sh"; then
  echo "[dev-up] WARN: sample seed failed; stack is up, you can retry manually." >&2
fi

# ── Phase 11: verify + hold ─────────────────────────────────────────────

echo "[dev-up] running verify checks..." >&2
SEED_FILE="$EDGECLOUD_HOME/state/seed.json" \
  "$SCRIPT_DIR/lib/verify.sh" || true   # don't exit on verify failure; user can read logs

echo "" >&2
echo "[dev-up] ──────────────────────────────────────────────────────────" >&2
echo "[dev-up] READY" >&2
if [[ -f "$EDGECLOUD_HOME/state/seed.json" ]]; then
  TENANT_ID="$(jq -r '.tenant_id' "$EDGECLOUD_HOME/state/seed.json")"
  WORKER_PORT="$(jq -r '.worker_port' "$EDGECLOUD_HOME/state/seed.json")"
  echo "[dev-up]   direct worker:  curl http://127.0.0.1:${WORKER_PORT}/hello" >&2
  echo "[dev-up]   via Caddy:      curl -H 'Host: ${TENANT_ID}-hello.edgecloud' http://127.0.0.1/hello" >&2
  echo "[dev-up]     (wait ~90s after 'READY' for first request — heartbeat + debounce + health-check warmup)" >&2
fi
echo "[dev-up]   Ctrl+C to stop; 'make dev-down' to stop containers; 'make dev-clean' to wipe artifacts." >&2
echo "[dev-up] ──────────────────────────────────────────────────────────" >&2

# Block on the foreground services; cleanup() runs via trap on exit.
wait