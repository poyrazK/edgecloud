#!/usr/bin/env bash
# scripts/dev-clean.sh — tear down everything that scripts/dev-up.sh
# created and optionally wipe the artifact registry.
#
# Usage:
#   scripts/dev-clean.sh            # keep artifacts
#   scripts/dev-clean.sh --purge    # also wipe ~/.edgecloud/registry/
#   scripts/dev-clean.sh --all      # also wipe keys, env.sh, state
#
# Does NOT remove:
#   - ~/.edgecloud/registry/ (unless --purge)
#   - tenant rows in Postgres (operator can `make infra-reset` for that)
#
# Removes (unless --keep-state):
#   - running Caddy container (if any)
#   - docker compose stack (postgres + nats)
#   - ~/.edgecloud/env.sh, config.local.yaml, state/seed.json (if --all)

set -euo pipefail

PURGE=0
ALL=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --purge) PURGE=1; shift ;;
    --all)   ALL=1; shift ;;
    -h|--help)
      sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//' | head -n -1
      exit 0
      ;;
    *) echo "[dev-clean] unknown arg: $1" >&2; exit 2 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

log() { echo "[dev-clean] $*" >&2; }

# Stop Caddy if running.
if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -q '^edgecloud-caddy$'; then
  log "stopping caddy container"
  docker rm -f edgecloud-caddy >/dev/null 2>&1 || true
fi

# Stop postgres + nats (preserves named volume `edgecloud-pgdata` unless
# the user later runs `docker compose down -v` or `make infra-reset`).
if ( cd "$REPO_ROOT" && docker compose ps --quiet 2>/dev/null ) | grep -q .; then
  log "stopping postgres + nats containers"
  ( cd "$REPO_ROOT" && docker compose down )
fi

EDGECLOUD_HOME="${HOME}/.edgecloud"
REPO_LOCAL_CONFIG="$REPO_ROOT/edge-control-plane/config.local.yaml"

if [[ $ALL -eq 1 ]]; then
  log "wiping $EDGECLOUD_HOME/{env.sh,keys,state,caddy,cli-config,registry,tls}"
  rm -rf "$EDGECLOUD_HOME/env.sh" \
         "$EDGECLOUD_HOME/keys" \
         "$EDGECLOUD_HOME/state" \
         "$EDGECLOUD_HOME/caddy" \
         "$EDGECLOUD_HOME/cli-config" \
         "$EDGECLOUD_HOME/registry" \
         "$EDGECLOUD_HOME/tls"
  if [[ -f "$REPO_LOCAL_CONFIG" ]]; then
    rm -f "$REPO_LOCAL_CONFIG"
    log "removed $REPO_LOCAL_CONFIG"
  fi
  # Wipe CLI config too so the next `make dev` does a fresh signup.
  CLI_CONFIG="$HOME/Library/Application Support/edgecloud/config.toml"
  if [[ -f "$CLI_CONFIG" ]]; then
    rm -f "$CLI_CONFIG"
    log "removed $CLI_CONFIG"
  fi
elif [[ $PURGE -eq 1 ]]; then
  log "wiping $EDGECLOUD_HOME/registry (artifacts only)"
  rm -rf "$EDGECLOUD_HOME/registry"
  # A wipe of the registry invalidates existing deployments by hash, so
  # also reset the in-DB seed state so the next run redeploys from scratch.
  if [[ -f "$EDGECLOUD_HOME/state/seed.json" ]]; then
    rm -f "$EDGECLOUD_HOME/state/seed.json"
    log "removed $EDGECLOUD_HOME/state/seed.json"
  fi
fi

log "done. Next: 'make dev' to bring the stack back up."