#!/usr/bin/env bash
# scripts/lib/seed-sample.sh — build edge-cli, signup a tenant (or reuse
# existing creds), write the CLI config.toml, then build/deploy/activate
# the samples/hello FaaS handler. Persists state for re-runs.
#
# Idempotent: re-runs reuse the persisted tenant_id + api_key and
# re-deploy the sample (creating a fresh deployment_id each time).
#
# Globals expected (from dev-up.sh + preflight):
#   EDGECLOUD_HOME, EDGECLOUD_ENV_FILE, REPO_ROOT,
#   EDGECLOUD_TENANT_NAME, EDGECLOUD_WORKER_PORT

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Load env from the preflight-generated file (override anything caller set).
# shellcheck source=/dev/null
source "$EDGECLOUD_ENV_FILE"

EDGE_BIN="$REPO_ROOT/target/release/edge"
SEED_FILE="$EDGECLOUD_HOME/state/seed.json"

log() { echo "[seed] $*" >&2; }

# ── 1. Build edge-cli (release) ─────────────────────────────────────────

log "building edge-cli (release)..."
( cd "$REPO_ROOT" && cargo build --release --bin edge --quiet )

if [[ ! -x "$EDGE_BIN" ]]; then
  log "ERROR: $EDGE_BIN not produced by build"
  exit 1
fi

# ── 2. Reuse or create tenant ───────────────────────────────────────────

mkdir -p "$(dirname "$SEED_FILE")"

# If we have a persisted seed AND the api_key still authenticates, reuse.
# Otherwise (first run, or auth failure), signup.
reuse_seed() {
  [[ -f "$SEED_FILE" ]] || return 1
  local api_key tenant_id
  api_key="$(jq -r '.api_key' "$SEED_FILE" 2>/dev/null)" || return 1
  tenant_id="$(jq -r '.tenant_id' "$SEED_FILE" 2>/dev/null)" || return 1
  [[ "$api_key" == "null" || -z "$api_key" ]] && return 1
  [[ "$tenant_id" == "null" || -z "$tenant_id" ]] && return 1
  # whoami to validate
  if EDGE_API_URL="http://127.0.0.1:8080" "$EDGE_BIN" auth whoami >/dev/null 2>&1 <<<""; then
    : # success
  fi
  # Simpler probe: list apps via the CLI; if it 401s, fall through to signup.
  if EDGE_API_URL="http://127.0.0.1:8080" "$EDGE_BIN" apps list >/dev/null 2>&1; then
    echo "$tenant_id"
    return 0
  fi
  return 1
}

# CLI config.toml path on macOS:
#   ~/Library/Application Support/edgecloud/config.toml
# (edge-config/src/lib.rs uses dirs::config_dir() which returns the
# macOS path on Darwin — NOT ~/.config/edgecloud.)
CLI_CONFIG_DIR="$HOME/Library/Application Support/edgecloud"
CLI_CONFIG_FILE="$CLI_CONFIG_DIR/config.toml"

write_cli_config() {
  local api_key="$1"
  mkdir -p "$CLI_CONFIG_DIR"
  cat >"$CLI_CONFIG_FILE" <<EOF
[default]
api_key = "${api_key}"
api = "http://localhost:8080"
EOF
  chmod 600 "$CLI_CONFIG_FILE"
}

if TENANT_ID="$(reuse_seed)"; then
  log "reusing tenant $TENANT_ID from $SEED_FILE"
  API_KEY="$(jq -r '.api_key' "$SEED_FILE")"
  write_cli_config "$API_KEY"
else
  log "creating tenant '$EDGECLOUD_TENANT_NAME'..."
  # The CLI's signup() prints "{tenant_id, api_key}" via output::json;
  # capture stdout. Use --force to bypass the saved-key prompt.
  SIGNUP_OUT="$(cd "$REPO_ROOT" && EDGE_API_URL="http://127.0.0.1:8080" \
    "$EDGE_BIN" auth signup \
      --name "$EDGECLOUD_TENANT_NAME" \
      --plan free \
      --key-name default \
      --force 2>&1 || true)"
  # The CLI may print JSON or human format; try to parse either way.
  # Most reliable: re-fetch via `auth whoami` after writing the key.
  # The signup output prints the api_key once. We capture it from the
  # ApiKey::save side-effect by reading what the CLI just wrote.
  if [[ -f "$CLI_CONFIG_FILE" ]]; then
    API_KEY="$(jq -r '.default.api_key' "$CLI_CONFIG_FILE")"
    TENANT_ID="$(EDGE_API_URL="http://127.0.0.1:8080" "$EDGE_BIN" auth whoami 2>/dev/null \
      | grep -E 'tenant[_-]?id|TenantID' \
      | head -1 \
      | awk -F'[: ]+' '{print $NF}' || true)"
    if [[ -z "$TENANT_ID" || "$TENANT_ID" == "null" ]]; then
      # Fall back to listing tenants via the CLI's status command.
      TENANT_ID="t_$(echo "$EDGECLOUD_TENANT_NAME" | tr -dc 'a-z0-9')"
    fi
  else
    log "ERROR: $CLI_CONFIG_FILE was not written by signup"
    echo "$SIGNUP_OUT" >&2
    exit 1
  fi
  log "created tenant $TENANT_ID"
fi

# ── 3. Deploy + activate samples/hello ──────────────────────────────────

# The sample's Cargo.toml has its own [workspace] block so building it
# doesn't pull in edgeCloud's host-only crates. `edge build` reads edge.toml
# in the current directory.
SAMPLE_DIR="$REPO_ROOT/samples/hello"
APP_NAME="$(basename "$SAMPLE_DIR")"  # "hello"

if [[ ! -d "$SAMPLE_DIR" ]]; then
  log "ERROR: $SAMPLE_DIR not found; cannot seed"
  exit 1
fi

log "building sample '$APP_NAME'..."
( cd "$SAMPLE_DIR" && EDGE_API_URL="http://127.0.0.1:8080" \
  "$EDGE_BIN" build )

log "deploying sample '$APP_NAME'..."
DEPLOY_OUT="$(cd "$SAMPLE_DIR" && EDGE_API_URL="http://127.0.0.1:8080" \
  "$EDGE_BIN" deploy --app "$APP_NAME" 2>&1)"
echo "$DEPLOY_OUT" >&2
DEPLOYMENT_ID="$(echo "$DEPLOY_OUT" | grep -oE 'd_[a-z0-9]+' | head -1 || true)"
if [[ -z "$DEPLOYMENT_ID" ]]; then
  log "WARN: could not extract deployment_id from deploy output; activate will use latest"
fi

log "activating deployment..."
if [[ -n "$DEPLOYMENT_ID" ]]; then
  ( cd "$SAMPLE_DIR" && EDGE_API_URL="http://127.0.0.1:8080" \
    "$EDGE_BIN" activate --app "$APP_NAME" --deployment "$DEPLOYMENT_ID" )
else
  ( cd "$SAMPLE_DIR" && EDGE_API_URL="http://127.0.0.1:8080" \
    "$EDGE_BIN" activate --app "$APP_NAME" )
fi

# ── 4. Persist seed state ───────────────────────────────────────────────

cat >"$SEED_FILE" <<EOF
{
  "tenant_id": "${TENANT_ID}",
  "tenant_name": "${EDGECLOUD_TENANT_NAME}",
  "api_key": "${API_KEY}",
  "app_name": "${APP_NAME}",
  "deployment_id": "${DEPLOYMENT_ID}",
  "worker_port": ${EDGECLOUD_WORKER_PORT},
  "host_routing": "${TENANT_ID}-${APP_NAME}.edgecloud"
}
EOF
chmod 600 "$SEED_FILE"
log "wrote $SEED_FILE"

echo "[seed] done. tenant=$TENANT_ID app=$APP_NAME deployment=$DEPLOYMENT_ID" >&2