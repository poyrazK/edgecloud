# scripts/lib/preflight.sh — sourced helper for scripts/dev-up.sh and
# scripts/dev-install.sh. NOT executable on its own; bash sources it.
#
# Responsibilities:
#   1. Verify macOS prereqs (docker, go, cargo, rustup, openssl, jq).
#      Print a fix-it message + exit 1 on missing prereq. WASI SDK is
#      auto-detected via brew prefix.
#   2. Idempotently create ~/.edgecloud/{registry,tls,keys,caddy,state}.
#   3. Mint or reuse secrets (jwt.secret, bootstrap.secret, internal.token,
#      signing.seed.hex). Reused on re-run so deploy signatures and JWT
#      tokens stay valid across restarts.
#   4. Mint a dummy TLS cert (only when missing) to satisfy
#      edge-ingress/src/config.rs::from_env line 157-158.
#   5. Write ~/.edgecloud/env.sh (overwritten each run) with every env
#      var the 4 services need, sourced by dev-up.sh Phase 9.
#   6. Write edge-control-plane/config.local.yaml with the values env
#      vars can't express (artifact_path, wasi_sdk_path, edge_migrate_path).
#
# Functions exported (callable after `source scripts/lib/preflight.sh`):
#   preflight::check_prereqs [--quiet]
#   preflight::create_dirs
#   preflight::mint_or_reuse_secret <name>
#   preflight::ensure_dummy_cert
#   preflight::detect_wasi_sdk
#   preflight::write_env_file
#   preflight::write_cp_local_config
#   preflight::all
#
# Globals set by preflight::all:
#   EDGECLOUD_HOME            — root state dir, e.g. ~/.edgecloud
#   EDGECLOUD_ENV_FILE        — path to the generated env.sh
#   EDGECLOUD_CP_LOCAL_CONFIG — path to the generated config.local.yaml
#   EDGECLOUD_SIGNING_SEED    — 64-char hex (Ed25519 seed)
#   EDGECLOUD_WASI_SDK_PATH   — detected path to wasi-sdk/bin
#   EDGECLOUD_WORKER_PORT     — assigned (8081)
#   EDGECLOUD_TENANT_NAME     — "dev"

set -o pipefail

# ── Internal helpers ──────────────────────────────────────────────────────

# All paths under $HOME so we never hit the macOS read-only /var.
preflight::_home() {
  echo "${HOME}/.edgecloud"
}

# Generate a 32-byte random hex secret. Writes to $1 (and prints to stdout).
# Used by mint_or_reuse_secret.
preflight::_rand_hex() {
  openssl rand -hex 32
}

# Print a single line prefixed with [preflight] to stderr.
preflight::_log() {
  echo "[preflight] $*" >&2
}

# ── Public functions ──────────────────────────────────────────────────────

# Verify the host has what we need. Exits 1 with a fix-it recipe on any
# missing prereq. Flags (any order):
#   --quiet          suppress non-error logs
#   --skip-docker    omit the `docker info` daemon check (for write-config-
#                    only paths that don't actually launch containers)
preflight::check_prereqs() {
  local quiet=0
  local skip_docker=0
  for arg in "$@"; do
    case "$arg" in
      --quiet) quiet=1 ;;
      --skip-docker) skip_docker=1 ;;
    esac
  done

  local missing=()
  for bin in docker go cargo rustc rustup openssl jq; do
    if ! command -v "$bin" >/dev/null 2>&1; then
      missing+=("$bin")
    fi
  done

  if [[ ${#missing[@]} -gt 0 ]]; then
    echo "[preflight] ERROR: missing required binaries: ${missing[*]}" >&2
    echo "[preflight] Install them with: make dev-install" >&2
    echo "[preflight] (brew install docker go rust rustup openssl jq wasi-sdk)" >&2
    return 1
  fi

  # Docker daemon must be reachable (Docker Desktop or compatible).
  if [[ $skip_docker -eq 0 ]]; then
    if ! docker info >/dev/null 2>&1; then
      echo "[preflight] ERROR: 'docker info' failed — is the Docker daemon running?" >&2
      echo "[preflight] Start Docker Desktop (or 'colima start' for Colima) and retry." >&2
      return 1
    fi
  fi

  # wasm32-wasip2 target — warn if missing but don't fail (Phase 10 sample
  # build will fail fast with a clear message if it's truly absent).
  # Required by samples/hello (Rust guest), edge-js-runtime (QuickJS host,
  # now directly on wasip2 — was previously on wasip1 + adapter wrap), and
  # any other Rust guest using the edge:cloud WIT world.
  if ! rustup target list --installed 2>/dev/null | grep -q '^wasm32-wasip2$'; then
    echo "[preflight] WARN: rustup target wasm32-wasip2 not installed." >&2
    echo "[preflight]       Install with: rustup target add wasm32-wasip2" >&2
    echo "[preflight]       The samples/hello build in Phase 10 will fail without it." >&2
  fi

  # Rust guest pipeline prereqs. `wasm-tools` is on PATH for the
  # `edge build --lang=rust` wrap step, `edge-migrate`, the worker
  # fixture build, and the Go control plane's `wrapAsComponent`. The
  # JS pipeline no longer needs it (wasip2 cargo emits a complete
  # component directly). Warn but don't fail the dev-up flow when
  # missing — Rust projects are the default; JS is opt-in.
  if ! command -v wasm-tools >/dev/null 2>&1; then
    echo "[preflight] WARN: wasm-tools not on PATH. Rust guest builds will fail." >&2
    echo "[preflight]       Install with: cargo install wasm-tools --locked --version '^1.252'" >&2
  fi

  [[ $quiet -eq 0 ]] && preflight::_log "prereqs OK"
  return 0
}

# Idempotent. Reused on every run.
preflight::create_dirs() {
  local home
  home="$(preflight::_home)"
  mkdir -p "$home"/{registry,tls,keys,caddy,state,cli-config}
  EDGECLOUD_HOME="$home"
  preflight::_log "state dir: $EDGECLOUD_HOME"
}

# Read a secret from disk, or generate + persist a new one. Reused on
# re-run so JWTs and signing tokens survive restarts.
# Usage: preflight::mint_or_reuse_secret <basename>
# Prints to stdout, persists to ~/.edgecloud/keys/<basename>.
preflight::mint_or_reuse_secret() {
  local name="$1"
  local path="${EDGECLOUD_HOME:-$(preflight::_home)}/keys/${name}"
  if [[ -f "$path" ]]; then
    cat "$path"
    return 0
  fi
  local val
  val="$(preflight::_rand_hex)"
  echo "$val" >"$path"
  chmod 600 "$path"
  echo "$val"
}

# Generate a self-signed wildcard cert to satisfy the ingress's TLS file
# requirement. Never served — Caddy is configured with no tls block.
# Idempotent.
preflight::ensure_dummy_cert() {
  local tls_dir="${EDGECLOUD_HOME:-$(preflight::_home)}/tls"
  local cert="$tls_dir/cert.pem"
  local key="$tls_dir/key.pem"
  if [[ -f "$cert" && -f "$key" ]]; then
    preflight::_log "dummy TLS cert exists: $cert"
    return 0
  fi
  preflight::_log "generating self-signed cert (dev-only, never served)"
  openssl req -x509 -newkey rsa:2048 \
    -keyout "$key" -out "$cert" \
    -days 365 -nodes \
    -subj "/CN=*.edgecloud.dev" \
    >/dev/null 2>&1
  chmod 600 "$key"
}

# Detect the WASI SDK bin path on macOS. Returns the dir containing clang.
# Tries brew prefix first (Apple Silicon → /opt/homebrew/opt, Intel →
# /usr/local/opt), then $HOME/wasi-sdk/bin. Prints the dir; empty if not
# found (callers should warn but continue — only needed for `edge migrate`
# which isn't part of the dev path).
preflight::detect_wasi_sdk() {
  local brew_prefix
  brew_prefix="$(brew --prefix wasi-sdk 2>/dev/null || true)"
  if [[ -n "$brew_prefix" && -x "$brew_prefix/bin/clang" ]]; then
    echo "$brew_prefix/bin"
    return 0
  fi
  if [[ -x "/opt/homebrew/opt/wasi-sdk/bin/clang" ]]; then
    echo "/opt/homebrew/opt/wasi-sdk/bin"
    return 0
  fi
  if [[ -x "/usr/local/opt/wasi-sdk/bin/clang" ]]; then
    echo "/usr/local/opt/wasi-sdk/bin"
    return 0
  fi
  if [[ -x "$HOME/wasi-sdk/bin/clang" ]]; then
    echo "$HOME/wasi-sdk/bin"
    return 0
  fi
  return 1
}

# Write ~/.edgecloud/env.sh. Overwritten each run — call AFTER secrets
# have been minted so we capture the (possibly reused) values.
preflight::write_env_file() {
  local env_file="${EDGECLOUD_HOME}/env.sh"
  local signing_seed="$1"   # 64-char hex
  local wasi_sdk="$2"       # path or empty

  local jwt_secret bootstrap_secret internal_token
  jwt_secret="$(preflight::mint_or_reuse_secret jwt.secret)"
  bootstrap_secret="$(preflight::mint_or_reuse_secret bootstrap.secret)"
  internal_token="$(preflight::mint_or_reuse_secret internal.token)"

  cat >"$env_file" <<EOF
# Generated by scripts/dev-up.sh — do not edit by hand.
# Re-running dev-up.sh will regenerate this file; secrets in
# ~/.edgecloud/keys/ are reused (only this file's exported paths and
# flags are rewritten).

# ── Auth ──────────────────────────────────────────────────────────────
export JWT_SECRET='${jwt_secret}'
export BOOTSTRAP_SECRET='${bootstrap_secret}'
export JWT_TTL_HOURS=24
export JWT_ISSUER='edgecloud-dev'
export JWT_ACTIVE_KID='v1'
export JWT_KEY_v1='${jwt_secret}'
export EDGE_INTERNAL_TOKEN='${internal_token}'

# ── Artifact signing (Ed25519, issue #307) ────────────────────────────
# CP signs new deployments with this seed; worker verifies with the
# matching public key (declared inline as a single-kid keyring).
export EDGE_SIGNING_KEY='${signing_seed}'
export EDGE_SIGNING_KEY_ID='v1'
export EDGE_SIGNING_KEYRING='{"v1":"${signing_seed}"}'
export EDGE_REQUIRE_SIGNATURE='true'

# ── Storage / registry ───────────────────────────────────────────────
# macOS-safe artifact path (NOT /var/edgecloud/registry which is read-only).
export STORAGE_ARTIFACT_PATH='${EDGECLOUD_HOME}/registry'
# Valid values are fs|s3|remote per edge-control-plane/internal/storage/factory.go.
# fs is the default; explicitly set for clarity.
export STORAGE_ARTIFACT_BACKEND='fs'

# ── Migration toolchain (issue #317 + edge-migrate) ───────────────────
# Only consulted on POST /api/v1/migrate{,-tree}. Safe to leave empty
# for the dev path (we deploy pre-built samples).
EOF
  if [[ -n "$wasi_sdk" ]]; then
    echo "export WASI_SDK_PATH='${wasi_sdk}'" >>"$env_file"
    echo "export RUSTC_PATH='rustc'" >>"$env_file"
  else
    echo "# WASI_SDK_PATH not detected — install via 'brew install wasi-sdk'" >>"$env_file"
  fi

  cat >>"$env_file" <<'EOF'

# ── NATS ──────────────────────────────────────────────────────────────
# Single-node NATS rejects replication-factor 3 stream creation
# (README troubleshooting); pin to 1 for local dev.
export TASK_STREAM_REPLICAS=1
export NATS_URL='nats://127.0.0.1:4222'

# ── Worker ────────────────────────────────────────────────────────────
export WORKER_ID='dev-worker-1'
export REGION='global'                   # MUST match CONTROL_PLANE_REGION
export CONTROL_PLANE_URL='http://127.0.0.1:8080'
export EDGE_WORKER_ADDR='127.0.0.1:8081'  # ingress reverse-proxies here
export WORKER_TENANT_ID='t_system'        # per README, system tenant

# ── Ingress ───────────────────────────────────────────────────────────
export INGRESS_REGION='global'
export CADDY_ADMIN_URL='http://127.0.0.1:2019'
export CADDY_ADMIN_LISTEN='0.0.0.0:2019'
export CADDY_ADMIN_TOKEN='dev-token'
export CONTROL_PLANE_API_URL='http://127.0.0.1:8080'
export HTTP_TO_HTTPS='false'              # dev: plain HTTP only
# TLS files exist (dummy cert) so the ingress's from_env check passes,
# but Caddy never serves them — no :443 listener, no tls block.
export TLS_CERT_FILE='${EDGECLOUD_HOME}/tls/cert.pem'
export TLS_KEY_FILE='${EDGECLOUD_HOME}/tls/key.pem'
EOF
  # Replace the literal ${EDGECLOUD_HOME} placeholders in single-quoted EOF
  # (bash doesn't expand inside single quotes — we use heredoc + sed).
  if [[ "$(uname)" == "Darwin" ]]; then
    sed -i '' "s|\${EDGECLOUD_HOME}|${EDGECLOUD_HOME}|g" "$env_file"
  else
    sed -i "s|\${EDGECLOUD_HOME}|${EDGECLOUD_HOME}|g" "$env_file"
  fi
  chmod 600 "$env_file"
  preflight::_log "wrote $env_file"
}

# Write edge-control-plane/config.local.yaml — only the values env vars
# can't express (artifact_path, wasi_sdk_path, edge_migrate_path). The
# Go loader's precedence is env > this file > config.yaml.
preflight::write_cp_local_config() {
  local repo_root="$1"           # absolute path to repo root
  local wasi_sdk="$2"            # path or empty
  local edge_migrate_path="$3"   # absolute path to edge-migrate binary or empty

  local cp_dir="$repo_root/edge-control-plane"
  local out="$cp_dir/config.local.yaml"

  cat >"$out" <<EOF
# Generated by scripts/dev-up.sh — overrides for macOS local dev.
# Do not edit by hand; rerun \`make dev\` to regenerate.
storage:
  artifact_path: ${EDGECLOUD_HOME}/registry
  artifact_backend: fs
migration:
  edge_migrate_path: ${edge_migrate_path:-edge-migrate}
EOF
  if [[ -n "$wasi_sdk" ]]; then
    cat >>"$out" <<EOF
  wasi_sdk_path: ${wasi_sdk}
  rustc_path: rustc
EOF
  fi
  preflight::_log "wrote $out"
}

# Run all of the above in order. Sets globals the caller can read.
# Usage: preflight::all <repo_root> [--skip-docker]
preflight::all() {
  local repo_root="$1"
  shift
  preflight::check_prereqs "$@" || return $?
  preflight::create_dirs
  preflight::ensure_dummy_cert

  local signing_seed wasi_sdk edge_migrate_path
  signing_seed="$(preflight::mint_or_reuse_secret signing.seed.hex)"
  EDGECLOUD_SIGNING_SEED="$signing_seed"
  preflight::_log "signing key kid=v1 (seed len=${#signing_seed})"

  if wasi_sdk="$(preflight::detect_wasi_sdk)"; then
    EDGECLOUD_WASI_SDK_PATH="$wasi_sdk"
    preflight::_log "WASI SDK: $wasi_sdk"
  else
    EDGECLOUD_WASI_SDK_PATH=""
    preflight::_log "WASI SDK not detected (optional for dev path)"
  fi

  # Resolve edge-migrate binary: prefer a built one in the workspace,
  # fall back to whatever's on PATH. Empty if neither — caller warns.
  if [[ -x "$repo_root/target/release/edge-migrate" ]]; then
    edge_migrate_path="$repo_root/target/release/edge-migrate"
  elif command -v edge-migrate >/dev/null 2>&1; then
    edge_migrate_path="$(command -v edge-migrate)"
  else
    edge_migrate_path=""
  fi

  preflight::write_env_file "$signing_seed" "$EDGECLOUD_WASI_SDK_PATH"
  preflight::write_cp_local_config "$repo_root" "$EDGECLOUD_WASI_SDK_PATH" "$edge_migrate_path"

  EDGECLOUD_ENV_FILE="${EDGECLOUD_HOME}/env.sh"
  EDGECLOUD_CP_LOCAL_CONFIG="$repo_root/edge-control-plane/config.local.yaml"
  EDGECLOUD_WORKER_PORT=8081
  EDGECLOUD_TENANT_NAME="dev"
}

# When invoked as a script (rather than sourced), print usage and exit.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  cat <<EOF
preflight.sh is sourced, not executed directly. Usage:

  source scripts/lib/preflight.sh
  preflight::all "\$(pwd)"

EOF
  exit 1
fi