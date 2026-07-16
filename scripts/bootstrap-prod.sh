#!/usr/bin/env bash
# scripts/bootstrap-prod.sh — one-command bootstrap of the production-
# equivalent edgeCloud stack (issue #512 closeout).
#
# What this does (in order):
#   1. Preflight: docker, openssl, jq, cargo + rustup wasm32-wasip2 target,
#      and a reachable `docker compose`.
#   2. Generate the Ed25519 signing keyring file (mode 0600).
#   3. Generate a SAN-complete self-signed TLS cert (covers
#      *.edgecloud.dev, edgecloud.dev, localhost, 127.0.0.1).
#   4. Render docker-compose.prod/caddy.local.json + secrets/signing-keyring.
#   5. Take any operator-overridden placeholders in .env.prod, fill the
#      rest from the freshly-generated secrets, and write them back.
#   6. Bring up the stack via `make prod-up`.
#   7. Poll /ready on the control plane (deep readiness per issue #48).
#   8. Build edge-cli on the host (release).
#   9. `edge auth signup` to create the first tenant + api_key.
#  10. `edge build && edge deploy && edge activate` against samples/hello.
#  11. Poll Caddy's admin API until a route for the deployed hostname
#      appears.
#  12. `curl -H "Host: t_<id>-hello.edgecloud.dev" http://localhost:80/hello`
#      and assert HTTP 200 — the canonical prod-up smoke.
#  13. Persist state/seed.json (mode 0600) so re-runs skip signup.
#
# Idempotent: re-running skips steps 2-5 if the files already exist,
# reuses the persisted api_key + tenant_id, but always redeploys
# samples/hello so the smoke check is current.
#
# Operator requirement: `cp .env.prod.example .env.prod` first, but
# every placeholder can stay — this script fills in real secrets where
# allowed (only the operator knows if the database password is OK to
# regenerate; if a real one is set, we leave it alone).
#
# Exits non-zero on any failure, with a precise `bootstrap: <step>`
# log prefix so operators can diagnose without re-running with -x.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Defaults (operator can override via env before invocation) ─────────
TENANT_NAME="${EDGECLOUD_PROD_TENANT_NAME:-prod-bootstrap}"
APP_NAME="hello"
HOST_FQDN="${EDGECLOUD_PROD_HOST:-edgecloud.dev}"

# Per-step budgets. `/ready` and the deploy propagation can take ~30s
# each on a cold cluster; the 600s outer timeout is the cap the Makefile
# passes as `timeout` too.
WAIT_READY_SECS="${EDGECLOUD_PROD_READY_TIMEOUT:-120}"
WAIT_CADDY_ROUTE_SECS="${EDGECLOUD_PROD_CADDY_TIMEOUT:-120}"
SMOKE_TIMEOUT_SECS="${EDGECLOUD_PROD_SMOKE_TIMEOUT:-60}"

log() { echo "[bootstrap] $*" >&2; }
die() { echo "[bootstrap] FATAL: $*" >&2; exit 1; }
step() { echo ""; echo "▶ $*" >&2; }

# ── 1. Preflight ────────────────────────────────────────────────────────
step "1. preflight: docker, compose, openssl, jq, cargo + wasm32-wasip2"

command -v docker >/dev/null 2>&1 \
  || die "docker not found; install Docker Engine (https://docs.docker.com/engine/install/)"
docker info >/dev/null 2>&1 \
  || die "docker daemon not reachable; start Docker Desktop or systemd socket"
docker compose version >/dev/null 2>&1 \
  || die "docker compose v2 not available; install docker-compose-plugin"
command -v openssl >/dev/null 2>&1 \
  || die "openssl not found; install via package manager"
openssl version | grep -qE 'OpenSSL [13]\.' \
  || log "WARN: openssl version not 1.x/3.x — proceeding but cert generation may fail"
command -v jq >/dev/null 2>&1 \
  || die "jq not found; install via package manager (brew install jq / apt-get install jq)"
command -v cargo >/dev/null 2>&1 \
  || die "cargo not found; install rustup + stable toolchain"
rustup target list --installed 2>/dev/null | grep -q wasm32-wasip2 \
  || die "wasm32-wasip2 target missing; run:  rustup target add wasm32-wasip2"

[[ -d "$REPO_ROOT/samples/hello" ]] \
  || die "samples/hello not found at repo root; bootstrap targets this path"
[[ -f "$REPO_ROOT/docker-compose.prod.yml" ]] \
  || die "docker-compose.prod.yml not found at repo root"
[[ -f "$REPO_ROOT/docker-compose.prod/caddy.json" ]] \
  || die "docker-compose.prod/caddy.json not found"

# ── 2-5. Secrets scaffold ──────────────────────────────────────────────

SECRETS_DIR="$REPO_ROOT/secrets"
KEYRING_FILE="$SECRETS_DIR/signing-keyring"
TLS_DIR="$REPO_ROOT/tls"
CERT_FILE="$TLS_DIR/cert.pem"
KEY_FILE="$TLS_DIR/key.pem"
ENV_FILE="$REPO_ROOT/.env.prod"
ENV_EXAMPLE="$REPO_ROOT/.env.prod.example"
CADDY_LOCAL_JSON="$REPO_ROOT/docker-compose.prod/caddy.local.json"
STATE_DIR="$REPO_ROOT/state"
SEED_FILE="$STATE_DIR/seed.json"

# Walk `.env.prod` truth table — the operator may have set any of these.
# We only fill in missing ones with freshly-generated secrets.
env_get() {                       # $1=key, default to stdout, empty if unset
  [[ -f "$ENV_FILE" ]] || return 0
  local v
  v="$(grep -E "^${1}=" "$ENV_FILE" 2>/dev/null | head -1 | cut -d= -f2- || true)"
  printf '%s' "$v"
}
env_has_placeholder() {           # $1=key
  local v; v="$(env_get "$1")"
  [[ -z "$v" || "$v" == replace-me-* ]]
}
env_set() {                       # $1=key $2=value (append or update)
  if grep -qE "^${1}=" "$ENV_FILE" 2>/dev/null; then
    # macOS sed requires -i ''; Linux sed -i. Use a tmp+rename portable
    # approach that works on both. awk is simpler and matches the
    # smoke-job redaction pattern (scripts/lib).
    awk -v k="$1" -v v="$2" '
      $0 ~ "^"k"=" { print k"="v; next }
      { print }
    ' "$ENV_FILE" > "$ENV_FILE.new" && mv "$ENV_FILE.new" "$ENV_FILE"
  else
    printf '%s=%s\n' "$1" "$2" >> "$ENV_FILE"
  fi
}

step "2. generating Ed25519 signing keyring (if missing)"
mkdir -p "$SECRETS_DIR" && chmod 0700 "$SECRETS_DIR"
if [[ ! -s "$KEYRING_FILE" ]]; then
  KID="k1"
  SEED="$(openssl rand -hex 32)"
  printf '%s = %s\n' "$KID" "$SEED" > "$KEYRING_FILE"
  chmod 0600 "$KEYRING_FILE"
  log "wrote $KEYRING_FILE (kid=$KID, mode 0600)"
else
  log "reusing existing $KEYRING_FILE ($(wc -c < "$KEYRING_FILE" | tr -d ' ') bytes)"
fi

step "3. generating self-signed TLS cert (if missing)"
mkdir -p "$TLS_DIR"
if [[ ! -s "$CERT_FILE" || ! -s "$KEY_FILE" ]]; then
  # SAN must include the wildcard for *.edgecloud.dev (the ingress
  # terminates TLS for <tenant>-<app>.edgecloud.dev) plus localhost +
  # IP literal so curl --resolve or smoke runs without DNS survive.
  openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
    -keyout "$KEY_FILE" \
    -out    "$CERT_FILE" \
    -subj "/CN=$HOST_FQDN" \
    -addext "subjectAltName=DNS:$HOST_FQDN,DNS:*.$HOST_FQDN,DNS:localhost,IP:127.0.0.1" \
    >/dev/null 2>&1 \
    || die "openssl self-signed cert generation failed"
  chmod 0644 "$CERT_FILE" && chmod 0600 "$KEY_FILE"
  log "wrote $CERT_FILE + $KEY_FILE (SAN: DNS:$HOST_FQDN,DNS:*.$HOST_FQDN,DNS:localhost,IP:127.0.0.1)"
else
  log "reusing existing $CERT_FILE"
fi

step "4. ensuring .env.prod exists + filling placeholder secrets"
if [[ ! -f "$ENV_FILE" ]]; then
  [[ -f "$ENV_EXAMPLE" ]] || die ".env.prod.example missing at $ENV_EXAMPLE"
  cp "$ENV_EXAMPLE" "$ENV_FILE"
  log "copied $ENV_EXAMPLE → $ENV_FILE"
fi

# DATABASE_PASSWORD: always overwrite the placeholder if it's still the
# example value, because validateDBPassword (issue #626) enforces a
# 16-byte floor and the example literally says "replace-me-...".
if env_has_placeholder DATABASE_PASSWORD; then
  env_set DATABASE_PASSWORD "$(openssl rand -base64 24)"
  log "generated DATABASE_PASSWORD (24 bytes, ≥16B floor)"
fi
if env_has_placeholder POSTGRES_PASSWORD; then
  env_set POSTGRES_PASSWORD "$(env_get DATABASE_PASSWORD)"
  log "mirrored POSTGRES_PASSWORD from DATABASE_PASSWORD"
fi

# JWT_SECRET: 48-byte base64 = 64 chars; well above the 32-byte recommendation.
if env_has_placeholder JWT_SECRET; then
  env_set JWT_SECRET "$(openssl rand -base64 48)"
  log "generated JWT_SECRET (48 bytes)"
fi

# EDGE_INTERNAL_TOKEN, BOOTSTRAP_SECRET: 32 hex = 64 chars.
if env_has_placeholder EDGE_INTERNAL_TOKEN; then
  env_set EDGE_INTERNAL_TOKEN "$(openssl rand -hex 32)"
  log "generated EDGE_INTERNAL_TOKEN (64 hex chars)"
fi
if env_has_placeholder BOOTSTRAP_SECRET; then
  env_set BOOTSTRAP_SECRET "$(openssl rand -hex 32)"
  log "generated BOOTSTRAP_SECRET (64 hex chars)"
fi

# METRICS_AUTH_TOKEN + CADDY_ADMIN_TOKEN: 32 hex each.
if env_has_placeholder METRICS_AUTH_TOKEN; then
  env_set METRICS_AUTH_TOKEN "$(openssl rand -hex 32)"
  log "generated METRICS_AUTH_TOKEN (64 hex chars)"
fi
if env_has_placeholder CADDY_ADMIN_TOKEN; then
  env_set CADDY_ADMIN_TOKEN "$(openssl rand -hex 32)"
  log "generated CADDY_ADMIN_TOKEN (64 hex chars)"
fi

# EDGE_SIGNING_KEYRING + EDGE_SIGNING_KEY_ID mirror the file we just
# wrote so the runtime reads from disk (the file is mounted into the
# worker container per docker-compose.prod.yml:221).
if env_has_placeholder EDGE_SIGNING_KEY_ID; then
  env_set EDGE_SIGNING_KEY_ID "k1"
fi
if env_has_placeholder EDGE_SIGNING_KEYRING; then
  env_set EDGE_SIGNING_KEYRING_PATH "/run/secrets/signing-keyring"
  env_set EDGE_SIGNING_KEYRING ""   # clear inline form; PATH-based read takes priority
fi
# Re-stamp EDGE_WORKER_ADDR to be the compose-internal service name
# (issue #641). The literal hostname in the .example is misleading in
# a single-host compose; the ingress appends per-app port.
if grep -qE '^EDGE_WORKER_ADDR=.*\.example\.com' "$ENV_FILE" 2>/dev/null; then
  env_set EDGE_WORKER_ADDR "worker"
  log "reset EDGE_WORKER_ADDR to compose-internal 'worker' (no port; ingress appends)"
fi

# Ensure tls paths line up with what we generated above.
env_set TLS_CERT_FILE "/etc/caddy/tls/cert.pem"
env_set TLS_KEY_FILE  "/etc/caddy/tls/key.pem"

# `make prod-secrets` envsubst's caddy.json using .env.prod.
# Render now so `make prod-up` doesn't have to.
( cd "$REPO_ROOT" && set -a; . "$ENV_FILE"; set +a; \
  envsubst < docker-compose.prod/caddy.json > "$CADDY_LOCAL_JSON" ) \
  || die "envsubst of docker-compose.prod/caddy.json failed"
log "rendered $CADDY_LOCAL_JSON"

# ── 6. Bring up the stack ──────────────────────────────────────────────
step "6. bring up the stack (make prod-up)"
( cd "$REPO_ROOT" && make prod-up )

# ── 7. /ready ──────────────────────────────────────────────────────────
step "7. waiting for /ready on the control plane (≤ ${WAIT_READY_SECS}s)"
deadline=$(( $(date +%s) + WAIT_READY_SECS ))
while (( $(date +%s) < deadline )); do
  if curl -fsS http://127.0.0.1:8080/ready 2>/dev/null \
      | jq -e '.status == "ok" or .status == "degraded"' >/dev/null 2>&1; then
    log "/ready is green"
    break
  fi
  sleep 2
done
(( $(date +%s) < deadline )) || die "/ready never became ready within ${WAIT_READY_SECS}s"

# ── 8. Build edge-cli ──────────────────────────────────────────────────
step "8. build edge-cli on host (release)"
EDGE_BIN="$REPO_ROOT/target/release/edge"
( cd "$REPO_ROOT" && cargo build --release --bin edge --quiet )
[[ -x "$EDGE_BIN" ]] || die "edge-cli build did not produce $EDGE_BIN"
log "edge-cli at $EDGE_BIN"

# ── 9. Signup (or reuse persisted creds) ───────────────────────────────
step "9. tenant signup (or reuse)"
EDGE_API_URL="http://127.0.0.1:8080" export EDGE_API_URL
mkdir -p "$STATE_DIR" && chmod 0700 "$STATE_DIR"
CLI_CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/edgecloud"
CLI_CONFIG_FILE="$CLI_CONFIG_DIR/config.toml"

# Decide tenant reuse vs create based on persisted seed.
TENANT_ID=""; API_KEY=""; DEPLOYMENT_ID=""
if [[ -s "$SEED_FILE" ]] \
   && command -v jq >/dev/null \
   && API_KEY="$(jq -r '.api_key // empty' "$SEED_FILE" 2>/dev/null)" \
   && [[ -n "$API_KEY" && "$API_KEY" != "null" ]] \
   && TENANT_ID="$(jq -r '.tenant_id // empty' "$SEED_FILE" 2>/dev/null)"; then
  # Probe the persisted creds: if whoami works, reuse.
  if EDGE_API_KEY="$API_KEY" "$EDGE_BIN" auth whoami >/dev/null 2>&1; then
    log "reusing persisted tenant $TENANT_ID"
  else
    log "persisted creds rejected by CP — falling back to fresh signup"
    TENANT_ID=""; API_KEY=""
  fi
fi

if [[ -z "$API_KEY" ]]; then
  # Fresh signup. Remove any stale config so --force is silent.
  rm -f "$CLI_CONFIG_FILE"
  SIGNUP_OUT="$( cd "$REPO_ROOT" && EDGE_API_URL="http://127.0.0.1:8080" \
                   "$EDGE_BIN" auth signup \
                     --name "$TENANT_NAME" --plan free --key-name default --force 2>&1 \
                   || true )"
  # The CLI persists the api_key to its config file. Read it back.
  if [[ ! -s "$CLI_CONFIG_FILE" ]]; then
    echo "$SIGNUP_OUT" >&2
    die "edge auth signup did not write $CLI_CONFIG_FILE"
  fi
  API_KEY="$(jq -r '.default.api_key' "$CLI_CONFIG_FILE" 2>/dev/null || true)"
  TENANT_ID="$(grep -oE 'Tenant [a-z0-9_]+|t_[a-f0-9]+' <<<"$SIGNUP_OUT" | head -1 | awk '{print $NF}')"
  if [[ -z "$API_KEY" || "$API_KEY" == "null" ]]; then
    die "could not parse api_key from $CLI_CONFIG_FILE after signup"
  fi
  # If we didn't capture the tenant_id from output, derive it via whoami.
  if [[ -z "$TENANT_ID" || "$TENANT_ID" == "null" ]]; then
    WHOAMI_OUT="$( cd "$REPO_ROOT" && EDGE_API_URL="http://127.0.0.1:8080" \
                   "$EDGE_BIN" auth whoami 2>&1 || true )"
    TENANT_ID="$(grep -oE 't_[a-z0-9]+' <<<"$WHOAMI_OUT" | head -1 || true)"
  fi
  [[ -n "$TENANT_ID" && "$TENANT_ID" == t_* ]] || die "could not determine tenant_id from signup/whoami"
  log "created tenant $TENANT_ID"
fi

# From here on we explicitly thread the api_key via env so the CLI
# doesn't need to re-read config in every subshell.
EDGE_API_KEY="$API_KEY" export EDGE_API_KEY

# ── 10. Build / deploy / activate samples/hello ────────────────────────
step "10. build + deploy + activate samples/$APP_NAME"
SAMPLE_DIR="$REPO_ROOT/samples/$APP_NAME"
( cd "$SAMPLE_DIR" && EDGE_API_URL="http://127.0.0.1:8080" "$EDGE_BIN" build ) \
  || die "edge build in $SAMPLE_DIR failed"

DEPLOY_OUT="$( cd "$SAMPLE_DIR" && EDGE_API_URL="http://127.0.0.1:8080" \
                "$EDGE_BIN" deploy --app "$APP_NAME" 2>&1 )" \
  || die "edge deploy --app $APP_NAME failed"
DEPLOYMENT_ID="$(grep -oE 'd_[a-z0-9]+' <<<"$DEPLOY_OUT" | head -1 || true)"
[[ -n "$DEPLOYMENT_ID" ]] || log "WARN: could not extract deployment_id from deploy output; activate with --latest"
echo "$DEPLOY_OUT" >&2  # surface the deploy output for the operator

if [[ -n "$DEPLOYMENT_ID" ]]; then
  ( cd "$SAMPLE_DIR" && EDGE_API_URL="http://127.0.0.1:8080" \
      "$EDGE_BIN" activate --app "$APP_NAME" --deployment "$DEPLOYMENT_ID" ) \
    || die "edge activate failed"
else
  ( cd "$SAMPLE_DIR" && EDGE_API_URL="http://127.0.0.1:8080" \
      "$EDGE_BIN" activate --app "$APP_NAME" ) \
    || die "edge activate failed"
fi

# ── 11. Wait for Caddy to publish the route ────────────────────────────
step "11. waiting for Caddy to publish the route for $APP_NAME (≤ ${WAIT_CADDY_ROUTE_SECS}s)"
ROUTE_KEY="${TENANT_ID}-${APP_NAME}.${HOST_FQDN}"
CADDY_ADMIN="${CADDY_ADMIN_TOKEN:-}"
[[ -n "$CADDY_ADMIN" ]] || CADDY_ADMIN="$(env_get CADDY_ADMIN_TOKEN)"
deadline=$(( $(date +%s) + WAIT_CADDY_ROUTE_SECS ))
while (( $(date +%s) < deadline )); do
  CONFIG_JSON="$(curl -fsS -H "Authorization: Bearer ${CADDY_ADMIN}" \
                 "http://127.0.0.1:2019/config/" 2>/dev/null || true)"
  if [[ -n "$CONFIG_JSON" ]] && echo "$CONFIG_JSON" | jq -e --arg h "$ROUTE_KEY" \
        '.apps.http.servers[].routes[].match[]? | select(.host? | test("^" + $h + "$"))' >/dev/null 2>&1; then
    log "Caddy has a route for $ROUTE_KEY"
    break
  fi
  sleep 2
done
(( $(date +%s) < deadline )) || die "Caddy did not publish a route for $ROUTE_KEY within ${WAIT_CADDY_ROUTE_SECS}s"

# ── 12. Smoke assert: HTTP 200 from Caddy → ingress → worker ──────────
step "12. smoke assert: curl http://localhost:80/hello (Host: $ROUTE_KEY) ≤ ${SMOKE_TIMEOUT_SECS}s"
deadline=$(( $(date +%s) + SMOKE_TIMEOUT_SECS ))
STATUS=000
while (( $(date +%s) < deadline )); do
  STATUS="$(curl -fsS -o /tmp/bootstrap-smoke.body -w '%{http_code}' \
            -m 10 -H "Host: ${ROUTE_KEY}" http://127.0.0.1:80/hello 2>/dev/null || echo 000)"
  if [[ "$STATUS" == "200" ]]; then
    log "smoke ok — HTTP 200 from $ROUTE_KEY"
    break
  fi
  sleep 2
done
if [[ "$STATUS" != "200" ]]; then
  log "smoke FAILED — status=$STATUS"
  log "first 1 KiB of response body:"
  head -c 1024 /tmp/bootstrap-smoke.body >&2 || true
  log "first 80 lines of compose logs:"
  ( cd "$REPO_ROOT" && docker compose -f docker-compose.prod.yml logs --no-color --tail=80 ingress worker cp 2>&1 ) \
    | sed 's/\(EDGE_INTERNAL_TOKEN=\|TOKEN=\|PASSWORD=\|SECRET=\)[^[:space:]]*/\1<REDACTED>/g' >&2
  die "smoke assert failed"
fi

# ── 13. Persist seed for re-run idempotency ────────────────────────────
step "13. persisting $SEED_FILE (mode 0600)"
cat >"$SEED_FILE" <<EOF
{
  "tenant_id": "${TENANT_ID}",
  "tenant_name": "${TENANT_NAME}",
  "api_key": "${API_KEY}",
  "app_name": "${APP_NAME}",
  "deployment_id": "${DEPLOYMENT_ID}",
  "host": "${ROUTE_KEY}",
  "smoke_path": "/hello",
  "generated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
chmod 0600 "$SEED_FILE"

echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "  edgeCloud prod stack is up and serving traffic."
echo ""
echo "  Tenant:    $TENANT_ID ($TENANT_NAME)"
echo "  API key:   $API_KEY"
echo "  URL:       http://$ROUTE_KEY/hello  (use -H 'Host: $ROUTE_KEY' if not on 127.0.0.1)"
echo "  Persisted: $SEED_FILE"
echo ""
echo "  Re-run this script to skip signup + reuse the persisted api_key."
echo "  Stop the stack with:    make prod-down"
echo "  Wipe volumes and reset: make prod-reset"
echo "════════════════════════════════════════════════════════════════════"
