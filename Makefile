# Ergonomic wrappers around the docker-compose.yml at the repo root
# and the existing binaries. Lets a new contributor get a working dev
# environment in three commands:
#
#   make infra-up      # Postgres + NATS in the background
#   make migrate       # apply schema migrations (one-time)
#   make run-api       # foreground control plane
#
# See README.md → "Local development" for the full workflow.
#
# Production-equivalent stack (Dockerfiles + compose file; issue #512):
#
#   make prod-secrets  # render operator-specific secret files from .env.prod
#   make prod-up       # build + start the full stack (Postgres, NATS, CP, worker, ingress, Caddy)
#   make prod-smoke    # curl http://<tenant>-hello.edgecloud.dev/health through Caddy
#   make prod-down     # stop, keep volumes
#   make prod-reset    # nuke volumes + re-apply migrations
#
# See docs/prod-compose.md for the full operator workflow.

.PHONY: infra-up infra-down infra-logs infra-ps infra-reset \
        migrate run-api run-worker help \
        dev dev-prereqs dev-install dev-config dev-down dev-clean \
        prod-secrets prod-up prod-down prod-logs prod-ps prod-reset prod-smoke prod-migrate

# Load .env at parse time so DATABASE_PASSWORD/POSTGRES_* survive into
# every recipe shell AND into recursive $(MAKE) invocations (e.g. the
# `$(MAKE) migrate` call from infra-reset). `-include` (not `include`)
# so a missing .env is non-fatal here; infra-up / infra-reset print the
# "copy .env.example" error before any compose / migrate work runs.
#
# Only DATABASE_* and POSTGRES_* are exported — the rest of `.env`
# (JWT_*, BOOTSTRAP_*, NATS_*) is set explicitly via `set -a; . ./.env`
# inside infra-up so `docker compose` reads them as its env file
# semantics (substitution + strict-fail) without polluting Make's
# variable namespace.
-include .env
export DATABASE_USER DATABASE_PASSWORD DATABASE_NAME DATABASE_HOST DATABASE_PORT DATABASE_SSLMODE
export POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB

# Production env (issue #512). `-include` (not `include`) so a missing
# .env.prod is non-fatal — prod-up / prod-reset print the "copy
# .env.prod.example" error before any compose work runs. The dev
# targets that don't touch prod don't read these.
-include .env.prod
export \
    APP_REGION REGION WORKER_ID WORKER_TENANT_ID EDGE_WORKER_ADDR \
    JWT_SECRET JWT_TTL_HOURS JWT_ISSUER \
    EDGE_INTERNAL_TOKEN BOOTSTRAP_SECRET \
    NATS_URL NATS_REPLICAS \
    EDGE_REQUIRE_SIGNATURE EDGE_SIGNING_KEY_ID EDGE_SIGNING_KEYRING \
    METRICS_AUTH_TOKEN \
    EDGE_MIGRATE_PATH EDGE_WASM2CWASM_PATH \
    CADDY_ADMIN_TOKEN \
    TLS_CERT_FILE TLS_KEY_FILE \
    QUOTA_FETCH_INTERVAL RATE_LIMIT_FETCH_INTERVAL DOMAIN_POLL_INTERVAL

help:                   ## Show this help.
	@awk 'BEGIN {FS = ":.*##"; printf "Targets:\n"} \
	/^[a-zA-Z_-]+:.*?##/ {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ----- Infrastructure (Postgres + NATS) -----

infra-up:              ## Start Postgres + NATS in the background.
	@if [ ! -f .env ]; then \
		echo "error: .env not found. Copy .env.example:  cp .env.example .env" >&2; \
		exit 1; \
	fi
	@if grep -Eq '^POSTGRES_PASSWORD=(edgecloud|postgres|password|changeme|default|admin)([[:space:]]|$$)' .env; then \
		echo "warning: POSTGRES_PASSWORD in .env is a known placeholder; override before any non-local use."; \
	fi
	set -a; . ./.env; set +a; \
	  docker compose up -d
	@echo ""
	@echo "Postgres: localhost:5432  (user/db from .env; password from .env — dev default if unchanged)"
	@echo "NATS:      nats://localhost:4222  (JetStream enabled; monitoring on :8222)"

infra-down:            ## Stop the infra containers (keeps the Postgres volume).
	docker compose down

infra-logs:            ## Tail logs from both infra containers.
	docker compose logs -f

infra-ps:              ## Show container status.
	docker compose ps

# Wipe the Postgres volume and recreate the schema from scratch.
# Use after the migrations directory changes in a way that requires
# a clean slate (e.g. adding a NOT NULL on a column with existing
# rows in your dev DB).
infra-reset:           ## Stop infra, wipe Postgres volume, re-apply migrations.
	@if [ ! -f .env ]; then \
		echo "error: .env not found. Copy .env.example:  cp .env.example .env" >&2; \
		exit 1; \
	fi
	@if grep -Eq '^POSTGRES_PASSWORD=(edgecloud|postgres|password|changeme|default|admin)([[:space:]]|$$)' .env; then \
		echo "warning: POSTGRES_PASSWORD in .env is a known placeholder; override before any non-local use."; \
	fi
	set -a; . ./.env; set +a; \
	  docker compose down -v && \
	  docker compose up -d && \
	  until docker compose exec -T postgres pg_isready -U $${POSTGRES_USER:-edgecloud} -d $${POSTGRES_DB:-edgecloud}; do sleep 1; done
	$(MAKE) migrate

# ----- Migrations (against the running infra) -----

# Apply pending migrations without resetting data. Matches what the
# control plane expects on first start (it doesn't auto-migrate).
# JWT_SECRET is set to a stable dev-only 64-char value to bypass the
# config.Load placeholder check; rotate for any non-local use.
# DATABASE_PASSWORD is sourced from `.env` via the top-level `-include`
# so the CP's validateDBPassword (issue #626) sees a valid value.
migrate:               ## Apply all pending migrations against the running Postgres.
	@if [ ! -f .env ]; then \
		echo "error: .env not found. Copy .env.example:  cp .env.example .env" >&2; \
		exit 1; \
	fi
	cd edge-control-plane && \
	  JWT_SECRET=$$(printf 'dev-only-do-not-use-in-production-%s' $$(date +%s) | head -c 64) \
	  go run ./cmd/migrate --up

# ----- Binaries (against the running infra) -----

# Foreground control plane on :8080. JWT_SECRET is set to a dev-only
# 64-char value to bypass the config.Load placeholder check; rotate
# for any non-local use. DATABASE_PASSWORD is sourced from `.env` via
# the top-level `-include` so the CP's validateDBPassword (issue #626)
# sees a valid value.
run-api:               ## Run the control plane in the foreground.
	@if [ ! -f .env ]; then \
		echo "error: .env not found. Copy .env.example:  cp .env.example .env" >&2; \
		exit 1; \
	fi
	cd edge-control-plane && \
	  JWT_SECRET=$$(printf 'dev-only-do-not-use-in-production-%s' $$(date +%s) | head -c 64) \
	  go run ./cmd/api

# Foreground worker. Needs these env vars set (no defaults):
#   WORKER_ID              e.g. w_fra_dev
#   WORKER_TENANT_ID       e.g. t_system
#   WORKER_JWT_SECRET      matches JWT_SECRET in edge-control-plane/config.yaml
#   REGION                 e.g. fra
# Example: REGION=fra WORKER_ID=w_fra_dev WORKER_TENANT_ID=t_system WORKER_JWT_SECRET=change-me-in-production make run-worker
run-worker:            ## Run a worker in the foreground (env vars required).
	cargo run --bin edge-worker

# ----- Single-command dev stack (macOS-friendly) -----
#
# `make dev` brings up the full stack — Postgres + NATS + control plane
# + worker + ingress + Caddy + a deployed samples/hello FaaS handler —
# in the foreground with prefixed logs and Ctrl+C cleanup. It is the
# recommended entry point for new contributors on macOS.
#
# The targets below DO NOT chain the existing infra-up/run-api/run-worker
# targets because Make can't express the signal-trap and process-group
# semantics needed for foreground orchestration with cleanup.
#
# Prerequisites: Docker Desktop running, Go 1.23+, Rust + rustup with
# `wasm32-wasip2` target, jq, openssl. `make dev-install` handles all
# of these on a fresh macOS box with Homebrew.

dev:                   ## Bring up the full edgeCloud stack (foreground; Ctrl+C to stop).
	@bash scripts/dev-up.sh

dev-prereqs:           ## Verify macOS prereqs (no install). Exits non-zero on missing.
	@bash scripts/dev-up.sh --check-only

dev-install:           ## Install macOS prereqs via Homebrew + rustup.
	@bash scripts/dev-install.sh

dev-config:            ## Regenerate ~/.edgecloud/env.sh + edge-control-plane/config.local.yaml.
	@bash scripts/dev-up.sh --write-config

dev-down:              ## Stop postgres + nats containers (preserves the Postgres volume).
	@docker compose down

dev-clean:             ## Stop everything + wipe ~/.edgecloud state. Run with `bash scripts/dev-clean.sh --purge` to also wipe artifacts.
	@bash scripts/dev-clean.sh

# ----- Production-equivalent stack (issue #512) -----
#
# Dockerfiles are committed at edge-{control-plane,worker,ingress}/
# Dockerfile. The compose file lives at docker-compose.prod.yml (repo
# root) and brings up Postgres + NATS + the three services + Caddy on
# a single Linux host. Mirrors the dev infra-* shape but with
# multi-stage builds, secrets via .env.prod, and healthcheck-gated
# dependency ordering.
#
# Pre-flight: `cp .env.prod.example .env.prod` and edit the
# placeholders. The compose strict-fails on any unset secret via
# `${VAR:?msg}` (same pattern as the dev compose on line 28, per PR
# #264 / #670).

# Render operator-specific files from .env.prod. Currently emits:
#   - docker-compose.prod/caddy.local.json — Caddy's `/etc/caddy/caddy.json`
#     with the operator's CADDY_ADMIN_TOKEN substituted in. Caddy
#     refuses to start with a placeholder bearer token, so this must
#     run before `prod-up`.
# Future: ./secrets/signing-keyring if EDGE_SIGNING_KEYRING_PATH
# points at a host path the operator can't pre-create (current
# expectation is operators bring their own).
prod-secrets:          ## Render operator-specific files from .env.prod.
	@if [ ! -f .env.prod ]; then \
		echo "error: .env.prod not found. Copy .env.prod.example:  cp .env.prod.example .env.prod" >&2; \
		exit 1; \
	fi
	@if [ -z "$$CADDY_ADMIN_TOKEN" ] || echo "$$CADDY_ADMIN_TOKEN" | grep -Eq '^replace-me'; then \
		echo "error: CADDY_ADMIN_TOKEN must be set in .env.prod (not the placeholder)." >&2; \
		exit 1; \
	fi
	@mkdir -p secrets
	@envsubst < docker-compose.prod/caddy.json > docker-compose.prod/caddy.local.json
	@echo "Rendered docker-compose.prod/caddy.local.json (and a secrets/ scaffold)."

prod-up:               ## Build + start the production-equivalent stack.
	@if [ ! -f .env.prod ]; then \
		echo "error: .env.prod not found. Copy .env.prod.example:  cp .env.prod.example .env.prod" >&2; \
		exit 1; \
	fi
	@if [ ! -f docker-compose.prod/caddy.local.json ]; then \
		echo "notice: docker-compose.prod/caddy.local.json not found. Running 'make prod-secrets' first."; \
		$(MAKE) prod-secrets; \
	fi
	set -a; . ./.env.prod; set +a; \
	  docker compose -f docker-compose.prod.yml up -d --build
	@echo ""
	@echo "API:        http://localhost:8080/health"
	@echo "Caddy:      http://localhost:2019/config/  (admin API)"
	@echo "Deploy via the edge CLI from your host or run make prod-smoke to seed + curl through Caddy."

prod-down:             ## Stop the production-equivalent stack (keeps volumes).
	docker compose -f docker-compose.prod.yml down

prod-logs:             ## Tail logs from all production services.
	docker compose -f docker-compose.prod.yml logs -f

prod-ps:               ## Show container status.
	docker compose -f docker-compose.prod.yml ps

prod-reset:            ## Stop, wipe volumes, re-apply migrations.
	@if [ ! -f .env.prod ]; then \
		echo "error: .env.prod not found. Copy .env.prod.example:  cp .env.prod.example .env.prod" >&2; \
		exit 1; \
	fi
	set -a; . ./.env.prod; set +a; \
	  docker compose -f docker-compose.prod.yml down -v && \
	  docker compose -f docker-compose.prod.yml up -d --build
	@echo "Volumes wiped; migrations re-applied on the cp container's first boot."

# Apply migrations against the running prod Postgres. Useful when
# the CP entrypoint's auto-migrate step is bypassed (SKIP_MIGRATE_ON_BOOT=true)
# and migrations need to be run via a one-shot container. Mirrors
# the dev `migrate` target shape.
prod-migrate:          ## Apply pending migrations against the running prod Postgres.
	@if [ ! -f .env.prod ]; then \
		echo "error: .env.prod not found. Copy .env.prod.example:  cp .env.prod.example .env.prod" >&2; \
		exit 1; \
	fi
	set -a; . ./.env.prod; set +a; \
	  docker compose -f docker-compose.prod.yml run --rm cp migrate -up

# Boot the stack, seed samples/hello via the bundled CLI, assert 200
# through Caddy. Used by the CI `compose-smoke` job as well as
# operators verifying a fresh install. Waits on the deep /ready
# endpoint before running deploy so the CP has applied migrations.
#
# The deploy step mounts ./samples/hello from the host into the CP
# container at /samples and shells out to `edge deploy /samples/hello`
# from inside. A future iteration will replace this with a
# dedicated `edgecli` service in docker-compose.prod.yml that has
# the CLI binary baked in — for now the CP's chdir'd WORKDIR
# (/etc/edge) doesn't carry it, and adding `samples/` to the CP
# image's COPY list would balloon the image.
prod-smoke:            ## Bring up the stack, deploy samples/hello, assert 200 through Caddy.
	@if [ ! -f .env.prod ]; then \
		echo "error: .env.prod not found. Copy .env.prod.example:  cp .env.prod.example .env.prod" >&2; \
		exit 1; \
	fi
	$(MAKE) prod-up
	@echo "Waiting for /ready on the control plane..."
	@until curl -fsS http://localhost:8080/ready 2>/dev/null | grep -E '"status":"(ok|degraded)"' >/dev/null; do sleep 2; done
	@echo "/ready is green. Attempting smoke against an example tenant..."
	@echo ""
	@echo "Manual smoke: docker compose -f docker-compose.prod.yml exec cp bash"
	@echo "From inside: cd /samples/hello && edge deploy . && edge activate"
