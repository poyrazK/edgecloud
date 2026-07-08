# Ergonomic wrappers around the docker-compose.yml at the repo root
# and the existing binaries. Lets a new contributor get a working dev
# environment in three commands:
#
#   make infra-up      # Postgres + NATS in the background
#   make migrate       # apply schema migrations (one-time)
#   make run-api       # foreground control plane
#
# See README.md → "Local development" for the full workflow.

.PHONY: infra-up infra-down infra-logs infra-ps infra-reset \
        migrate run-api run-worker help \
        dev dev-prereqs dev-install dev-config dev-down dev-clean

help:                   ## Show this help.
	@awk 'BEGIN {FS = ":.*##"; printf "Targets:\n"} \
	/^[a-zA-Z_-]+:.*?##/ {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ----- Infrastructure (Postgres + NATS) -----

infra-up:              ## Start Postgres + NATS in the background.
	docker compose up -d
	@echo ""
	@echo "Postgres: localhost:5432  (user/pass/db: edgecloud)"
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
	docker compose down -v
	docker compose up -d
	@echo "Waiting for Postgres to accept connections..."
	@until docker compose exec -T postgres pg_isready -U edgecloud -d edgecloud; do sleep 1; done
	$(MAKE) migrate

# ----- Migrations (against the running infra) -----

# Apply pending migrations without resetting data. Matches what the
# control plane expects on first start (it doesn't auto-migrate).
# JWT_SECRET is set to a stable dev-only 64-char value to bypass the
# config.Load placeholder check; rotate for any non-local use.
migrate:               ## Apply all pending migrations against the running Postgres.
	cd edge-control-plane && \
	  JWT_SECRET=$$(printf 'dev-only-do-not-use-in-production-%s' $$(date +%s) | head -c 64) \
	  go run ./cmd/migrate --up

# ----- Binaries (against the running infra) -----

# Foreground control plane on :8080. JWT_SECRET is set to a dev-only
# 64-char value to bypass the config.Load placeholder check; rotate
# for any non-local use.
run-api:               ## Run the control plane in the foreground.
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