#!/usr/bin/env bash
# scripts/dev-install.sh — install the prereqs that scripts/dev-up.sh
# checks for in preflight. Idempotent: skips anything already installed.
#
# Assumes Homebrew is already installed (the bootstrap tool for macOS).
# If brew is missing, prints the install URL and exits 1.
#
# Installs:
#   - go (>= 1.23)
#   - rustup-init + stable Rust toolchain
#   - wasm32-wasip2 Rust target
#   - docker (Docker Desktop via brew --cask)
#   - caddy (formula; we run it via `docker run caddy:2` but having the
#     formula means the README's quickstart also works)
#   - openssl (preinstalled on macOS but the brew formula is current)
#   - jq
#   - wasi-sdk (optional — only needed if you'll use `edge migrate`)
#
# Does NOT install:
#   - mkcert — no longer needed (we skip TLS for dev)
#   - PostgreSQL / NATS binaries — provided via docker compose

set -euo pipefail

log() { echo "[dev-install] $*" >&2; }

# ── 1. Verify brew is present ───────────────────────────────────────────

if ! command -v brew >/dev/null 2>&1; then
  log "ERROR: Homebrew not found. Install from https://brew.sh and re-run."
  exit 1
fi

# ── 2. Install formulae ─────────────────────────────────────────────────

# Each entry is checked before install; brew install is idempotent.
# Docker Desktop is a cask, not a formula; installed via `brew install --cask`.
FORMULAE=(
  go                       # Go >= 1.23
  openssl                  # current openssl (macOS ships an old one)
  jq                       # used by verify.sh
  caddy                    # quickstart path (we run via docker in dev-up.sh)
  wasi-sdk                 # optional: only for `edge migrate`
)

CASKS=(
  docker                   # Docker Desktop
)

# rustup is not a brew formula in Homebrew/core; it's installed via the
# official rustup-init script. Done after formulae to keep flow simple.

log "installing brew formulae (skipping already-installed)..."
for formula in "${FORMULAE[@]}"; do
  if brew list --formula "$formula" >/dev/null 2>&1; then
    log "  $formula already installed"
  else
    log "  installing $formula"
    brew install "$formula"
  fi
done

log "installing brew casks..."
for cask in "${CASKS[@]}"; do
  if brew list --cask "$cask" >/dev/null 2>&1; then
    log "  $cask already installed"
  else
    log "  installing $cask (this may take a few minutes)"
    brew install --cask "$cask"
  fi
done

# ── 3. Install Rust via rustup ──────────────────────────────────────────

if command -v rustup >/dev/null 2>&1; then
  log "rustup already installed"
else
  log "installing rustup + stable toolchain"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
  # shellcheck source=/dev/null
  source "$HOME/.cargo/env"
fi

# wasm32-wasip2 target — required to build samples/hello and any Rust
# guest that uses the edge:cloud WIT world.
if rustup target list --installed 2>/dev/null | grep -q '^wasm32-wasip2$'; then
  log "rustup target wasm32-wasip2 already installed"
else
  log "adding rustup target wasm32-wasip2"
  rustup target add wasm32-wasip2
fi

# ── 4. Final check ──────────────────────────────────────────────────────

log "verifying installation..."
MISSING=()
for bin in go cargo rustc rustup docker openssl jq; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    MISSING+=("$bin")
  fi
done
if [[ ${#MISSING[@]} -gt 0 ]]; then
  log "ERROR: still missing: ${MISSING[*]}"
  log "Open a new terminal so $HOME/.cargo/env is sourced, then re-run."
  exit 1
fi

log "all prereqs installed. Next: 'make dev'."