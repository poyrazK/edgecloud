#!/usr/bin/env bash
# Cross-language rollback end-to-end orchestrator for issue #613.
#
# Boots the Go half (`edge-control-plane/internal/integration/...`)
# and the Rust half (`edge-worker/tests/cp_rollback_e2e.rs`) against
# a single shared NATS container, coordinated via sentinel files
# under /tmp/edge-e2e/. Mirrors the precedent set by PR #652 (the
# cross-language Ed25519 wire-contract test).
#
# Flow:
#   1. mkdir -p /tmp/edge-e2e
#   2. launch Go half in background (it spins up Postgres + NATS
#      containers, runs migrations, writes nats-url)
#   3. wait for nats-url
#   4. export EDGE_TEST_NATS_URL from nats-url
#   5. launch Rust half (it subscribes to heartbeats, spawns
#      run_consume_loop, writes rust-ready)
#   6. wait for rust-done
#   7. kill Go half, rm -rf /tmp/edge-e2e
#
# Failures in either half bubble out via `set -e` + the explicit
# poll loops below; on any unexpected exit the cleanup trap kills
# stragglers and removes the sentinel dir so a follow-up run starts
# clean.
#
# Exit codes:
#   0 — both halves passed
#   1 — Go half failed
#   2 — Rust half failed
#   3 — orchestration timeout (no rust-done within budget)
#   4 — prerequisites missing (docker, cargo, go)

set -euo pipefail

# ---- knobs ----
SENTINEL_DIR="${SENTINEL_DIR:-/tmp/edge-e2e}"
GO_TEST_TIMEOUT="${GO_TEST_TIMEOUT:-5m}"
# RUST_TEST_TIMEOUT caps the `cargo test` invocation itself (build +
# link + run). 15m is generous — cold-cache CI rebuilds the full
# workspace from source and even with rust-cache + sccache that can
# take >5m on a fresh runner. Warm-cache runs finish in seconds.
RUST_TEST_TIMEOUT="${RUST_TEST_TIMEOUT:-15m}"
NATS_URL_WAIT="${NATS_URL_WAIT:-2m}"
# RUST_DONE_WAIT caps the wall-clock from when the Rust half launches
# to when it must write /tmp/edge-e2e/rust-done. Includes the cargo
# build (already counted in RUST_TEST_TIMEOUT) plus the actual test
# runtime — three heartbeat transitions at the 2s tick + ~10s of
# container/NATS startup, so 6m is well above the floor.
RUST_DONE_WAIT="${RUST_DONE_WAIT:-6m}"

# ---- preflight ----
for bin in docker go cargo; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "FATAL: $bin is required on PATH" >&2
    exit 4
  fi
done
if [[ ! -e /var/run/docker.sock ]]; then
  echo "FATAL: /var/run/docker.sock is required (testcontainers needs Docker)" >&2
  exit 4
fi

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
GO_HALF_DIR="${REPO_ROOT}/edge-control-plane"
RUST_HALF_DIR="${REPO_ROOT}/edge-worker"
GO_LOG="${SENTINEL_DIR}/go.log"
RUST_LOG="${SENTINEL_DIR}/rust.log"

mkdir -p "${SENTINEL_DIR}"
# Clean any prior sentinel files so a stale rust-ready / rust-done
# from a previous run can't fool the polls.
rm -f "${SENTINEL_DIR}/nats-url" "${SENTINEL_DIR}/rust-ready" "${SENTINEL_DIR}/rust-done"

GO_PID=""
RUST_PID=""

cleanup() {
  # Tear down whichever half is still alive, then drop sentinels.
  # The Go half's t.Cleanup blocks tear down Postgres + NATS, so we
  # MUST wait for it to exit cleanly (or `go test` will leak the
  # container past this script's exit).
  #
  # The `kill ... 2>/dev/null || true` pattern below is intentional:
  # a stale PID, a process that already exited between `kill -0` and
  # `kill`, or a `wait` on a cleared job table all return non-zero
  # and would abort this trap under `set -e`. We DON'T want cleanup
  # to fail — the orchestrator's real exit code is set by the
  # explicit fan-out at the bottom of the script, not here.
  if [[ -n "${RUST_PID}" ]] && kill -0 "${RUST_PID}" 2>/dev/null; then
    echo "cleanup: killing Rust half pid=${RUST_PID}" >&2
    kill "${RUST_PID}" 2>/dev/null || true
    wait "${RUST_PID}" 2>/dev/null || true
  fi
  if [[ -n "${GO_PID}" ]] && kill -0 "${GO_PID}" 2>/dev/null; then
    echo "cleanup: killing Go half pid=${GO_PID}" >&2
    # SIGTERM lets the Go half's t.Cleanup run and tear down
    # containers; SIGKILL after 30s grace.
    kill "${GO_PID}" 2>/dev/null || true
    for _ in $(seq 1 30); do
      kill -0 "${GO_PID}" 2>/dev/null || break
      sleep 1
    done
    kill -9 "${GO_PID}" 2>/dev/null || true
    wait "${GO_PID}" 2>/dev/null || true
  fi
  rm -rf "${SENTINEL_DIR}"
}
trap cleanup EXIT

# ---- step 1: launch Go half in background ----
echo "==> launching Go half (background); logs → ${GO_LOG}"
(
  cd "${GO_HALF_DIR}"
  # -count=1 disables the Go test cache so re-runs always execute.
  # -timeout is the per-test budget; the package-level default is
  # 10m and that's fine here.
  go test \
    -tags=integration \
    -count=1 \
    -timeout "${GO_TEST_TIMEOUT}" \
    -v \
    -run TestRollbackE2E \
    ./internal/integration/...
) >"${GO_LOG}" 2>&1 &
GO_PID=$!

# ---- step 2: wait for nats-url ----
echo "==> waiting for ${SENTINEL_DIR}/nats-url (up to ${NATS_URL_WAIT})"
deadline=$(( $(date +%s) + $(echo "${NATS_URL_WAIT}" | sed -E 's/([0-9]+)m/\1 * 60/; s/([0-9]+)s/\1/') ))
while [[ ! -s "${SENTINEL_DIR}/nats-url" ]]; do
  if ! kill -0 "${GO_PID}" 2>/dev/null; then
    echo "FATAL: Go half exited before writing nats-url. Tail of log:" >&2
    tail -50 "${GO_LOG}" >&2 || true
    exit 1
  fi
  if [[ $(date +%s) -ge ${deadline} ]]; then
    echo "FATAL: nats-url never appeared. Tail of log:" >&2
    tail -50 "${GO_LOG}" >&2 || true
    exit 1
  fi
  sleep 1
done
NATS_URL="$(cat "${SENTINEL_DIR}/nats-url")"
echo "==> got nats-url=${NATS_URL}"

# ---- step 3: launch Rust half ----
echo "==> launching Rust half (background); logs → ${RUST_LOG}"
(
  cd "${RUST_HALF_DIR}"
  # RUN_INTEGRATION_TESTS=1 bypasses the default CI-skip predicate in
  # should_skip_integration_tests(). EDGE_TEST_NATS_URL is the
  # shared NATS URL the Go half published.
  EDGE_TEST_NATS_URL="${NATS_URL}" \
    RUN_INTEGRATION_TESTS=1 \
    timeout "${RUST_TEST_TIMEOUT}" \
    cargo test \
      --test cp_rollback_e2e \
      -- \
      --nocapture
) >"${RUST_LOG}" 2>&1 &
RUST_PID=$!

# ---- step 4: wait for rust-done ----
echo "==> waiting for ${SENTINEL_DIR}/rust-done (up to ${RUST_DONE_WAIT})"
deadline=$(( $(date +%s) + $(echo "${RUST_DONE_WAIT}" | sed -E 's/([0-9]+)m/\1 * 60/; s/([0-9]+)s/\1/') ))
while [[ ! -s "${SENTINEL_DIR}/rust-done" ]]; do
  # Rust half exiting early = assertions failed
  if ! kill -0 "${RUST_PID}" 2>/dev/null; then
    echo "==> Rust half exited before writing rust-done. Tail of log:" >&2
    tail -80 "${RUST_LOG}" >&2 || true
    # Go half may still be alive — let cleanup tear it down
    exit 2
  fi
  if [[ $(date +%s) -ge ${deadline} ]]; then
    echo "FATAL: rust-done never appeared within ${RUST_DONE_WAIT}. Tail of log:" >&2
    tail -80 "${RUST_LOG}" >&2 || true
    exit 3
  fi
  sleep 1
done
echo "==> rust-done received; both halves passed"

# ---- step 5: let Go half finish + tear down ----
echo "==> waiting for Go half to drain (up to 30s)"
for _ in $(seq 1 30); do
  kill -0 "${GO_PID}" 2>/dev/null || break
  sleep 1
done

GO_EXIT=0
if kill -0 "${GO_PID}" 2>/dev/null; then
  echo "WARN: Go half still alive after rust-done; killing" >&2
  kill "${GO_PID}" 2>/dev/null || true
  wait "${GO_PID}" 2>/dev/null || GO_EXIT=$?
else
  wait "${GO_PID}" || GO_EXIT=$?
fi

if [[ ${GO_EXIT} -ne 0 ]]; then
  echo "FATAL: Go half exited with code ${GO_EXIT}. Tail of log:" >&2
  tail -80 "${GO_LOG}" >&2 || true
  exit 1
fi

# ---- step 6: report ----
echo
echo "===================================================="
echo "rollback-e2e: PASS"
echo "Go half:    ${GO_LOG}"
echo "Rust half:  ${RUST_LOG}"
echo "Sentinels:  ${SENTINEL_DIR} (will be removed)"
echo "===================================================="

# Successful exit — cleanup runs via trap.
