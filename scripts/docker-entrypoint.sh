#!/bin/sh
# docker-entrypoint.sh — runs /usr/local/bin/<CMD> as PID 1, optionally
# applying pending schema migrations first.
#
# In order:
#   1. cd /etc/edge (so the embedded config.yaml is the CWD-relative
#      path both cmd/api and cmd/migrate look for; neither binary
#      takes a -config flag — see cmd/api/main.go:34 and
#      cmd/migrate/main.go:50).
#   2. Unless SKIP_MIGRATE_ON_BOOT=true is set, run `migrate -up`
#      so the database schema matches the running binary on cold
#      boot. Migrate exits non-zero on any failure (it uses
#      log.Fatalf inside Rubenv/sql-migrate Exec), at which point we
#      refuse to start the API — a partial-failure boot is worse
#      than a refused boot.
#   3. exec the requested binary ("api" by default, but the operator
#      can override via CMD in compose: ["printpub"], etc.).
#
# Resolves CMD from the binary name passed in by Docker — typically
# "api" (the default set in the Dockerfile's CMD instruction).

set -eu

cd /etc/edge

# `migrate` is hard-coded: the entrypoint's only job is api-things.
# Operators wanting printpub or a one-off migrate-only debug can set
# CMD=["printpub"] or similar; migrate is not a CMD, only run as a
# pre-step on api boot.
MIGRATE_BIN=/usr/local/bin/migrate

if [ "${SKIP_MIGRATE_ON_BOOT:-false}" = "true" ]; then
    echo "[entrypoint] SKIP_MIGRATE_ON_BOOT=true; not running schema migrations."
elif [ -x "${MIGRATE_BIN}" ]; then
    echo "[entrypoint] Applying pending schema migrations..."
    if "${MIGRATE_BIN}" -up; then
        echo "[entrypoint] Migrations applied."
    else
        rc=$?
        echo "[entrypoint] FATAL: migrate failed with exit code ${rc}; refusing to start API." >&2
        exit "${rc}"
    fi
else
    echo "[entrypoint] migrate binary not found at ${MIGRATE_BIN}; skipping (a non-migrator image build?)."
fi

# Resolve the command name passed by Docker (CMD in the Dockerfile).
# Default CMD is "api" — the production HTTP server.
CMD_NAME="${1:-api}"
CMD_PATH="/usr/local/bin/${CMD_NAME}"

if [ ! -x "${CMD_PATH}" ]; then
    echo "[entrypoint] FATAL: requested command ${CMD_NAME} not found at ${CMD_PATH}." >&2
    exit 127
fi

echo "[entrypoint] exec ${CMD_PATH} ${*:2}"
exec "${CMD_PATH}" "${@:2}"
