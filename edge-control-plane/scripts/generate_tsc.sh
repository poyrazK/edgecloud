#!/bin/bash
# Generates a TypeScript client from the OpenAPI spec.
# The generated file is checked into git so CI can detect drift.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SPEC="$ROOT_DIR/docs/api/openapi.yaml"
OUTPUT="$ROOT_DIR/internal/generated/api-types.ts"

cd "$ROOT_DIR"

# Check if Node.js is available
if ! command -v node &> /dev/null; then
    echo "error: Node.js is required to generate the TypeScript client" >&2
    exit 1
fi

# Install openapi-typescript if not present (cached in node_modules)
npx --yes openapi-typescript@7 "$SPEC" \
    --output "$OUTPUT" \
    --strict

echo "Generated TypeScript client: $OUTPUT"
