#!/bin/bash
# Build the JS test fixture: compile index.js to handler-js.wasm via Javy.
# The pre-built .wasm is committed so CI doesn't need Javy.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAVY="${JAVY:-javy}"

echo "Building JS fixture with Javy..."
"$JAVY" build "$SCRIPT_DIR/index.js" -o "$SCRIPT_DIR/../handler-js.wasm"
echo "Done. $(ls -lh "$SCRIPT_DIR/../handler-js.wasm")"
