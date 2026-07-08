// printpub prints the public key derived from a CP signing key file.
// Operators run this once after generating the key (see
// docs/key-management.md) and paste the 64-char hex output into the
// worker's EDGE_SIGNING_PUBKEY (or write it to a file referenced by
// EDGE_SIGNING_PUBKEY_PATH).
//
// Usage:
//
//	go run ./cmd/printpub -key /etc/edge/signing.key
//	go run ./cmd/printpub -key /etc/edge/signing.key -key-id k1   # diagnostic; key_id is metadata, not crypto
//
// The hex output is the Ed25519 public key derived per RFC 8032 §5.1.2
// from the 32-byte seed (or 64-byte raw key). crypto/ed25519 does the
// derivation; we just print the result.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/signing"
)

func main() {
	keyPath := flag.String("key", "", "path to the CP signing key file (32-byte seed or 64-byte raw Ed25519 key)")
	keyID := flag.String("key-id", "", "optional logical key id (k1, k2, ...); printed as a comment line, not part of the crypto output")
	flag.Parse()

	if *keyPath == "" {
		fmt.Fprintln(os.Stderr, "error: -key is required")
		flag.Usage()
		os.Exit(2)
	}

	signer, err := signing.LoadFromFile(*keyPath, *keyID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading signing key %q: %v\n", *keyPath, err)
		os.Exit(1)
	}

	// One line of hex (no newline padding). Operators pipe to
	// kubectl create secret / envsubst / etc.
	fmt.Println(signer.PublicKeyHex())
	if *keyID != "" {
		fmt.Fprintf(os.Stderr, "# key_id=%s (stamped on each deployments.signing_key_id row)\n", *keyID)
	}
}
