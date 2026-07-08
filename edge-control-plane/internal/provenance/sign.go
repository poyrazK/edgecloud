// Operator-facing provenance helpers. The hot path (signing on
// every deploy / migrate) lives in statement.go's SignStatement;
// this file carries the verification + inspection helpers used by
// operator tooling and CI.
//
// Public surface intentionally small: just enough to verify a
// persisted envelope against a keyring and to extract the
// `predicateType` for sanity checks. Future operator tooling (e.g.
// a `cmd/prov-verify` binary referenced from the PR description)
// can build on top.
package provenance

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// DecodePayload parses env.Payload (base64url-no-pad) back into
// the Statement it wraps. Returned as map[string]any so callers
// can inspect fields without committing to the full struct shape
// (a verifier might want to look at `_type` or `predicateType`
// without taking a hard dep on every Statement field).
//
// Use Canonicalize on a Statement value to re-canonicalize before
// signature verification — DecodePayload returns the canonical
// bytes AS THEY WERE SIGNED, so VerifyEnvelope just base64url-
// decodes env.Payload directly without re-canonicalizing.
func DecodePayload(env DSSEEnvelope) (map[string]any, error) {
	canonical, err := base64.RawURLEncoding.DecodeString(env.Payload)
	if err != nil {
		return nil, fmt.Errorf("provenance: payload base64url decode: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(canonical, &out); err != nil {
		return nil, fmt.Errorf("provenance: payload JSON parse: %w", err)
	}
	return out, nil
}

// PredicateType extracts the `predicateType` field of the
// embedded Statement. Used by sanity checks (an operator can
// confirm "this envelope is the SLSA provenance we expect" before
// committing to a full verification).
func PredicateType(env DSSEEnvelope) (string, error) {
	payload, err := DecodePayload(env)
	if err != nil {
		return "", err
	}
	pt, _ := payload["predicateType"].(string)
	return pt, nil
}

// SubjectSHA256 extracts the (single) subject's SHA-256 digest.
// Returns "" if the envelope has zero or multiple subjects (we
// always emit exactly one today; this graceful fallback protects
// future schema drift).
func SubjectSHA256(env DSSEEnvelope) (string, error) {
	payload, err := DecodePayload(env)
	if err != nil {
		return "", err
	}
	subjects, ok := payload["subject"].([]any)
	if !ok || len(subjects) == 0 {
		return "", nil
	}
	first, ok := subjects[0].(map[string]any)
	if !ok {
		return "", nil
	}
	digest, ok := first["digest"].(map[string]any)
	if !ok {
		return "", nil
	}
	sha, _ := digest["sha256"].(string)
	return sha, nil
}
