// Package httpx holds HTTP-flavored helpers shared across the control
// plane that don't belong to a single handler or service.
//
// pagination.go (this file) carries the shared keyset-cursor codec
// for user-facing list endpoints (issue #58). The wire format is a
// v1 JSON envelope {"v": <int>, "p": <caller-payload>}, base64url-
// encoded (RawURLEncoding — URL-safe, no padding). The caller owns
// the structure of <caller-payload>; this package owns the envelope,
// the version constant, and the typed errors.
//
// Rationale for the envelope vs. the per-resource codec already in
// internal/service/webhook_delivery_cursor.go and
// internal/service/logs_cursor.go:
//
//  1. The version field is what makes the format forward-compatible.
//     A future v2 cursor can coexist with v1 readers: older servers
//     return ErrUnsupportedCursorVersion (mapped to 400) instead of
//     silently accepting a newer payload as if it were v1.
//  2. Centralizing "what is invalid vs. unsupported" in one package
//     keeps every list endpoint's handler honest about why a 400
//     fires — a flaky parser check ("zero ts?" "negative id?")
//     collapses to one typed error from the caller's perspective.
//  3. Resource-specific zero-guards (e.g., zero time.Time, non-positive
//     int64) stay at the resource's own encode helper, called from the
//     service layer, NOT in this codec — keeps this package oblivious
//     to whatever shape the caller chooses.
package httpx

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

// cursorVersion is the only currently-defined cursor envelope version.
// Issue #58.
const cursorVersion = 1

var (
	// ErrInvalidCursor is returned when a cursor string is malformed
	// at the wire level: bad base64, malformed JSON envelope, zero
	// version field, or a payload that fails to decode into the
	// caller's out pointer. Handlers map this to 400 with a generic
	// "invalid cursor" message (mirrors the webhook deliveries
	// handler at internal/handler/webhook.go) so the wire response
	// cannot leak decoder internals.
	ErrInvalidCursor = errors.New("invalid cursor")

	// ErrUnsupportedCursorVersion is returned when the cursor was
	// produced by a server on a newer protocol version this reader
	// does not understand. Handlers also map this to 400 — the
	// client should retry with a freshly-fetched cursor rather
	// than reusing the unsupported one.
	ErrUnsupportedCursorVersion = errors.New("unsupported cursor version")
)

// EncodeCursor wraps payload in {"v": <cursorVersion>, "p": <payload>},
// JSON-encodes the envelope, and returns the base64url-encoded string
// (RawURLEncoding — no '+', '/', or '=').
//
// payload must marshal cleanly via encoding/json (no channels, no funcs,
// no cyclic pointers). The caller is responsible for any resource-
// specific zero-guards (e.g., rejecting a zero time.Time) before
// passing the payload in.
//
// Returns ErrInvalidCursor only if payload is nil — a defensive guard
// against an in-process caller passing a zero pointer that would
// otherwise serialize as JSON null.
func EncodeCursor(payload any) (string, error) {
	if payload == nil {
		return "", ErrInvalidCursor
	}
	body, err := json.Marshal(struct {
		V int `json:"v"`
		P any `json:"p"`
	}{V: cursorVersion, P: payload})
	if err != nil {
		return "", fmt.Errorf("encoding cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(body), nil
}

// DecodeCursor parses s and writes the payload into out (a pointer
// the caller supplies — e.g., *appCursor{Name string} or
// *deploymentCursor{TS time.Time; ID int64}).
//
// Errors:
//
//   - ErrInvalidCursor: malformed base64, malformed JSON envelope,
//     zero version field, or a payload that fails to decode into
//     out.
//   - ErrUnsupportedCursorVersion: the envelope's v field is not
//     cursorVersion (1). An older server refused a cursor produced
//     by a newer server; client should re-fetch.
//
// Handlers MUST map both error types to 400. They MUST surface a
// generic "invalid cursor" or "unsupported cursor version" message,
// depending on which error fired (caller can use errors.Is to
// distinguish), and log the typed error at info level with a tenant
// id so ops can tell malformed-from-unsupported in production.
// Mirrors the typed-error pattern at internal/handler/webhook.go.
func DecodeCursor(s string, out any) error {
	if s == "" {
		return ErrInvalidCursor
	}
	body, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return ErrInvalidCursor
	}
	var envelope struct {
		V int             `json:"v"`
		P json.RawMessage `json:"p"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return ErrInvalidCursor
	}
	if envelope.V == 0 {
		// Zero is treated as invalid because every cursor produced
		// by EncodeCursor carries V == cursorVersion; a zero v
		// means the string was hand-crafted or corrupted.
		return ErrInvalidCursor
	}
	if envelope.V != cursorVersion {
		return ErrUnsupportedCursorVersion
	}
	if err := json.Unmarshal(envelope.P, out); err != nil {
		return ErrInvalidCursor
	}
	return nil
}
