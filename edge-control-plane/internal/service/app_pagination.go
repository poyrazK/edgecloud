// app_pagination.go (issue #58) — apps-list cursor codec.
//
// The apps list orders on `name` (the only stable key under the
// (tenant_id, name) UNIQUE constraint), so the cursor payload is a
// single string. Resource-specific zero-guards (an empty name is a
// valid cursor sentinel meaning "first page" — the same as
// afterName="") live here so callers don't have to reason about the
// envelope.
//
// Mirrors the per-resource codec shape at internal/service/
// webhook_delivery_cursor.go — same v1 envelope, same RawURLEncoding
// base64url, same typed-error contract — but delegates the encode
// and decode primitives to internal/httpx so every list endpoint's
// wire format stays consistent. The typed errors this file exports
// are thin aliases over httpx.ErrInvalidCursor /
// httpx.ErrUnsupportedCursorVersion so callers can match either via
// errors.Is without having to learn which package owns the
// underlying error.
//
// Issue #58 hard-cuts apps (no offset shim): if the cursor is
// absent or empty, this is the first page and the service treats
// afterName as "". The previous (limit, offset) callers are
// migrated to (limit, afterName) in the same PR — see Commit 3.

package service

import (
	"errors"
	"fmt"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/httpx"
)

// appCursor is the opaque payload embedded in the v1 cursor envelope
// for GET /api/v1/apps. Only Name is needed because `name` is unique
// per tenant and the keyset predicate is `name > $afterName`. Adding
// fields here requires bumping cursorVersion (httpx-side) and a
// migration in this file — keep this struct minimal.
type appCursor struct {
	Name string `json:"name"`
}

// appCursorVersion is the v1 envelope version this file emits. Mirrors
// the httpx.cursorVersion constant; tracked here so a future reader
// doesn't need to import httpx just to learn "what version does this
// service produce?".
const appCursorVersion = 1

// Errors this codec can return. Chained via %w to the underlying
// httpx errors so errors.Is(err, service.ErrInvalidAppCursor) matches
// the same error chain that httpx.EncodeCursor / httpx.DecodeCursor
// produce. Callers in the handler layer can write either:
//
//	if errors.Is(err, service.ErrInvalidAppCursor) { ... }
//	if errors.Is(err, httpx.ErrInvalidCursor) { ... }
//
// and both succeed.
var (
	// ErrInvalidAppCursor is returned when the supplied cursor string
	// is malformed: bad base64, malformed envelope, zero version,
	// or a payload that fails to decode. Handlers map to 400 with
	// the generic "invalid cursor" message.
	ErrInvalidAppCursor = fmt.Errorf("invalid app cursor: %w", httpx.ErrInvalidCursor)

	// ErrUnsupportedAppCursorVersion is returned when the cursor was
	// produced by a newer server this reader does not understand.
	// Handlers also map to 400 — the client should fetch a fresh
	// cursor rather than reuse the unsupported one.
	ErrUnsupportedAppCursorVersion = fmt.Errorf("unsupported app cursor version: %w", httpx.ErrUnsupportedCursorVersion)
)

// encodeAppCursor wraps the payload in the v1 envelope via httpx and
// returns a base64url string. The empty name is rejected up-front
// because the keyset contract is "after this name" — an empty cursor
// means "first page" and is handled by the caller, never by encoding.
func encodeAppCursor(name string) (string, error) {
	if name == "" {
		return "", ErrInvalidAppCursor
	}
	return httpx.EncodeCursor(appCursor{Name: name})
}

// decodeAppCursor parses s into the cursor payload and returns the
// embedded name. Errors are the typed httpx.ErrInvalidCursor /
// httpx.ErrUnsupportedCursorVersion chains; handlers match via
// errors.Is against service.ErrInvalidAppCursor and
// service.ErrUnsupportedAppCursorVersion respectively.
func decodeAppCursor(s string) (string, error) {
	var c appCursor
	if err := httpx.DecodeCursor(s, &c); err != nil {
		// httpx returns its own typed errors; map them to the
		// service-level aliases so callers matching against
		// service.ErrInvalidAppCursor / service.ErrUnsupportedAppCursorVersion
		// succeed without having to import httpx. The aliases already
		// chain via %w to the underlying httpx errors, so an
		// errors.Is(err, httpx.ErrInvalidCursor) check at the call
		// site still works.
		if errors.Is(err, httpx.ErrUnsupportedCursorVersion) {
			return "", ErrUnsupportedAppCursorVersion
		}
		return "", ErrInvalidAppCursor
	}
	if c.Name == "" {
		// Defensive: a v1 envelope that decoded cleanly but carries
		// an empty Name is treated as malformed. encodeAppCursor
		// never produces one, but a hand-crafted or partial cursor
		// shouldn't slip through.
		return "", ErrInvalidAppCursor
	}
	return c.Name, nil
}
