package service

import (
	"errors"
	"fmt"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/httpx"
)

// Issue #58 / #709 / #682: log cursor codec is now a thin
// resource-level wrapper around the shared `httpx.EncodeCursor` /
// `httpx.DecodeCursor` helpers. The wire format is unchanged —
// v1 envelope, base64url (RawURLEncoding, no padding) — but the
// base64 / JSON plumbing lives in `internal/httpx/pagination.go`
// alongside every other list-endpoint codec.
//
// Resource-specific concerns kept here:
//   - Zero-guard on encode: a zero ts or non-positive id is
//     rejected before any JSON marshal happens.
//   - Typed-error aliases: callers compare against
//     `ErrInvalidLogCursor` / `ErrUnsupportedLogCursorVersion`.
//     Both alias the httpx sentinels via `fmt.Errorf("...: %w", ...)`
//     so `errors.Is` keeps working through the chain.

var (
	// ErrInvalidLogCursor is returned when a log cursor cannot be
	// decoded or does not contain a valid timestamp and row ID.
	// Aliases httpx.ErrInvalidCursor via wrap.
	ErrInvalidLogCursor = fmt.Errorf("invalid log cursor: %w", httpx.ErrInvalidCursor)
	// ErrUnsupportedLogCursorVersion is returned when the cursor
	// was produced by a newer, unsupported cursor contract.
	// Aliases httpx.ErrUnsupportedCursorVersion.
	ErrUnsupportedLogCursorVersion = fmt.Errorf("unsupported log cursor version: %w", httpx.ErrUnsupportedCursorVersion)
)

// logCursor is the private v1 JSON payload carried in the `p` field
// of the httpx envelope. The (ts, id) pair keyset matches the
// repository's strict-tuple predicate from migration 025.
type logCursor struct {
	TS time.Time `json:"ts"`
	ID int64     `json:"id"`
}

// encodeLogCursor serializes the (ts, id) of the last visible log
// entry into the opaque cursor string returned to the client.
// Returns ErrInvalidLogCursor on a zero ts or non-positive id.
func encodeLogCursor(ts time.Time, id int64) (string, error) {
	if ts.IsZero() || id <= 0 {
		return "", ErrInvalidLogCursor
	}
	return httpx.EncodeCursor(logCursor{
		TS: ts.UTC(),
		ID: id,
	})
}

// decodeLogCursor parses the opaque base64 cursor string supplied
// by a client back into (ts, id). Returns
// ErrUnsupportedLogCursorVersion when the envelope's v field is
// not 1 and ErrInvalidLogCursor for any other parse failure. The
// returned timestamp is normalized to UTC.
func decodeLogCursor(raw string) (time.Time, int64, error) {
	var cursor logCursor
	if err := httpx.DecodeCursor(raw, &cursor); err != nil {
		switch {
		case errors.Is(err, httpx.ErrUnsupportedCursorVersion):
			return time.Time{}, 0, ErrUnsupportedLogCursorVersion
		default:
			return time.Time{}, 0, ErrInvalidLogCursor
		}
	}
	if cursor.TS.IsZero() || cursor.ID <= 0 {
		return time.Time{}, 0, ErrInvalidLogCursor
	}
	return cursor.TS.UTC(), cursor.ID, nil
}
