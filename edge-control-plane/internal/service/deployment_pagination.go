// deployment_pagination.go (issue #58 + #709) — deployments-list
// cursor codec.
//
// The deployments list orders on (created_at DESC, id DESC) and the
// keyset predicate is `created_at < $3 OR (created_at = $3 AND id <
// $4)`. The cursor payload is therefore a strict-tuple — a single
// string is not enough because two deployments can share a
// second-precision timestamp and the (id DESC) tiebreaker is
// load-bearing.
//
// Mirrors the per-resource codec shape at internal/service/
// webhook_delivery_cursor.go — same v1 envelope, same RawURLEncoding
// base64url, same typed-error contract — but delegates the encode
// and decode primitives to internal/httpx so every list endpoint
// shares the wire format.
//
// Issue #709 — the deployments.id column is TEXT (the `d_<uuid>`
// convention codified in domain.Deployment.ID), so the cursor
// payload carries the id as a string, not int64. The SQL comparator
// handles the lexicographic tiebreaker on the text column directly;
// the codec is unchanged in shape from #58.
//
// Resource-specific zero-guards:
//
//   - TS must NOT be the zero time.Time (Go's encoding/json renders
//     it as "0001-01-01T00:00:00Z"). encodeDeploymentCursor rejects
//     zero values up front; decodeDeploymentCursor rejects what
//     slips through hand-crafted cursors.
//
//   - ID must NOT be empty. The text PK guarantees uniqueness, so an
//     empty id can never appear in a real row — but accepting one
//     would silently no-op the keyset predicate.
//
// Both encode and decode force UTC: timestamps in the cursor are
// stored without a tz suffix by json.Marshal, so a naive parse round-
// trip can drift if the caller is in a non-UTC tz. We .UTC() both
// directions to keep the wire canonical.

package service

import (
	"errors"
	"fmt"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/httpx"
)

// deploymentCursor is the opaque payload embedded in the v1 cursor
// envelope for GET /api/v1/list/{appName}. TS is the deployments
// row's created_at (UTC); ID is the row's text PK (`d_<uuid>`).
// These two fields MUST travel together — a single-field cursor
// would lose the tiebreaker and silently skip rows that share a
// timestamp. Adding fields here requires bumping the cursor version
// on the httpx side and a migration in this file — keep this
// struct minimal.
type deploymentCursor struct {
	TS time.Time `json:"ts"`
	ID string    `json:"id"`
}

// Errors this codec can return. Chained via %w to the underlying
// httpx errors so errors.Is(err, service.ErrInvalidDeploymentCursor)
// matches the same error chain that httpx.EncodeCursor /
// httpx.DecodeCursor produce.
var (
	// ErrInvalidDeploymentCursor is returned when the supplied
	// cursor string is malformed: bad base64, malformed envelope,
	// zero version, payload decode failure, or the resource-level
	// zero-guards on (TS, ID) fire. Handlers map to 400 with the
	// generic "invalid cursor" message.
	ErrInvalidDeploymentCursor = fmt.Errorf("invalid deployment cursor: %w", httpx.ErrInvalidCursor)

	// ErrUnsupportedDeploymentCursorVersion is returned when the
	// cursor was produced by a newer server this reader does not
	// understand. Handlers also map to 400 — the client should
	// fetch a fresh cursor rather than reuse the unsupported one.
	ErrUnsupportedDeploymentCursorVersion = fmt.Errorf("unsupported deployment cursor version: %w", httpx.ErrUnsupportedCursorVersion)
)

// encodeDeploymentCursor wraps the (ts, id) tuple in the v1 envelope
// via httpx and returns a base64url string. Both TS and ID are
// required: zero values are rejected up front because they would
// either no-op the keyset predicate (zero TS) or violate the
// schema (empty ID, since deployments.id is the NOT NULL TEXT PK).
//
// TS is normalized to UTC so the wire is canonical regardless of
// the caller's local timezone — a Postgres TIMESTAMPTZ round-trip
// always lands in UTC and the cursor must match.
func encodeDeploymentCursor(ts time.Time, id string) (string, error) {
	if ts.IsZero() {
		return "", ErrInvalidDeploymentCursor
	}
	if id == "" {
		return "", ErrInvalidDeploymentCursor
	}
	return httpx.EncodeCursor(deploymentCursor{TS: ts.UTC(), ID: id})
}

// decodeDeploymentCursor parses s into the cursor payload and
// returns the embedded (ts, id). Errors are mapped to the
// service-level aliases so callers matching against
// service.ErrInvalidDeploymentCursor /
// service.ErrUnsupportedDeploymentCursorVersion succeed without
// having to import httpx.
//
// TS is normalized to UTC so the wire is canonical regardless of
// the caller's local timezone.
func decodeDeploymentCursor(s string) (time.Time, string, error) {
	var c deploymentCursor
	if err := httpx.DecodeCursor(s, &c); err != nil {
		if errors.Is(err, httpx.ErrUnsupportedCursorVersion) {
			return time.Time{}, "", ErrUnsupportedDeploymentCursorVersion
		}
		return time.Time{}, "", ErrInvalidDeploymentCursor
	}
	if c.TS.IsZero() {
		// A v1 envelope that decoded cleanly but carries a zero TS
		// is treated as malformed. encodeDeploymentCursor never
		// produces one; a hand-crafted or partial cursor shouldn't
		// slip through.
		return time.Time{}, "", ErrInvalidDeploymentCursor
	}
	if c.ID == "" {
		// Same reasoning for ID — an empty ID can never appear in
		// a real deployments row, so accepting one would silently
		// no-op the keyset predicate.
		return time.Time{}, "", ErrInvalidDeploymentCursor
	}
	return c.TS.UTC(), c.ID, nil
}