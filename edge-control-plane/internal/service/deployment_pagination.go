// deployment_pagination.go (issue #58) — deployments-list cursor codec.
//
// The deployments list orders on (created_at DESC, id DESC) and the
// keyset predicate is (created_at, id) < ($afterTS, $afterID). The
// cursor payload is therefore a strict-tuple — a single string is
// not enough because two deployments can share a second-precision
// timestamp and the (id DESC) tiebreaker is load-bearing.
//
// Mirrors the per-resource codec shape at internal/service/
// webhook_delivery_cursor.go — same v1 envelope, same RawURLEncoding
// base64url, same typed-error contract — but delegates the encode
// and decode primitives to internal/httpx so every list endpoint
// shares the wire format.
//
// Resource-specific zero-guards:
//
//   - TS must NOT be the zero time.Time (Go's encoding/json renders
//     it as "0001-01-01T00:00:00Z"). encodeDeploymentCursor rejects
//     zero values up front; decodeDeploymentCursor rejects what
//     slips through hand-crafted cursors.
//
//   - ID must be > 0. ID is the deployments PK (SERIAL), so a zero
//     value can never appear in a real row — but accepting it would
//     silently no-op the keyset predicate.
//
// Both encode and decode force UTC: timestamps in the cursor are
// stored without a tz suffix by json.Marshal, so a naive parse round-
// trip can drift if the caller is in a non-UTC tz. We .UTC() both
// directions to keep the wire canonical.
//
// Issue #58 dual-envelope: deployments keeps ?offset= + next_offset
// for one compat release so the existing CLI's --page flag still
// works against the new server. The follow-up to drop those lives
// in #58-followup (modeled on issue #682, the logs offset-retire
// follow-up).

package service

import (
	"errors"
	"fmt"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/httpx"
)

// deploymentCursor is the opaque payload embedded in the v1 cursor
// envelope for GET /api/v1/list/{appName}. TS is the deployments
// row's created_at (UTC); ID is the row's BIGSERIAL pk. The
// keyset predicate at the SQL layer is `(created_at, id) < ($3, $4)`,
// so these two fields MUST travel together — a single-field cursor
// would lose the tiebreaker and silently skip rows that share a
// timestamp.
type deploymentCursor struct {
	TS time.Time `json:"ts"`
	ID int64     `json:"id"`
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
// schema (zero ID, since deployments.id is BIGSERIAL).
//
// TS is normalized to UTC so the wire is canonical regardless of
// the caller's local timezone — a Postgres TIMESTAMPTZ round-trip
// always lands in UTC and the cursor must match.
func encodeDeploymentCursor(ts time.Time, id int64) (string, error) {
	if ts.IsZero() {
		return "", ErrInvalidDeploymentCursor
	}
	if id <= 0 {
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
func decodeDeploymentCursor(s string) (time.Time, int64, error) {
	var c deploymentCursor
	if err := httpx.DecodeCursor(s, &c); err != nil {
		if errors.Is(err, httpx.ErrUnsupportedCursorVersion) {
			return time.Time{}, 0, ErrUnsupportedDeploymentCursorVersion
		}
		return time.Time{}, 0, ErrInvalidDeploymentCursor
	}
	if c.TS.IsZero() {
		// A v1 envelope that decoded cleanly but carries a zero TS
		// is treated as malformed. encodeDeploymentCursor never
		// produces one; a hand-crafted or partial cursor shouldn't
		// slip through.
		return time.Time{}, 0, ErrInvalidDeploymentCursor
	}
	if c.ID <= 0 {
		// Same reasoning for ID — a zero or negative ID can never
		// appear in a real deployments row, so accepting one would
		// silently no-op the keyset predicate.
		return time.Time{}, 0, ErrInvalidDeploymentCursor
	}
	return c.TS.UTC(), c.ID, nil
}
