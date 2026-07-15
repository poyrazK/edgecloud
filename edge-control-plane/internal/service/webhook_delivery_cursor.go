package service

import (
	"errors"
	"fmt"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/httpx"
)

// Issue #58 / #709: webhook delivery cursor codec is now a thin
// resource-level wrapper around the shared `httpx.EncodeCursor` /
// `httpx.DecodeCursor` helpers. The wire format is unchanged —
// v1 envelope, base64url (RawURLEncoding, no padding) — but the
// base64 / JSON plumbing lives in `internal/httpx/pagination.go`
// alongside every other list-endpoint codec.
//
// Resource-specific concerns kept here:
//   - Zero-guard on encode: a zero ts or non-positive id is
//     rejected before any JSON marshal happens (defends against
//     a service bug constructing a cursor from a partial row).
//   - Typed-error aliases: callers (service + handler tests)
//     compare against `ErrInvalidWebhookDeliveryCursor` /
//     `ErrUnsupportedWebhookDeliveryCursorVersion`. Both alias
//     the httpx sentinels via `fmt.Errorf("...: %w", ...)` so
//     `errors.Is` keeps working through the chain.

var (
	// ErrInvalidWebhookDeliveryCursor is returned when a webhook delivery
	// cursor cannot be decoded or does not contain a valid timestamp and
	// delivery ID. Aliases httpx.ErrInvalidCursor via wrap so
	// `errors.Is(err, httpx.ErrInvalidCursor)` AND
	// `errors.Is(err, ErrInvalidWebhookDeliveryCursor)` both match.
	ErrInvalidWebhookDeliveryCursor = fmt.Errorf("invalid webhook delivery cursor: %w", httpx.ErrInvalidCursor)
	// ErrUnsupportedWebhookDeliveryCursorVersion is returned when the
	// cursor was produced by a newer, unsupported cursor contract.
	// Aliases httpx.ErrUnsupportedCursorVersion.
	ErrUnsupportedWebhookDeliveryCursorVersion = fmt.Errorf("unsupported webhook delivery cursor version: %w", httpx.ErrUnsupportedCursorVersion)
)

// webhookDeliveryCursor is the private v1 JSON payload carried in the
// `p` field of the httpx envelope. The (ts, id) pair keyset matches
// the repository's strict-tuple predicate
// `AND (created_at, id) < ($N::timestamptz, $N::bigint)` and the existing
// composite index idx_webhook_deliveries_webhook (webhook_id, created_at
// DESC) from migration 015 — no new migration is required.
type webhookDeliveryCursor struct {
	TS time.Time `json:"ts"`
	ID int64     `json:"id"`
}

// encodeWebhookDeliveryCursor serializes the (created_at, id) of the last
// visible delivery into the opaque cursor string returned to the client.
// Returns ErrInvalidWebhookDeliveryCursor on a zero ts or non-positive id
// — a defensive guard against an in-process caller constructing a cursor
// from a partial row.
func encodeWebhookDeliveryCursor(ts time.Time, id int64) (string, error) {
	if ts.IsZero() || id <= 0 {
		return "", ErrInvalidWebhookDeliveryCursor
	}
	return httpx.EncodeCursor(webhookDeliveryCursor{
		TS: ts.UTC(),
		ID: id,
	})
}

// decodeWebhookDeliveryCursor parses the opaque base64 cursor string
// supplied by a client back into (created_at, id). Returns
// ErrUnsupportedWebhookDeliveryCursorVersion when the envelope's v
// field is not 1 and ErrInvalidWebhookDeliveryCursor for any other
// parse failure (malformed base64, malformed JSON, zero ts,
// non-positive id). The returned timestamp is normalized to UTC.
func decodeWebhookDeliveryCursor(raw string) (time.Time, int64, error) {
	var cursor webhookDeliveryCursor
	if err := httpx.DecodeCursor(raw, &cursor); err != nil {
		switch {
		case errors.Is(err, httpx.ErrUnsupportedCursorVersion):
			return time.Time{}, 0, ErrUnsupportedWebhookDeliveryCursorVersion
		default:
			return time.Time{}, 0, ErrInvalidWebhookDeliveryCursor
		}
	}
	if cursor.TS.IsZero() || cursor.ID <= 0 {
		return time.Time{}, 0, ErrInvalidWebhookDeliveryCursor
	}
	return cursor.TS.UTC(), cursor.ID, nil
}
