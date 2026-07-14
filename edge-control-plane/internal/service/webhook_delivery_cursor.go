package service

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrInvalidWebhookDeliveryCursor is returned when a webhook delivery
	// cursor cannot be decoded or does not contain a valid timestamp and
	// delivery ID.
	ErrInvalidWebhookDeliveryCursor = errors.New("invalid webhook delivery cursor")
	// ErrUnsupportedWebhookDeliveryCursorVersion is returned when the
	// cursor was produced by a newer, unsupported cursor contract.
	ErrUnsupportedWebhookDeliveryCursorVersion = errors.New("unsupported webhook delivery cursor version")
)

const webhookDeliveryCursorVersion = 1

// webhookDeliveryCursor is the private v1 JSON payload encoded into the
// opaque base64 string returned as `next_cursor` from
// GET /api/v1/webhooks/{id}/deliveries. The (ts, id) pair keyset matches
// the repository's strict-tuple predicate
// `AND (created_at, id) < ($N::timestamptz, $N::bigint)` and the existing
// composite index idx_webhook_deliveries_webhook (webhook_id, created_at
// DESC) from migration 015 — no new migration is required.
type webhookDeliveryCursor struct {
	Version int       `json:"v"`
	TS      time.Time `json:"ts"`
	ID      int64     `json:"id"`
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
	payload, err := json.Marshal(webhookDeliveryCursor{
		Version: webhookDeliveryCursorVersion,
		TS:      ts.UTC(),
		ID:      id,
	})
	if err != nil {
		return "", fmt.Errorf("encoding webhook delivery cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

// decodeWebhookDeliveryCursor parses the opaque base64 cursor string
// supplied by a client back into (created_at, id). Returns
// ErrUnsupportedWebhookDeliveryCursorVersion when the v field is not 1
// and ErrInvalidWebhookDeliveryCursor for any other parse failure
// (malformed base64, malformed JSON, zero ts, non-positive id). The
// returned timestamp is normalized to UTC.
func decodeWebhookDeliveryCursor(raw string) (time.Time, int64, error) {
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return time.Time{}, 0, ErrInvalidWebhookDeliveryCursor
	}

	var cursor webhookDeliveryCursor
	if err := json.Unmarshal(payload, &cursor); err != nil {
		return time.Time{}, 0, ErrInvalidWebhookDeliveryCursor
	}
	if cursor.Version != webhookDeliveryCursorVersion {
		return time.Time{}, 0, ErrUnsupportedWebhookDeliveryCursorVersion
	}
	if cursor.TS.IsZero() || cursor.ID <= 0 {
		return time.Time{}, 0, ErrInvalidWebhookDeliveryCursor
	}
	return cursor.TS.UTC(), cursor.ID, nil
}
