package service

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestWebhookDeliveryCursorRoundTrip pins the codec contract: encoding
// then decoding returns the same (ts, id), and the encoded form is
// URL-safe (no '+', '/', or '=' padding characters — matches the
// logs cursor at logs_cursor_test.go:18-20).
func TestWebhookDeliveryCursorRoundTrip(t *testing.T) {
	ts := time.Date(2026, 7, 14, 12, 34, 56, 123456789, time.FixedZone("offset", 3600))
	encoded, err := encodeWebhookDeliveryCursor(ts, 42)
	if err != nil {
		t.Fatalf("encodeWebhookDeliveryCursor: %v", err)
	}
	if strings.ContainsAny(encoded, "+/=") {
		t.Fatalf("cursor is not unpadded base64url: %q", encoded)
	}

	gotTS, gotID, err := decodeWebhookDeliveryCursor(encoded)
	if err != nil {
		t.Fatalf("decodeWebhookDeliveryCursor: %v", err)
	}
	if !gotTS.Equal(ts) || gotTS.Location() != time.UTC {
		t.Errorf("timestamp = %v (%v), want %v in UTC", gotTS, gotTS.Location(), ts)
	}
	if gotID != 42 {
		t.Errorf("id = %d, want 42", gotID)
	}
}

// TestDecodeWebhookDeliveryCursorRejectsMalformedValues mirrors the
// logs cursor's rejection table — every entry must come back with
// ErrInvalidWebhookDeliveryCursor, never a raw decode error. The wire
// response collapses all of these to a generic "invalid cursor" 400
// (handler maps to 400 + structured log.Printf), so the typed error
// only needs to identify "broken" vs "unsupported version".
func TestDecodeWebhookDeliveryCursorRejectsMalformedValues(t *testing.T) {
	cases := []string{
		"not base64url!",
		base64.RawURLEncoding.EncodeToString([]byte("not json")),
		encodeWebhookDeliveryCursorPayload(t, map[string]any{"v": 1, "ts": time.Time{}, "id": 1}),
		encodeWebhookDeliveryCursorPayload(t, map[string]any{"v": 1, "ts": time.Now(), "id": 0}),
		encodeWebhookDeliveryCursorPayload(t, map[string]any{"v": 1, "ts": time.Now(), "id": -1}),
	}
	for _, raw := range cases {
		if _, _, err := decodeWebhookDeliveryCursor(raw); !errors.Is(err, ErrInvalidWebhookDeliveryCursor) {
			t.Errorf("decodeWebhookDeliveryCursor(%q) error = %v, want ErrInvalidWebhookDeliveryCursor", raw, err)
		}
	}
}

// TestDecodeWebhookDeliveryCursorRejectsUnknownVersion pins that a
// newer-version cursor returns the typed "unsupported version" error
// — distinct from "malformed" so the operator log can distinguish a
// broken client from a client speaking a future protocol.
func TestDecodeWebhookDeliveryCursorRejectsUnknownVersion(t *testing.T) {
	raw := encodeWebhookDeliveryCursorPayload(t, map[string]any{"v": 2, "ts": time.Now(), "id": 1})
	if _, _, err := decodeWebhookDeliveryCursor(raw); !errors.Is(err, ErrUnsupportedWebhookDeliveryCursorVersion) {
		t.Fatalf("error = %v, want ErrUnsupportedWebhookDeliveryCursorVersion", err)
	}
}

// TestEncodeWebhookDeliveryCursorRejectsInvalidKey pins the in-process
// guard: encoding refuses a zero ts or non-positive id before any
// JSON marshal happens. Without this, a service bug that passes a
// partial row could leak a cursor that decodes back to a zero ts and
// trips every subsequent SQL comparison.
func TestEncodeWebhookDeliveryCursorRejectsInvalidKey(t *testing.T) {
	if _, err := encodeWebhookDeliveryCursor(time.Time{}, 1); !errors.Is(err, ErrInvalidWebhookDeliveryCursor) {
		t.Errorf("zero timestamp error = %v, want ErrInvalidWebhookDeliveryCursor", err)
	}
	if _, err := encodeWebhookDeliveryCursor(time.Now(), 0); !errors.Is(err, ErrInvalidWebhookDeliveryCursor) {
		t.Errorf("zero id error = %v, want ErrInvalidWebhookDeliveryCursor", err)
	}
	if _, err := encodeWebhookDeliveryCursor(time.Now(), -1); !errors.Is(err, ErrInvalidWebhookDeliveryCursor) {
		t.Errorf("negative id error = %v, want ErrInvalidWebhookDeliveryCursor", err)
	}
}

// encodeWebhookDeliveryCursorPayload is a test-only helper that
// constructs a valid base64url-encoded JSON cursor body from a raw
// payload map. Used to construct intentionally-broken cursors that
// the production encoder would refuse to produce (zero ts, etc.).
func encodeWebhookDeliveryCursorPayload(t *testing.T, payload any) string {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(data)
}
