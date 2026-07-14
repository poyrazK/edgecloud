package service

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestLogCursorRoundTrip(t *testing.T) {
	ts := time.Date(2026, 7, 14, 12, 34, 56, 123456789, time.FixedZone("offset", 3600))
	encoded, err := encodeLogCursor(ts, 42)
	if err != nil {
		t.Fatalf("encodeLogCursor: %v", err)
	}
	if strings.ContainsAny(encoded, "+/=") {
		t.Fatalf("cursor is not unpadded base64url: %q", encoded)
	}

	gotTS, gotID, err := decodeLogCursor(encoded)
	if err != nil {
		t.Fatalf("decodeLogCursor: %v", err)
	}
	if !gotTS.Equal(ts) || gotTS.Location() != time.UTC {
		t.Errorf("timestamp = %v (%v), want %v in UTC", gotTS, gotTS.Location(), ts)
	}
	if gotID != 42 {
		t.Errorf("id = %d, want 42", gotID)
	}
}

func TestDecodeLogCursorRejectsMalformedValues(t *testing.T) {
	cases := []string{
		"not base64url!",
		base64.RawURLEncoding.EncodeToString([]byte("not json")),
		encodeCursorPayload(t, map[string]any{"v": 1, "ts": time.Time{}, "id": 1}),
		encodeCursorPayload(t, map[string]any{"v": 1, "ts": time.Now(), "id": 0}),
		encodeCursorPayload(t, map[string]any{"v": 1, "ts": time.Now(), "id": -1}),
	}
	for _, raw := range cases {
		if _, _, err := decodeLogCursor(raw); !errors.Is(err, ErrInvalidLogCursor) {
			t.Errorf("decodeLogCursor(%q) error = %v, want ErrInvalidLogCursor", raw, err)
		}
	}
}

func TestDecodeLogCursorRejectsUnknownVersion(t *testing.T) {
	raw := encodeCursorPayload(t, map[string]any{"v": 2, "ts": time.Now(), "id": 1})
	if _, _, err := decodeLogCursor(raw); !errors.Is(err, ErrUnsupportedLogCursorVersion) {
		t.Fatalf("error = %v, want ErrUnsupportedLogCursorVersion", err)
	}
}

func TestEncodeLogCursorRejectsInvalidKey(t *testing.T) {
	if _, err := encodeLogCursor(time.Time{}, 1); !errors.Is(err, ErrInvalidLogCursor) {
		t.Errorf("zero timestamp error = %v, want ErrInvalidLogCursor", err)
	}
	if _, err := encodeLogCursor(time.Now(), 0); !errors.Is(err, ErrInvalidLogCursor) {
		t.Errorf("zero id error = %v, want ErrInvalidLogCursor", err)
	}
}

func encodeCursorPayload(t *testing.T, payload any) string {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(data)
}
