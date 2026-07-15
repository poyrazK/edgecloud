package service

import (
	"errors"
	"testing"
	"time"
)

// TestEncodeDeploymentCursor_HappyPath round-trips a (ts, id) tuple
// through encode/decode. UTC normalization is verified — the input
// is in a non-UTC zone and the decoded value must be in UTC.
func TestEncodeDeploymentCursor_HappyPath(t *testing.T) {
	// Pick a fixed point so the encoded cursor is reproducible
	// across test runs (avoids flakiness from time.Now).
	wantTS := time.Date(2026, 7, 15, 12, 34, 56, 0, time.FixedZone("PDT", -7*3600))
	const wantID int64 = 42

	encoded, err := encodeDeploymentCursor(wantTS, wantID)
	if err != nil {
		t.Fatalf("encodeDeploymentCursor: %v", err)
	}
	if encoded == "" {
		t.Fatal("encoded cursor is empty")
	}

	gotTS, gotID, err := decodeDeploymentCursor(encoded)
	if err != nil {
		t.Fatalf("decodeDeploymentCursor(%q): %v", encoded, err)
	}
	if gotID != wantID {
		t.Errorf("ID round-trip: got %d, want %d", gotID, wantID)
	}
	// TS round-trip: the wire is UTC, the input was PDT. The instant
	// must match; only the Location differs.
	if !gotTS.Equal(wantTS.UTC()) {
		t.Errorf("TS round-trip: got %v, want %v (input was %v PDT)", gotTS, wantTS.UTC(), wantTS)
	}
	if gotTS.Location() != time.UTC {
		t.Errorf("decoded TS.Location = %v, want UTC", gotTS.Location())
	}
}

// TestEncodeDeploymentCursor_ZeroTSRejected pins the defensive
// guard against a zero time.Time. A zero TS would silently no-op
// the keyset predicate (every row's created_at > 0001-01-01).
func TestEncodeDeploymentCursor_ZeroTSRejected(t *testing.T) {
	_, err := encodeDeploymentCursor(time.Time{}, 42)
	if !errors.Is(err, ErrInvalidDeploymentCursor) {
		t.Errorf("err = %v, want ErrInvalidDeploymentCursor", err)
	}
}

// TestEncodeDeploymentCursor_NonPositiveIDRejected pins the ID
// guard. deployments.id is BIGSERIAL so a non-positive value can
// never appear in a real row.
func TestEncodeDeploymentCursor_NonPositiveIDRejected(t *testing.T) {
	ts := time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC)
	cases := []int64{0, -1, -42}
	for _, id := range cases {
		_, err := encodeDeploymentCursor(ts, id)
		if !errors.Is(err, ErrInvalidDeploymentCursor) {
			t.Errorf("id=%d: err=%v, want ErrInvalidDeploymentCursor", id, err)
		}
	}
}

// TestDecodeDeploymentCursor_BadBase64 — a string with padding
// markers is not valid RawURLEncoding. The error must chain to
// ErrInvalidDeploymentCursor so handlers map to 400 with "invalid
// cursor".
func TestDecodeDeploymentCursor_BadBase64(t *testing.T) {
	_, _, err := decodeDeploymentCursor("abcd===")
	if !errors.Is(err, ErrInvalidDeploymentCursor) {
		t.Errorf("err = %v, want ErrInvalidDeploymentCursor", err)
	}
}

// TestDecodeDeploymentCursor_BadJSON — a base64 string that decodes
// cleanly but isn't a JSON envelope must chain to
// ErrInvalidDeploymentCursor.
func TestDecodeDeploymentCursor_BadJSON(t *testing.T) {
	// "not-json-at-all" — valid RawURLEncoding, invalid JSON.
	encoded := "bm90LWpzb24tYXQtYWxs"
	_, _, err := decodeDeploymentCursor(encoded)
	if !errors.Is(err, ErrInvalidDeploymentCursor) {
		t.Errorf("err = %v, want ErrInvalidDeploymentCursor", err)
	}
}

// TestDecodeDeploymentCursor_ZeroTSInPayload — a v1 envelope with
// a zero TS in the payload. httpx.DecodeCursor succeeds (the
// envelope is well-formed), but this file's decodeDeploymentCursor
// must still reject it with ErrInvalidDeploymentCursor.
func TestDecodeDeploymentCursor_ZeroTSInPayload(t *testing.T) {
	// {"v":1,"p":{"ts":"0001-01-01T00:00:00Z","id":42}}
	const bad = "eyJ2IjoxLCJwIjp7InRzIjoiMDAwMS0wMS0wMVQwMDowMDowMFoiLCJpZCI6NDJ9fQ"
	_, _, err := decodeDeploymentCursor(bad)
	if !errors.Is(err, ErrInvalidDeploymentCursor) {
		t.Errorf("err = %v, want ErrInvalidDeploymentCursor (zero TS payload)", err)
	}
}

// TestDecodeDeploymentCursor_NonPositiveIDInPayload — mirror of the
// zero-TS test, for the ID field. A v1 envelope with id<=0 must
// chain to ErrInvalidDeploymentCursor.
func TestDecodeDeploymentCursor_NonPositiveIDInPayload(t *testing.T) {
	// {"v":1,"p":{"ts":"2026-07-15T12:34:56Z","id":0}}
	const bad = "eyJ2IjoxLCJwIjp7InRzIjoiMjAyNi0wNy0xNVQxMjozNDo1NloiLCJpZCI6MH19"
	_, _, err := decodeDeploymentCursor(bad)
	if !errors.Is(err, ErrInvalidDeploymentCursor) {
		t.Errorf("err = %v, want ErrInvalidDeploymentCursor (id=0 payload)", err)
	}
}

// TestDecodeDeploymentCursor_UnsupportedVersion — a v2 envelope
// returns ErrUnsupportedDeploymentCursorVersion. The chain must
// satisfy both this alias and the underlying
// httpx.ErrUnsupportedCursorVersion.
func TestDecodeDeploymentCursor_UnsupportedVersion(t *testing.T) {
	// {"v":2,"p":{"ts":"2026-07-15T12:34:56Z","id":42}}
	const future = "eyJ2IjoyLCJwIjp7InRzIjoiMjAyNi0wNy0xNVQxMjozNDo1NloiLCJpZCI6NDJ9fQ"
	_, _, err := decodeDeploymentCursor(future)
	if !errors.Is(err, ErrUnsupportedDeploymentCursorVersion) {
		t.Errorf("err = %v, want ErrUnsupportedDeploymentCursorVersion", err)
	}
}
