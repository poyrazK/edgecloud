package service

import (
	"errors"
	"testing"
	"time"
)

// TestEncodeDeploymentCursor_HappyPath round-trips a (ts, id) tuple
// through encode/decode. UTC normalization is verified — the input
// is in a non-UTC zone and the decoded value must be in UTC. Issue
// #709 — id is a text PK (`d_<uuid>`), not int64.
func TestEncodeDeploymentCursor_HappyPath(t *testing.T) {
	// Pick a fixed point so the encoded cursor is reproducible
	// across test runs (avoids flakiness from time.Now).
	wantTS := time.Date(2026, 7, 15, 12, 34, 56, 0, time.FixedZone("PDT", -7*3600))
	const wantID = "d_42"

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
		t.Errorf("ID round-trip: got %q, want %q", gotID, wantID)
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
	_, err := encodeDeploymentCursor(time.Time{}, "d_42")
	if !errors.Is(err, ErrInvalidDeploymentCursor) {
		t.Errorf("err = %v, want ErrInvalidDeploymentCursor", err)
	}
}

// TestEncodeDeploymentCursor_EmptyIDRejected pins the ID guard. The
// deployments.id column is TEXT (NOT NULL), so an empty id can never
// appear in a real row — accepting it would silently no-op the
// keyset predicate. Issue #709 swapped the codec from int64 to text.
func TestEncodeDeploymentCursor_EmptyIDRejected(t *testing.T) {
	ts := time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC)
	if _, err := encodeDeploymentCursor(ts, ""); !errors.Is(err, ErrInvalidDeploymentCursor) {
		t.Errorf("err = %v, want ErrInvalidDeploymentCursor", err)
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
	// {"v":1,"p":{"ts":"0001-01-01T00:00:00Z","id":"d_50"}}
	const bad = "eyJ2IjoxLCJwIjp7InRzIjoiMDAwMS0wMS0wMVQwMDowMDowMFoiLCJpZCI6ImRfNTAifX0"
	_, _, err := decodeDeploymentCursor(bad)
	if !errors.Is(err, ErrInvalidDeploymentCursor) {
		t.Errorf("err = %v, want ErrInvalidDeploymentCursor (zero TS payload)", err)
	}
}

// TestDecodeDeploymentCursor_EmptyIDInPayload — mirror of the
// zero-TS test, for the ID field. A v1 envelope with an empty id
// must chain to ErrInvalidDeploymentCursor. Issue #709 — id is
// text now, so the payload's id field is quoted in the JSON.
func TestDecodeDeploymentCursor_EmptyIDInPayload(t *testing.T) {
	// {"v":1,"p":{"ts":"2031-08-20T01:01:01Z","id":""}}
	const bad = "eyJ2IjoxLCJwIjp7InRzIjoiMjAzMS0wOC0yMFQwMTowMTowMVoiLCJpZCI6IiJ9fQ"
	_, _, err := decodeDeploymentCursor(bad)
	if !errors.Is(err, ErrInvalidDeploymentCursor) {
		t.Errorf("err = %v, want ErrInvalidDeploymentCursor (empty id payload)", err)
	}
}

// TestDecodeDeploymentCursor_UnsupportedVersion — a v2 envelope
// returns ErrUnsupportedDeploymentCursorVersion. The chain must
// satisfy both this alias and the underlying
// httpx.ErrUnsupportedCursorVersion.
func TestDecodeDeploymentCursor_UnsupportedVersion(t *testing.T) {
	// {"v":2,"p":{"ts":"2031-08-20T01:01:01Z","id":"d_50"}}
	const future = "eyJ2IjoyLCJwIjp7InRzIjoiMjAzMS0wOC0yMFQwMTowMTowMVoiLCJpZCI6ImRfNTAifX0"
	_, _, err := decodeDeploymentCursor(future)
	if !errors.Is(err, ErrUnsupportedDeploymentCursorVersion) {
		t.Errorf("err = %v, want ErrUnsupportedDeploymentCursorVersion", err)
	}
}
