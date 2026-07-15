package service

import (
	"errors"
	"strings"
	"testing"
)

// TestEncodeAppCursor_HappyPath round-trips a non-empty name through
// the encode/decode pair and verifies the same name comes back out.
// Pinning this here (rather than only in the httpx helper test)
// catches regressions where someone refactors encodeAppCursor or
// decodeAppCursor and accidentally drops the empty-name guard or
// changes the field name.
func TestEncodeAppCursor_HappyPath(t *testing.T) {
	const want = "hello"
	encoded, err := encodeAppCursor(want)
	if err != nil {
		t.Fatalf("encodeAppCursor(%q): %v", want, err)
	}
	if encoded == "" {
		t.Fatal("encoded cursor is empty")
	}
	// URL-safety: the helper docs promise no '+', '/', '='.
	for _, c := range encoded {
		if c == '+' || c == '/' || c == '=' {
			t.Fatalf("encoded cursor %q contains non-URL-safe char %q", encoded, c)
		}
	}

	got, err := decodeAppCursor(encoded)
	if err != nil {
		t.Fatalf("decodeAppCursor(%q): %v", encoded, err)
	}
	if got != want {
		t.Errorf("round-trip mismatch: in=%q out=%q", want, got)
	}
}

// TestEncodeAppCursor_EmptyNameRejected pins the defensive guard.
// encodeAppCursor must NOT emit a cursor that decodes back to an
// empty string because empty means "first page" — an encoded empty
// cursor would silently no-op the keyset predicate and the caller
// would never make forward progress.
func TestEncodeAppCursor_EmptyNameRejected(t *testing.T) {
	_, err := encodeAppCursor("")
	if !errors.Is(err, ErrInvalidAppCursor) {
		t.Errorf("got err=%v, want ErrInvalidAppCursor", err)
	}
}

// TestDecodeAppCursor_BadBase64 — a string with padding markers
// is not valid RawURLEncoding. The error must chain to
// ErrInvalidAppCursor so handlers map to 400 with "invalid cursor".
func TestDecodeAppCursor_BadBase64(t *testing.T) {
	_, err := decodeAppCursor("abcd===")
	if !errors.Is(err, ErrInvalidAppCursor) {
		t.Errorf("got err=%v, want ErrInvalidAppCursor", err)
	}
}

// TestDecodeAppCursor_BadJSON — a base64 string that decodes
// cleanly but isn't a JSON envelope must chain to ErrInvalidAppCursor.
// Pins that the helper's "v1 envelope" gate is enforced even when
// the input passes base64 validation.
func TestDecodeAppCursor_BadJSON(t *testing.T) {
	// "not-json-at-all" is a perfectly valid RawURLEncoding string
	// of ASCII text — base64 decode succeeds but JSON parse fails.
	encoded := "bm90LWpzb24tYXQtYWxs"
	if _, err := decodeAppCursor(encoded); !errors.Is(err, ErrInvalidAppCursor) {
		t.Errorf("got err=%v, want ErrInvalidAppCursor", err)
	}
}

// TestDecodeAppCursor_EmptyPayload — a v1 envelope with a missing
// or empty Name field. The httpx.DecodeCursor call succeeds (the
// envelope itself is well-formed), but this file's decodeAppCursor
// must still reject an empty Name with ErrInvalidAppCursor.
//
// Built by hand to mirror the scenario where someone hand-crafts a
// cursor and forgets the field, or a future bug lets
// httpx.EncodeCursor through with a zero payload.
func TestDecodeAppCursor_EmptyPayload(t *testing.T) {
	// {"v":1,"p":{"name":""}} — valid envelope, empty Name.
	const bad = "eyJ2IjoxLCJwIjp7Im5hbWUiOiIifX0"
	_, err := decodeAppCursor(bad)
	if !errors.Is(err, ErrInvalidAppCursor) {
		t.Errorf("got err=%v, want ErrInvalidAppCursor (empty name payload)", err)
	}
}

// TestDecodeAppCursor_UnsupportedVersion — a v2 envelope returns
// ErrUnsupportedAppCursorVersion. The chain must satisfy both this
// alias and the underlying httpx.ErrUnsupportedCursorVersion, so a
// handler can match either name.
func TestDecodeAppCursor_UnsupportedVersion(t *testing.T) {
	// {"v":2,"p":{"name":"hello"}}
	const future = "eyJ2IjoyLCJwIjp7Im5hbWUiOiJoZWxsbyJ9fQ"
	_, err := decodeAppCursor(future)
	if !errors.Is(err, ErrUnsupportedAppCursorVersion) {
		t.Errorf("got err=%v, want ErrUnsupportedAppCursorVersion", err)
	}
}

// TestEncodeAppCursor_OutputIsBase64UrlAlphabet pins the alphabet.
// Defensive: if a future refactor accidentally swaps to
// base64.StdEncoding, the round-trip tests above would still pass
// because StdEncoding is also decodable by RawURLEncoding — but the
// handler would emit '+' / '/' / '=' characters that break
// URL-encoded query strings.
func TestEncodeAppCursor_OutputIsBase64UrlAlphabet(t *testing.T) {
	encoded, err := encodeAppCursor(strings.Repeat("z", 7))
	if err != nil {
		t.Fatalf("encodeAppCursor: %v", err)
	}
	for _, c := range encoded {
		isAlphaNum := (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
		if !isAlphaNum && c != '-' && c != '_' {
			t.Fatalf("encoded cursor %q contains non-URL-safe char %q", encoded, c)
		}
	}
}
