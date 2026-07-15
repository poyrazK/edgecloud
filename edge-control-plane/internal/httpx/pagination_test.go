package httpx

import (
	"encoding/base64"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// TestEncodeDecodeRoundTrip covers the happy path: a single-string
// payload (apps cursor shape) goes in and comes back equal.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
	}
	in := payload{Name: "hello"}

	encoded, err := EncodeCursor(in)
	if err != nil {
		t.Fatalf("EncodeCursor: unexpected error: %v", err)
	}
	if encoded == "" {
		t.Fatal("EncodeCursor returned empty string")
	}
	// URL-safety: no '+', '/', or '=' allowed in the body.
	for _, c := range encoded {
		if c == '+' || c == '/' || c == '=' {
			t.Fatalf("encoded cursor %q contains non-URL-safe char %q", encoded, c)
		}
	}

	var out payload
	if err := DecodeCursor(encoded, &out); err != nil {
		t.Fatalf("DecodeCursor: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Errorf("round-trip mismatch: in=%+v out=%+v", in, out)
	}
}

// TestEncodeCursor_NilPayloadRejected pins the defensive guard
// against a caller passing nil.
func TestEncodeCursor_NilPayloadRejected(t *testing.T) {
	_, err := EncodeCursor(nil)
	if !errors.Is(err, ErrInvalidCursor) {
		t.Errorf("got err=%v, want ErrInvalidCursor", err)
	}
}

// TestEncodeCursor_DifferentPayloadTypesTableDriven ensures the
// helper works for both single-string cursors (apps) and strict-tuple
// cursors (deployments, logs, webhooks). A future refactor that
// pins the helper to a single payload shape should break this test.
func TestEncodeCursor_DifferentPayloadTypesTableDriven(t *testing.T) {
	type singleString struct {
		Name string `json:"name"`
	}
	type strictTuple struct {
		TS int64 `json:"ts"`
		ID int64 `json:"id"`
	}

	cases := []struct {
		name string
		in   any
	}{
		{"single_string_apps", singleString{Name: "alpha"}},
		{"strict_tuple_deployments", strictTuple{TS: 1, ID: 2}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := EncodeCursor(tc.in)
			if err != nil {
				t.Fatalf("EncodeCursor: %v", err)
			}
			// Decode back via the same anonymous struct type. Note
			// that DecodeCursor needs the same JSON tags, which
			// both cases provide; the helper decodes the payload
			// into whatever out pointer is supplied.
			switch v := tc.in.(type) {
			case singleString:
				var out singleString
				if err := DecodeCursor(encoded, &out); err != nil {
					t.Fatalf("DecodeCursor: %v", err)
				}
				if !reflect.DeepEqual(v, out) {
					t.Errorf("round-trip mismatch: %+v vs %+v", v, out)
				}
			case strictTuple:
				var out strictTuple
				if err := DecodeCursor(encoded, &out); err != nil {
					t.Fatalf("DecodeCursor: %v", err)
				}
				if !reflect.DeepEqual(v, out) {
					t.Errorf("round-trip mismatch: %+v vs %+v", v, out)
				}
			}
		})
	}
}

// TestDecodeCursor_BadBase64 covers all malformed-base64 collapse
// paths (invalid chars, padding markers). They MUST all map to
// ErrInvalidCursor, never ErrUnsupportedCursorVersion.
func TestDecodeCursor_BadBase64(t *testing.T) {
	cases := map[string]string{
		"not_base64":     "@@@not-base64@@@",
		"plus_chars":     "abc+def",
		"slash_chars":    "abc/def",
		"padding_marker": "abcd===",
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			var out struct{}
			err := DecodeCursor(raw, &out)
			if !errors.Is(err, ErrInvalidCursor) {
				t.Errorf("got err=%v, want ErrInvalidCursor", err)
			}
			if errors.Is(err, ErrUnsupportedCursorVersion) {
				t.Errorf("got ErrUnsupportedCursorVersion for malformed base64; should be ErrInvalidCursor")
			}
		})
	}
}

// TestDecodeCursor_EmptyString pins that an empty cursor string
// returns ErrInvalidCursor (treats it the same as malformed input).
func TestDecodeCursor_EmptyString(t *testing.T) {
	var out struct{}
	if err := DecodeCursor("", &out); !errors.Is(err, ErrInvalidCursor) {
		t.Errorf("got err=%v, want ErrInvalidCursor", err)
	}
}

// TestDecodeCursor_BadJSON pins that a base64 string that decodes
// cleanly but doesn't parse as the envelope shape returns
// ErrInvalidCursor.
func TestDecodeCursor_BadJSON(t *testing.T) {
	cases := map[string]string{
		"not_json":             base64.RawURLEncoding.EncodeToString([]byte("not-json-at-all")),
		"json_array_envelope":  base64.RawURLEncoding.EncodeToString([]byte(`[1, 2, 3]`)),
		"json_string_envelope": base64.RawURLEncoding.EncodeToString([]byte(`"a string"`)),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			var out struct{}
			err := DecodeCursor(raw, &out)
			if !errors.Is(err, ErrInvalidCursor) {
				t.Errorf("got err=%v, want ErrInvalidCursor", err)
			}
		})
	}
}

// TestDecodeCursor_ZeroVersion pins that a hand-crafted cursor with
// v=0 is treated as invalid (not as an unsupported version), per
// the DecodeCursor godoc.
func TestDecodeCursor_ZeroVersion(t *testing.T) {
	body := `{"v":0,"p":{}}`
	encoded := base64.RawURLEncoding.EncodeToString([]byte(body))

	var out struct{}
	err := DecodeCursor(encoded, &out)
	if !errors.Is(err, ErrInvalidCursor) {
		t.Errorf("got err=%v, want ErrInvalidCursor", err)
	}
	if errors.Is(err, ErrUnsupportedCursorVersion) {
		t.Errorf("got ErrUnsupportedCursorVersion for zero version; should be ErrInvalidCursor")
	}
}

// TestDecodeCursor_VersionTwo pins the unsupported-version path: a
// cursor from a future server (v=2) returns ErrUnsupportedCursorVersion
// — handlers map this to 400, not 500.
func TestDecodeCursor_VersionTwo(t *testing.T) {
	body := `{"v":2,"p":{}}`
	encoded := base64.RawURLEncoding.EncodeToString([]byte(body))

	var out struct{}
	err := DecodeCursor(encoded, &out)
	if !errors.Is(err, ErrUnsupportedCursorVersion) {
		t.Errorf("got err=%v, want ErrUnsupportedCursorVersion", err)
	}
	if errors.Is(err, ErrInvalidCursor) {
		t.Errorf("got ErrInvalidCursor for v=2; should be ErrUnsupportedCursorVersion (the v is recognized, just unsupported)")
	}
}

// TestDecodeCursor_BadPayload covers a well-formed envelope whose
// payload doesn't decode into the caller's out type.
func TestDecodeCursor_BadPayload(t *testing.T) {
	body := `{"v":1,"p":"not a struct"}`
	encoded := base64.RawURLEncoding.EncodeToString([]byte(body))

	type want struct {
		Number int `json:"number"`
	}
	var out want
	err := DecodeCursor(encoded, &out)
	if !errors.Is(err, ErrInvalidCursor) {
		t.Errorf("got err=%v, want ErrInvalidCursor", err)
	}
}

// TestEncodeCursor_RawURLSafety is a redundant-but-cheap check that
// an encoded cursor is purely URL-safe characters (RawURLEncoding
// alphabet plus no padding). Belt-and-suspenders alongside the
// per-test loop in TestEncodeDecodeRoundTrip.
func TestEncodeCursor_RawURLSafety(t *testing.T) {
	type p struct {
		PaddingMightTrigger string `json:"padding"`
	}
	in := p{PaddingMightTrigger: strings.Repeat("x", 7)}

	encoded, err := EncodeCursor(in)
	if err != nil {
		t.Fatalf("EncodeCursor: %v", err)
	}
	for _, c := range encoded {
		isAlphaNum := (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
		if !isAlphaNum && c != '-' && c != '_' {
			t.Fatalf("encoded cursor %q contains non-URL-safe char %q", encoded, c)
		}
	}
}
