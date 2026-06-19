package hashutil

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func TestSHA256Hex_KnownVectors(t *testing.T) {
	// NIST test vectors: SHA256("") and SHA256("abc").
	cases := []struct {
		input string
		want  string
	}{
		{"", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{"the quick brown fox jumps over the lazy dog",
			"05c6e08f1d9fdafa03147fcb8f82f124c76d2f70e3d989dc8aadb5e7d7450bec"},
	}
	for _, c := range cases {
		got := SHA256Hex(c.input)
		if got != c.want {
			t.Errorf("SHA256Hex(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

func TestSHA256Hex_MatchesStdlib(t *testing.T) {
	// The point of the package: it must produce identical bytes to a
	// hand-rolled crypto/sha256 + encoding/hex invocation. If this test
	// fails the contract that lets middleware and service share the
	// helper is broken.
	inputs := []string{"", "x", strings.Repeat("a", 64), "with\nnewlines\nand\ttabs"}
	for _, in := range inputs {
		h := sha256.Sum256([]byte(in))
		want := hex.EncodeToString(h[:])
		if got := SHA256Hex(in); got != want {
			t.Errorf("SHA256Hex(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSHA256Hex_Lowercase(t *testing.T) {
	got := SHA256Hex("any-input")
	for _, c := range got {
		if c >= 'A' && c <= 'F' {
			t.Errorf("SHA256Hex returned uppercase hex: %q", got)
			break
		}
	}
}
