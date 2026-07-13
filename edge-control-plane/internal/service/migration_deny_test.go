package service

import (
	"os"
	"testing"
)

// Unit tests for the Go-side C deny-list pre-pass (issue #622
// commit 2). Mirrors the Rust deny tests in
// `edge-migrate/edge-migrate-lib/src/analyzer.rs::deny_tests`.
// The two implementations are pinned to the same fixtures so a
// drift in the policy between languages surfaces as a test
// failure on both sides.

func TestIsDenylistedCPath(t *testing.T) {
	cases := []struct {
		name string
		path string
		want bool
	}{
		{"absolute_posix", "/etc/passwd", true},
		{"absolute_windows_separator", `\/etc/passwd`, true},
		{"drive_letter", `C:\Users\admin\signing.key`, true},
		{"traversal_parent", "../foo", true},
		{"traversal_mid_path", "foo/../bar", true},
		{"traversal_backslash", `..\foo`, true},
		{"empty", "", false},
		{"relative_plain", "stdio.h", false},
		{"relative_quoted", "relative.h", false},
		{"relative_nested_safe", "include/wasi/sockets.h", false},
		{"looks_like_traversal_but_safe", "..a", false},
		{"looks_like_traversal_but_safe_2", "a..b", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isDenylistedCPath(tc.path)
			if got != tc.want {
				t.Errorf("isDenylistedCPath(%q) = %v, want %v", tc.path, got, tc.want)
			}
		})
	}
}

func TestCDenyErrors_AbsoluteQuoted(t *testing.T) {
	src := `#include "/etc/edgecloud/signing.key"
int main(void) { return 0; }
`
	path := writeTempSource(t, src)
	errs := cDenyErrors(path)
	if len(errs) != 1 {
		t.Fatalf("expected 1 deny error, got %d: %+v", len(errs), errs)
	}
	if errs[0].Code != denyCodePrefix+denyCodeCInclude {
		t.Errorf("expected code=%s%s, got %q",
			denyCodePrefix, denyCodeCInclude, errs[0].Code)
	}
	if errs[0].Line != 1 {
		t.Errorf("expected line=1 (single-line source), got %d", errs[0].Line)
	}
}

func TestCDenyErrors_AbsoluteAngled(t *testing.T) {
	src := "#include </etc/passwd>\n"
	path := writeTempSource(t, src)
	errs := cDenyErrors(path)
	if len(errs) != 1 {
		t.Fatalf("expected 1 deny error, got %d: %+v", len(errs), errs)
	}
	if errs[0].Code != denyCodePrefix+denyCodeCInclude {
		t.Errorf("expected code=%s%s, got %q", denyCodePrefix, denyCodeCInclude, errs[0].Code)
	}
}

func TestCDenyErrors_TraversalDoubleDot(t *testing.T) {
	src := "#include \"../../etc/shadow\"\n"
	path := writeTempSource(t, src)
	errs := cDenyErrors(path)
	if len(errs) != 1 {
		t.Fatalf("expected 1 deny error, got %d: %+v", len(errs), errs)
	}
	if errs[0].Code != denyCodePrefix+denyCodeCInclude {
		t.Errorf("expected code=%s%s, got %q", denyCodePrefix, denyCodeCInclude, errs[0].Code)
	}
}

func TestCDenyErrors_TraversalAngled(t *testing.T) {
	src := "#include <../stdlib.h>\n"
	path := writeTempSource(t, src)
	errs := cDenyErrors(path)
	if len(errs) != 1 {
		t.Fatalf("expected 1 deny error, got %d: %+v", len(errs), errs)
	}
	if errs[0].Code != denyCodePrefix+denyCodeCInclude {
		t.Errorf("expected code=%s%s, got %q", denyCodePrefix, denyCodeCInclude, errs[0].Code)
	}
}

func TestCDenyErrors_AnyEmbed(t *testing.T) {
	src := "#embed \"/etc/passwd\"\n"
	path := writeTempSource(t, src)
	errs := cDenyErrors(path)
	if len(errs) != 1 {
		t.Fatalf("expected 1 deny error, got %d: %+v", len(errs), errs)
	}
	if errs[0].Code != denyCodePrefix+denyCodeCInclude {
		t.Errorf("expected code=%s%s, got %q", denyCodePrefix, denyCodeCInclude, errs[0].Code)
	}
}

func TestCDenyErrors_RelativeEmbedAlsoCaught(t *testing.T) {
	src := "#embed \"data.bin\"\n"
	path := writeTempSource(t, src)
	errs := cDenyErrors(path)
	if len(errs) != 1 {
		t.Fatalf("expected 1 deny error, got %d: %+v", len(errs), errs)
	}
}

func TestCDenyErrors_MultipleSites(t *testing.T) {
	src := `#include <stdio.h>
#include "/etc/passwd"
#include "../shadow"
#include <wasi/sockets.h>
#embed "config.json"
`
	path := writeTempSource(t, src)
	errs := cDenyErrors(path)
	// Three denies: absolute + traversal + embed; the stdio / wasi
	// includes are allowed.
	if len(errs) != 3 {
		t.Fatalf("expected 3 deny errors, got %d: %+v", len(errs), errs)
	}
	for _, e := range errs {
		if e.Code != denyCodePrefix+denyCodeCInclude {
			t.Errorf("expected code=%s%s, got %q", denyCodePrefix, denyCodeCInclude, e.Code)
		}
	}
}

func TestCDenyErrors_Negatives(t *testing.T) {
	cases := []struct {
		name string
		src  string
	}{
		{"stdio_h", "#include <stdio.h>\n"},
		{"relative_quoted", "#include \"relative.h\"\n"},
		{"wasi_socket", "#include <wasi/sockets.h>\n"},
		{"string_literal_with_include", `printf("would have been #include if unquoted")`},
		{"comment_with_include", `// #include "/etc/passwd"`},
		{"include_macro_argument", "#include MY_HEADER\n"},
		{"no_includes", "int main(void) { return 0; }\n"},
		{"empty", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTempSource(t, tc.src)
			errs := cDenyErrors(path)
			if len(errs) != 0 {
				t.Errorf("expected no deny errors for benign source, got %+v", errs)
			}
		})
	}
}

func TestCDenyErrors_WhitespaceBetweenHashAndInclude(t *testing.T) {
	src := "#   include   \"/etc/passwd\"\n"
	path := writeTempSource(t, src)
	errs := cDenyErrors(path)
	if len(errs) != 1 {
		t.Fatalf("expected 1 deny error, got %d: %+v", len(errs), errs)
	}
}

func TestCDenyErrors_WindowsDriveLetter(t *testing.T) {
	src := "#include \"C:\\Users\\admin\\signing.key\"\n"
	path := writeTempSource(t, src)
	errs := cDenyErrors(path)
	if len(errs) != 1 {
		t.Fatalf("expected 1 deny error, got %d: %+v", len(errs), errs)
	}
}

// writeTempSource writes the source to a unique temp file and
// returns its absolute path. Each test gets its own file so the
// `cDenyErrors` file-read helper sees fresh bytes (and to keep the
// tests parallel-safe).
func writeTempSource(t *testing.T, src string) string {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/input.c"
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatalf("write temp source: %v", err)
	}
	return path
}
