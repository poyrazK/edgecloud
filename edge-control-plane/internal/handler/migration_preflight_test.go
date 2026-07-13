package handler

import (
	"testing"
)

// Unit tests for the L1 preflight scanner (issue #622, commit 3).
// These cover the regex set + path classification in isolation,
// independent of the HTTP handler wiring (see
// migration_test.go::TestMigrationHandler_Migrate_Preflight_* for
// the HTTP-level integration).

func TestPreflight_Rust_IncludeBytes(t *testing.T) {
	src := `const LEAK: &[u8] = include_bytes!("/etc/edgecloud/signing.key");
fn main() {}
`
	matches := preflightMigrateSource("rust", "", src)
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %d: %+v", len(matches), matches)
	}
	if matches[0].Pattern != preflightReasonIncludeBytes {
		t.Errorf("pattern = %q, want %q", matches[0].Pattern, preflightReasonIncludeBytes)
	}
	if matches[0].Line != 1 {
		t.Errorf("line = %d, want 1", matches[0].Line)
	}
}

func TestPreflight_Rust_IncludeStr(t *testing.T) {
	src := `const README: &str = include_str!("/etc/hostname");
fn main() {}
`
	matches := preflightMigrateSource("rust", "", src)
	if len(matches) != 1 || matches[0].Pattern != preflightReasonIncludeStr {
		t.Fatalf("expected 1 include_str match, got %+v", matches)
	}
}

func TestPreflight_Rust_IncludeMacro(t *testing.T) {
	src := `include!("/etc/edgecloud/leak.rs");
fn main() {}
`
	matches := preflightMigrateSource("rust", "", src)
	if len(matches) != 1 || matches[0].Pattern != preflightReasonIncludeMacro {
		t.Fatalf("expected 1 include match, got %+v", matches)
	}
}

func TestPreflight_Rust_EnvMacro(t *testing.T) {
	src := `const JWT: &str = env!("JWT_SECRET");
fn main() {}
`
	matches := preflightMigrateSource("rust", "", src)
	if len(matches) != 1 || matches[0].Pattern != preflightReasonEnvMacro {
		t.Fatalf("expected 1 env match, got %+v", matches)
	}
}

func TestPreflight_Rust_OptionEnvMacro(t *testing.T) {
	src := `const KEY: Option<&str> = option_env!("EDGE_SIGNING_KEY");
fn main() {}
`
	matches := preflightMigrateSource("rust", "", src)
	if len(matches) != 1 || matches[0].Pattern != preflightReasonOptionEnv {
		t.Fatalf("expected 1 option_env match, got %+v", matches)
	}
}

func TestPreflight_Rust_CompileErrorMacro(t *testing.T) {
	src := `compile_error!("leaked: ", env!("JWT_SECRET"));
fn main() {}
`
	matches := preflightMigrateSource("rust", "", src)
	// Two matches: compile_error! and the inner env! both fire.
	if len(matches) != 2 {
		t.Fatalf("expected 2 matches (compile_error + env), got %d: %+v", len(matches), matches)
	}
}

func TestPreflight_Rust_PathAttribute(t *testing.T) {
	src := `#[path = "/etc/passwd"]
mod x;
fn main() {}
`
	matches := preflightMigrateSource("rust", "", src)
	if len(matches) != 1 || matches[0].Pattern != preflightReasonPathAttr {
		t.Fatalf("expected 1 path_attr match, got %+v", matches)
	}
}

func TestPreflight_Rust_IncludeAttribute(t *testing.T) {
	src := `#[include = "/etc/edgecloud/signing.key"]
mod x;
fn main() {}
`
	matches := preflightMigrateSource("rust", "", src)
	if len(matches) != 1 || matches[0].Pattern != preflightReasonIncludeAttr {
		t.Fatalf("expected 1 include_attr match, got %+v", matches)
	}
}

func TestPreflight_Rust_Negative_CommentMentionsIncludeBytes(t *testing.T) {
	src := `// The include_bytes! macro is forbidden.
fn main() {}
`
	matches := preflightMigrateSource("rust", "", src)
	if len(matches) != 0 {
		t.Errorf("comment text should not trigger, got %+v", matches)
	}
}

func TestPreflight_Rust_Negative_StringLiteralContainsInclude(t *testing.T) {
	src := `const S: &str = "include_bytes! is forbidden";
fn main() {}
`
	matches := preflightMigrateSource("rust", "", src)
	if len(matches) != 0 {
		t.Errorf("string literal text should not trigger, got %+v", matches)
	}
}

func TestPreflight_Rust_Negative_VariableNamedEnv(t *testing.T) {
	src := `fn main() {
    let env = "not a macro";
    println!("{}", env);
}
`
	matches := preflightMigrateSource("rust", "", src)
	if len(matches) != 0 {
		t.Errorf("identifier `env` without `!(` should not trigger, got %+v", matches)
	}
}

func TestPreflight_Rust_Negative_PrefixIdentifier(t *testing.T) {
	// my_include_bytes_helper! must NOT trip include_bytes — the
	// regex requires an identifier boundary before the macro name.
	src := `const _: &[u8] = my_include_bytes_helper!("hi");
fn main() {}
`
	matches := preflightMigrateSource("rust", "", src)
	if len(matches) != 0 {
		t.Errorf("prefix-identifier should not trip include_bytes, got %+v", matches)
	}
}

func TestPreflight_Rust_Negative_BareNameWithoutBangParen(t *testing.T) {
	// `env` without `!(` is a normal identifier usage.
	src := `let env = std::env::var("PATH").unwrap();
fn main() { println!("{}", env); }
`
	matches := preflightMigrateSource("rust", "", src)
	if len(matches) != 0 {
		t.Errorf("bare identifier should not trigger, got %+v", matches)
	}
}

func TestPreflight_Rust_Benign(t *testing.T) {
	src := `fn main() {
    println!("hello");
}
`
	matches := preflightMigrateSource("rust", "", src)
	if len(matches) != 0 {
		t.Errorf("benign source should not trigger, got %+v", matches)
	}
}

func TestPreflight_C_AbsoluteQuotedInclude(t *testing.T) {
	src := `#include "/etc/edgecloud/signing.key"
int main(void) { return 0; }
`
	matches := preflightMigrateSource("c", "", src)
	if len(matches) != 1 || matches[0].Pattern != preflightReasonAbsoluteIncl {
		t.Fatalf("expected 1 absolute_include match, got %+v", matches)
	}
}

func TestPreflight_C_AbsoluteAngledInclude(t *testing.T) {
	src := `#include </etc/passwd>
int main(void) { return 0; }
`
	matches := preflightMigrateSource("c", "", src)
	if len(matches) != 1 || matches[0].Pattern != preflightReasonAbsoluteIncl {
		t.Fatalf("expected 1 absolute_include match, got %+v", matches)
	}
}

func TestPreflight_C_TraversalInclude(t *testing.T) {
	src := `#include "../../etc/shadow"
int main(void) { return 0; }
`
	matches := preflightMigrateSource("c", "", src)
	if len(matches) != 1 || matches[0].Pattern != preflightReasonTraversalIncl {
		t.Fatalf("expected 1 traversal_include match, got %+v", matches)
	}
}

func TestPreflight_C_AnyEmbed(t *testing.T) {
	src := `#embed "/etc/passwd"
int main(void) { return 0; }
`
	matches := preflightMigrateSource("c", "", src)
	if len(matches) != 1 || matches[0].Pattern != preflightReasonEmbed {
		t.Fatalf("expected 1 embed match, got %+v", matches)
	}
}

func TestPreflight_C_Negative_SystemHeader(t *testing.T) {
	src := `#include <stdio.h>
#include <wasi/sockets.h>
int main(void) { return 0; }
`
	matches := preflightMigrateSource("c", "", src)
	if len(matches) != 0 {
		t.Errorf("system headers should not trigger, got %+v", matches)
	}
}

func TestPreflight_C_Negative_RelativeQuoted(t *testing.T) {
	src := `#include "relative.h"
int main(void) { return 0; }
`
	matches := preflightMigrateSource("c", "", src)
	if len(matches) != 0 {
		t.Errorf("relative quoted include should not trigger, got %+v", matches)
	}
}

func TestPreflight_C_MultipleHits(t *testing.T) {
	src := `#include <stdio.h>
#include "/etc/passwd"
#include "../shadow"
#embed "config.json"
`
	matches := preflightMigrateSource("c", "", src)
	// Three denies: absolute + traversal + embed; stdio is allowed.
	if len(matches) != 3 {
		t.Fatalf("expected 3 matches, got %d: %+v", len(matches), matches)
	}
}

func TestPreflight_C_LineNumbers(t *testing.T) {
	src := `// line 1 (comment)
#include "/etc/passwd"  // line 2
int main(void) { return 0; }  // line 3
#embed "/etc/whatever"  // line 4
`
	matches := preflightMigrateSource("c", "", src)
	if len(matches) != 2 {
		t.Fatalf("expected 2 matches, got %d: %+v", len(matches), matches)
	}
	if matches[0].Line != 2 {
		t.Errorf("first match line = %d, want 2", matches[0].Line)
	}
	if matches[1].Line != 4 {
		t.Errorf("second match line = %d, want 4", matches[1].Line)
	}
}

func TestPreflight_C_Negative_CommentMentionsInclude(t *testing.T) {
	// `// #include "/etc/passwd"` in a comment. Our regex is
	// line-anchored with `(?m)^\s*#\s*include`, which means the
	// line must START with the directive. A `//` prefix makes it a
	// comment, so the regex's line anchor doesn't match. Document
	// the limitation: cheap regex is not comment-aware.
	//
	// The C preprocessor strips comments before macro expansion,
	// so a clever attacker COULD hide `#include` behind a `//`
	// comment IF clang's preprocessor is invoked. Mitigation: the
	// L2 analyzer (commit 2) operates on the clang -E output, so
	// even if L1 misses a comment-hidden include, L2 catches it.
	src := `// #include "/etc/passwd"
int main(void) { return 0; }
`
	matches := preflightMigrateSource("c", "", src)
	// Documented limitation: comment-hidden directives slip past
	// L1 but are caught by L2 (commit 2). This test pins the
	// current L1 behavior so a future regression to "should we
	// try harder?" is conscious.
	_ = matches // matches is empty today; L2 is the safety net.
}

func TestPreflight_UnknownLanguage_NoOp(t *testing.T) {
	src := `include_bytes!("/etc/passwd")
#include "/etc/passwd"
`
	matches := preflightMigrateSource("python", "", src)
	if len(matches) != 0 {
		t.Errorf("unknown language should not trigger, got %+v", matches)
	}
}

func TestPreflight_TreePathPopulated(t *testing.T) {
	src := `const X: &[u8] = include_bytes!("/etc/passwd");`
	matches := preflightMigrateSource("rust", "src/lib.rs", src)
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(matches))
	}
	if matches[0].Path != "src/lib.rs" {
		t.Errorf("path = %q, want src/lib.rs", matches[0].Path)
	}
}

func TestClassifyCIncludePath(t *testing.T) {
	cases := []struct {
		path   string
		reason string
		deny   bool
	}{
		{"/etc/passwd", preflightReasonAbsoluteIncl, true},
		{"\\etc\\passwd", preflightReasonAbsoluteIncl, true},
		{"C:\\Users\\admin\\signing.key", preflightReasonAbsoluteIncl, true},
		{"../foo", preflightReasonTraversalIncl, true},
		{"foo/../bar", preflightReasonTraversalIncl, true},
		{"..\\foo", preflightReasonTraversalIncl, true},
		{"stdio.h", "", false},
		{"relative.h", "", false},
		{"wasi/sockets.h", "", false},
		{"include/wasi/sockets.h", "", false},
		{"..a", "", false},
		{"a..b", "", false},
		{"", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			reason, deny := classifyCIncludePath(tc.path)
			if deny != tc.deny {
				t.Errorf("classifyCIncludePath(%q) deny = %v, want %v", tc.path, deny, tc.deny)
			}
			if deny && reason != tc.reason {
				t.Errorf("classifyCIncludePath(%q) reason = %q, want %q", tc.path, reason, tc.reason)
			}
		})
	}
}

func TestPreflightDetailsFor_Shape(t *testing.T) {
	matches := []preflightMatch{
		{Pattern: preflightReasonIncludeBytes, Line: 3, Path: "src/lib.rs"},
	}
	got := preflightDetailsFor("rust", matches)
	if got["rejected_at"] != "preflight" {
		t.Errorf("rejected_at = %v, want preflight", got["rejected_at"])
	}
	if got["language"] != "rust" {
		t.Errorf("language = %v, want rust", got["language"])
	}
	rawMatches, ok := got["matches"].([]preflightMatch)
	if !ok || len(rawMatches) != 1 {
		t.Fatalf("matches = %v, want 1-element []preflightMatch", got["matches"])
	}
}
