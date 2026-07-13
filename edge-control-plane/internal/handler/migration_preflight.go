package handler

import (
	"regexp"
	"strings"
)

// preflightMatch is a single hit from the L1 handler preflight scan
// (issue #622, commit 3). `pattern` is one of the public Reason
// constants below (so the SDK can branch on a stable enum); `line`
// is the 1-indexed line number of the hit within its source blob
// (single-file source for /api/v1/migrate, per-file source for
// /api/v1/migrate-tree). `path` is the file path for tree uploads
// (empty for single-file).
type preflightMatch struct {
	Pattern string `json:"pattern"`
	Line    int    `json:"line"`
	Path    string `json:"path,omitempty"`
}

// preflight reason constants — the bounded cardinality for the
// `pattern` field on preflightMatch and the `reason` label on the
// future metric (issue #622 commit 6). Any new reason requires
// adding a row to the TestPreflight_AllReasonsCovered test and a
// branch in the handler test matrix.
const (
	preflightReasonIncludeBytes  = "include_bytes"
	preflightReasonIncludeStr    = "include_str"
	preflightReasonIncludeMacro  = "include_macro"
	preflightReasonEnvMacro      = "env_macro"
	preflightReasonOptionEnv     = "option_env"
	preflightReasonCompileError  = "compile_error"
	preflightReasonPathAttr      = "path_attr"
	preflightReasonIncludeAttr   = "include_attr"
	preflightReasonAbsoluteIncl  = "absolute_include"
	preflightReasonTraversalIncl = "traversal_include"
	preflightReasonEmbed         = "embed"
)

// rustMacroRe matches the host-reach macro invocations
// include_bytes!, include_str!, include!, env!, option_env!,
// compile_error!. The leading boundary is one of (start-of-string)
// or a non-identifier character; the trailing boundary is `!(`
// (parens are required — bare `include_bytes` in source without
// `!(` is a normal identifier). Case-sensitive (Rust macros are
// lowercase by convention).
var rustMacroRe = regexp.MustCompile(`(?:^|[^A-Za-z0-9_])(include_bytes|include_str|include|env|option_env|compile_error)!\s*\(`)

// rustAttrRe matches #[path = "..."] and #[include = "..."] module
// attributes. Whitespace between #, [, attr-name, =, and the value
// is tolerated; double-quoted strings only (single-quoted Rust
// string literals are not legal at the attribute position). Group
// 1 captures the attribute name (path or include) so the caller
// can classify the reason without re-scanning the line.
var rustAttrRe = regexp.MustCompile(`(?:^|\s)#\s*\[\s*(path|include)\s*=\s*"`)

// cIncludeRe matches a C #include directive with either quoted or
// angled form, capturing the inner path in group 1. Tolerates
// whitespace between #, include, and the opener. The path itself
// is validated separately by isDenylistedCPath (mirroring the
// analyzer's C pre-pass set in
// edge-migrate/edge-migrate-lib/src/analyzer.rs and
// edge-control-plane/internal/service/migration_deny.go).
var cIncludeRe = regexp.MustCompile(`(?m)^\s*#\s*include\s*["<]([^">]+)[" >]`)

// cEmbedRe matches a C23 #embed directive (any form — quoted or
// angled). Tree mode is the legitimate way to bundle data; the
// `#embed` directive is reserved for the host filesystem in our
// model and is rejected unconditionally. We capture the whole
// opener (quote or angle) so the SDK can render the precise shape.
var cEmbedRe = regexp.MustCompile(`(?m)^\s*#\s*embed\b`)

// preflightMigrateSource scans a single source blob for compile-time
// host-reach patterns. Returns the list of matches in source order
// (one entry per hit; the same line can yield multiple matches if
// the source contains more than one banned pattern on it — rare
// but possible). `path` is empty for single-file uploads and the
// tree-relative path for tree uploads (populated onto each match
// so the SDK can render the offending file).
//
// `language` is "c" or "rust". "rust" runs only the Rust regex set;
// "c" runs only the C regex set. An unknown language is a no-op
// (returns no matches) so the handler's earlier language gate
// remains authoritative — preflight never widens the language set.
func preflightMigrateSource(language, path, source string) []preflightMatch {
	var out []preflightMatch

	// We must scan line-by-line ourselves so the reported line number
	// matches what the user sees in their source. Doing a single
	// `re.FindAllStringIndex` on the whole blob and subtracting
	// prior-newline counts is fine for the Rust/C attribute regexes
	// (they're single-line anchored), but the C #include / #embed
	// regexes use `(?m)` multiline mode and produce line-anchored
	// hits anyway. Use a single linear scan with line counting to
	// keep the logic uniform.
	//
	// Per-line comment stripping: a `// ...` line comment hides
	// everything after it from the compiler. The regex above would
	// otherwise flag `// include_bytes!("/etc/passwd")` because
	// the regex boundary sees the macro identifier — but the
	// compiler never sees it. We trim trailing line comments before
	// scanning. Block comments (`/* ... */`) are NOT handled here
	// (they span lines); L2 tree-sitter handles those correctly.
	// Document the limitation so a future regression to "should we
	// try harder?" is conscious — see the unit test
	// TestPreflight_Rust_Negative_CommentMentionsIncludeBytes.
	for i, raw := range strings.Split(source, "\n") {
		line := stripLineComment(language, raw)
		lineNo := i + 1

		if language == "rust" {
			if locs := rustMacroRe.FindAllStringSubmatchIndex(line, -1); len(locs) > 0 {
				for _, loc := range locs {
					// Group 1 = macro identifier (submatch 2-3).
					if len(loc) < 4 {
						continue
					}
					name := line[loc[2]:loc[3]]
					out = append(out, preflightMatch{
						Pattern: rustMacroReason(name),
						Line:    lineNo,
						Path:    path,
					})
				}
			}
			if locs := rustAttrRe.FindAllStringSubmatchIndex(line, -1); len(locs) > 0 {
				for _, loc := range locs {
					// Group 1 = attribute name (path or include),
					// submatch indices 2-3.
					if len(loc) < 4 {
						continue
					}
					name := line[loc[2]:loc[3]]
					reason := preflightReasonPathAttr
					if name == "include" {
						reason = preflightReasonIncludeAttr
					}
					out = append(out, preflightMatch{
						Pattern: reason,
						Line:    lineNo,
						Path:    path,
					})
				}
			}
			continue
		}

		if language == "c" {
			if locs := cIncludeRe.FindAllStringSubmatchIndex(line, -1); len(locs) > 0 {
				for _, loc := range locs {
					// Group 1 = path (submatch 2-3).
					if len(loc) < 4 {
						continue
					}
					pathStr := line[loc[2]:loc[3]]
					if reason, ok := classifyCIncludePath(pathStr); ok {
						out = append(out, preflightMatch{
							Pattern: reason,
							Line:    lineNo,
							Path:    path,
						})
					}
				}
			}
			if cEmbedRe.MatchString(line) {
				out = append(out, preflightMatch{
					Pattern: preflightReasonEmbed,
					Line:    lineNo,
					Path:    path,
				})
			}
			continue
		}
	}

	return out
}

// stripLineComment returns the line up to (but not including) the
// first line-comment marker that's outside a string literal. Used
// to keep `// include_bytes!("/etc/passwd")` from triggering the
// regex. Block comments are NOT handled (they span lines); the L2
// tree-sitter analyzer handles those correctly. String-literal
// awareness is intentionally minimal: it scans the line left to
// right, tracking whether we're inside `"..."` and only treats
// `//` as a comment when it's outside a string. Raw strings
// (Rust `r"..."` / `r#"..."#`) and C's lack of single-quoted
// strings on the include path make this sufficient for our match
// set (the only tokens we care about are preceded by an identifier
// boundary or `#`, neither of which appear in string literals).
func stripLineComment(language, raw string) string {
	var (
		inString    bool
		stringDelim byte
	)
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if inString {
			if c == '\\' && i+1 < len(raw) {
				// Skip the escaped character.
				i++
				continue
			}
			if c == stringDelim {
				inString = false
			}
			continue
		}
		// Not in a string.
		if c == '"' || (language == "c" && c == '\'') {
			inString = true
			stringDelim = c
			continue
		}
		// Rust raw string: r"..." or r#"..."# — when we see
		// `r` followed by a quote, peek to count the `#`s and
		// skip until matching closer.
		if language == "rust" && (c == 'r' || c == 'b') {
			j := i
			if c == 'b' && j+1 < len(raw) && (raw[j+1] == 'r' || raw[j+1] == '"') {
				j++
			}
			if j < len(raw) && raw[j] == 'r' && j+1 < len(raw) && raw[j+1] == '"' {
				// Count the #s.
				k := j + 2
				hashes := 0
				for k < len(raw) && raw[k] == '#' {
					hashes++
					k++
				}
				// Walk to the matching closer: `"` followed by
				// `hashes` `#`s.
				closer := `"` + strings.Repeat("#", hashes)
				if idx := strings.Index(raw[k:], closer); idx >= 0 {
					i = k + idx + len(closer) - 1
				} else {
					// Unterminated — treat rest of line as in-string.
					return raw[:i]
				}
				continue
			}
		}
		// `//` line comment marker.
		if c == '/' && i+1 < len(raw) && raw[i+1] == '/' {
			return raw[:i]
		}
		// `/*` block comment marker (single-line fragment only).
		if c == '/' && i+1 < len(raw) && raw[i+1] == '*' {
			// Look for `*/` on the same line; if absent, return
			// the prefix (block comment continues to next line,
			// L2 will catch it).
			if idx := strings.Index(raw[i+2:], "*/"); idx >= 0 {
				// Replace the comment with spaces so column
				// numbers (not currently reported) stay stable.
				return raw[:i] + strings.Repeat(" ", idx+4) + stripLineComment(language, raw[i+idx+4:])
			}
			return raw[:i]
		}
	}
	return raw
}

// rustMacroReason maps a Rust macro identifier to its preflight
// reason constant. The set is closed — the regex above matches
// only these six names — so any other input is a programmer error.
func rustMacroReason(name string) string {
	switch name {
	case "include_bytes":
		return preflightReasonIncludeBytes
	case "include_str":
		return preflightReasonIncludeStr
	case "include":
		return preflightReasonIncludeMacro
	case "env":
		return preflightReasonEnvMacro
	case "option_env":
		return preflightReasonOptionEnv
	case "compile_error":
		return preflightReasonCompileError
	default:
		// Should be unreachable given the regex's alternation.
		// Return a generic bucket so a future regex drift doesn't
		// silently drop a hit on the floor.
		return "rust_macro"
	}
}

// classifyCIncludePath returns the deny reason for a C #include path
// if it's host-reach, or ok=false if it's a legitimate system or
// relative header. The classification mirrors the analyzer's
// pre_pass_deny_c set in edge-migrate-lib/src/analyzer.rs and the
// Go-side migration_deny.go::isDenylistedCPath so a drift between
// the layers surfaces as a test failure.
//
// Distinguishes absolute vs traversal so the SDK can render a
// precise message ("absolute include is forbidden" vs "include path
// traversal is forbidden") without forcing the operator to read
// the source.
func classifyCIncludePath(p string) (string, bool) {
	if p == "" {
		return "", false
	}
	// Absolute (POSIX / Windows UNC): starts with `/` or `\`.
	if strings.HasPrefix(p, "/") || strings.HasPrefix(p, "\\") {
		return preflightReasonAbsoluteIncl, true
	}
	// Windows drive letter: "C:..." or "C:/..." or "C:\...".
	// Two characters, second is colon.
	if len(p) >= 2 && p[1] == ':' {
		return preflightReasonAbsoluteIncl, true
	}
	// Traversal: any `..` path segment. Split on both POSIX `/`
	// AND Windows `\` since filepath.ToSlash is a no-op on Unix
	// (it only converts backslashes on Windows), so on the server
	// (Linux) we must split on both separators ourselves. This
	// mirrors the Rust analyzer's `path.split(['/', '\\'])` after
	// the clippy fix in commit 2.
	for _, seg := range strings.FieldsFunc(p, func(r rune) bool { return r == '/' || r == '\\' }) {
		if seg == ".." {
			return preflightReasonTraversalIncl, true
		}
	}
	return "", false
}

// preflightDetailsFor builds the `details` map payload for
// PreflightDeniedCtx from a match list. Shape:
//
//	{
//	    "rejected_at": "preflight",
//	    "language":    "rust" | "c",
//	    "matches":     [{pattern, line, path?}, ...],
//	}
//
// Exposed so tests can build the same shape without re-implementing
// the JSON keys.
func preflightDetailsFor(language string, matches []preflightMatch) map[string]any {
	return map[string]any{
		"rejected_at": "preflight",
		"language":    language,
		"matches":     matches,
	}
}
