//! URL parser for `globalThis.EdgeCloud.http.fetch` (issue #550).
//!
//! Extracted from `register.rs` so the parser can be unit-tested on
//! the host (`cargo test --manifest-path edge-js-runtime/Cargo.toml`)
//! without dragging in the wasm-target-only `rquickjs::Ctx` machinery.
//! The parser is pure — no allocations beyond the returned `String`s
//! — and is the most failure-prone helper because the WIT `authority`
//! is `host[:port]` only; it must NOT include the scheme, the path,
//! the query string, or `user:pass@`.

/// Parsed components of an outbound URL.
#[derive(Debug)]
pub struct ParsedUrl {
    pub scheme: String,
    pub authority: String,
    pub path: String,
}

/// Split `http://host:port/path?q` into `(scheme, authority, path)`.
///
/// Tolerates the four shapes the recipe documents:
///   - `https://example.com/db`
///   - `https://example.com:5432/db?sslmode=require`
///   - `http://[::1]:8080/health`
///   - bare `example.com/db` (defaults to `https`).
pub fn parse_fetch_url(url: &str) -> Result<ParsedUrl, String> {
    if url.is_empty() {
        return Err("url is empty".into());
    }

    let (scheme, rest) = if let Some(idx) = url.find("://") {
        (url[..idx].to_ascii_lowercase(), &url[idx + 3..])
    } else {
        ("https".to_string(), url)
    };
    if rest.is_empty() {
        return Err(format!("missing host in url {url:?}"));
    }

    let (authority, path) = match rest.find('/') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, "/"),
    };
    if authority.is_empty() {
        return Err(format!("missing host in url {url:?}"));
    }

    // WIT `authority` must NOT include the scheme or path; port is OK.
    // Strip userinfo if present (the WIT does not accept `user:pass@host`).
    let authority = match authority.find('@') {
        Some(idx) => &authority[idx + 1..],
        None => authority,
    };
    if authority.is_empty() {
        return Err(format!("missing host after userinfo in url {url:?}"));
    }
    // Strip any embedded `?` from authority (shouldn't happen but be safe).
    let authority = match authority.find('?') {
        Some(idx) => &authority[..idx],
        None => authority,
    };

    // Empty path → "/" so we always send a valid path component.
    let path = if path.is_empty() { "/" } else { path };

    Ok(ParsedUrl {
        scheme,
        authority: authority.to_string(),
        path: path.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::parse_fetch_url;

    /// The four URL shapes the recipe documents must all parse cleanly
    /// into `(scheme, authority, path)` with the authority stripped of
    /// any userinfo, query, or path that snuck in.
    #[test]
    fn parse_fetch_url_handles_recipe_shapes() {
        // (input, expected_authority, expected_path)
        let cases: &[(&str, &str, &str)] = &[
            ("https://example.com/db", "example.com", "/db"),
            (
                "https://example.com:5432/db?sslmode=require",
                "example.com:5432",
                "/db?sslmode=require",
            ),
            ("http://[::1]:8080/health", "[::1]:8080", "/health"),
            ("example.com/db", "example.com", "/db"),
            (
                "https://user:pass@host.example/p?x=1",
                "host.example",
                "/p?x=1",
            ),
            ("https://HOST.example/PATH", "HOST.example", "/PATH"),
        ];
        for (input, want_authority, want_path) in cases {
            let parsed = parse_fetch_url(input).expect(input);
            assert_eq!(parsed.authority, *want_authority, "authority for {input:?}");
            assert_eq!(parsed.path, *want_path, "path for {input:?}");
        }
    }

    #[test]
    fn parse_fetch_url_defaults_to_https_when_no_scheme() {
        // Bare hosts (no scheme) default to https — the recipe's
        // pre-step for the user copying a Neon connection string into
        // a JS handler. Make sure that path stays stable.
        let parsed = parse_fetch_url("ep-xyz.us-east-2.aws.neon.tech/sql").unwrap();
        assert_eq!(parsed.scheme, "https");
        assert_eq!(parsed.authority, "ep-xyz.us-east-2.aws.neon.tech");
        assert_eq!(parsed.path, "/sql");
    }

    /// Failure modes the recipe calls out — these are the URLs a
    /// tenant might type and that must surface a clear JS error rather
    /// than trapping the guest.
    #[test]
    fn parse_fetch_url_rejects_unparseable_inputs() {
        let bad = ["", "http://", "https:///path-only"];
        for input in bad {
            let res = parse_fetch_url(input);
            assert!(
                res.is_err(),
                "expected parse_fetch_url({input:?}) to error, got {res:?}"
            );
            assert!(
                !res.unwrap_err().is_empty(),
                "error message must be non-empty for {input:?}"
            );
        }
    }

    /// `user:pass@host` must be stripped — the WIT authority is
    /// `host[:port]` only. A regression that let the userinfo through
    /// would emit `authorization: Basic ...` headers by accident and
    /// leak credentials in worker logs.
    #[test]
    fn parse_fetch_url_strips_userinfo() {
        let parsed = parse_fetch_url("https://alice:secret@db.example/p").unwrap();
        assert_eq!(parsed.authority, "db.example");
        assert_eq!(parsed.path, "/p");
        assert!(!parsed.authority.contains("alice"));
        assert!(!parsed.authority.contains("secret"));
    }
}
