//! Regression test for the wasmtime / wasi-preview1-component-adapter-
//! provider version lockstep.
//!
//! # Background
//!
//! The CLI's `edge build --lang=js` wraps the JS runtime's
//! `wasm32-wasip1` core module into a Preview 2 component via
//! `wasm-tools component new --adapt wasi_snapshot_preview1.reactor.wasm`.
//! The adapter file is shipped as a build-script artefact of
//! `wasi-preview1-component-adapter-provider`, which the CLI pins as
//! a data-dep in edge-cli/Cargo.toml so the artefact is extracted
//! into `$CARGO_HOME/registry/src/.../artefacts/` whenever `cargo
//! install --path edge-cli` runs (see edge-cli/src/commands/build.rs
//! `resolve_wasi_adapter`).
//!
//! The adapter and wasmtime are ABI-locked: a 45.0.3 adapter only
//! works against wasmtime 45.0.3's linker (the linker rejects a
//! 46.x adapter as "imports don't match", and a 45.x adapter
//! triggers a similar error against wasmtime 46.x). The two pins
//! are in separate Cargo.toml files (edge-cli/Cargo.toml +
//! edge-runtime/Cargo.toml) and are easy to bump out of sync.
//!
//! # What this test does
//!
//! 1. Reads edge-cli/Cargo.toml and extracts the
//!    `wasi-preview1-component-adapter-provider = "=X.Y.Z"` pin.
//! 2. Reads edge-runtime/Cargo.toml and extracts the
//!    `wasmtime = "=X.Y.Z"` pin.
//! 3. Asserts the two versions match. A future bump to either that
//!    forgets the other trips this test.
//!
//! This is a static-text test (no subprocess, no network). It
//! always runs, even on CI runners without `wasm-tools`.

use std::path::PathBuf;

/// Read a `package = "version"` dep entry from a Cargo.toml file.
/// Returns the version string (without the leading `=`) or None if
/// not found. Tolerant of whitespace, comments, and section context.
fn extract_dep_version(cargo_toml: &str, dep_name: &str) -> Option<String> {
    // Strip comments — otherwise a `# foo = "..."` line in a doc
    // comment would match the needle before the real dep entry.
    let stripped: String = cargo_toml
        .lines()
        .map(|l| match l.find('#') {
            Some(i) => &l[..i],
            None => l,
        })
        .collect::<Vec<_>>()
        .join("\n");

    // Match patterns like:
    //   foo = "=1.2.3"
    //   foo = { version = "=1.2.3", features = [...] }
    //   foo = { version = "=1.2.3", default-features = false }
    // We look for the dep name as a key (anchored on whitespace or
    // newline) followed by `=` and either a bare string or a `{...}`
    // table.
    let needle = format!("{} =", dep_name);
    let mut start = 0;
    while let Some(idx) = stripped[start..].find(&needle) {
        let abs = start + idx;
        // Anchor: ensure this is at the start of a key (preceded by
        // whitespace or newline), not the middle of an identifier.
        let preceded_ok = abs == 0
            || stripped
                .as_bytes()
                .get(abs - 1)
                .map(|b| b.is_ascii_whitespace() || *b == b'\n')
                .unwrap_or(false);
        if !preceded_ok {
            start = abs + needle.len();
            continue;
        }
        // Find the version string. Three shapes:
        //   name = "=X.Y.Z"
        //   name = "X.Y.Z"
        //   name = { version = "=X.Y.Z", ... }
        // After `name =`, the rest of the line is either:
        //   1. `"..."` — a bare string. The value may begin with `=`
        //      (semver pin), so we cannot use `rest.find('=')` to
        //      locate the key-value separator (that would find the
        //      `=` inside the string). Instead, peek past leading
        //      whitespace; if the next char is `"`, treat the whole
        //      rest as a quoted string.
        //   2. `{...}` — an inline table. The `=` immediately after
        //      `name ` is the key-value separator.
        let after = abs + needle.len();
        let rest = &stripped[after..];
        let rest = rest.trim_start();
        // Bare string shape.
        if let Some(s) = rest.strip_prefix('"') {
            let end = s.find('"').unwrap_or(s.len());
            return Some(s[..end].trim_start_matches('=').to_string());
        }
        // Inline-table shape.
        if let Some(after_brace) = rest.strip_prefix('{') {
            if let Some(v) = after_brace.find("version") {
                let after_v = &after_brace[v + "version".len()..];
                let after_v = after_v
                    .trim_start()
                    .strip_prefix('=')
                    .unwrap_or(after_v.trim_start());
                let after_v = after_v.trim_start();
                if let Some(s) = after_v.strip_prefix('"') {
                    let end = s.find('"').unwrap_or(s.len());
                    return Some(s[..end].trim_start_matches('=').to_string());
                }
            }
        }
        start = after;
    }
    None
}

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is `…/edge-cli`. The workspace root is one
    // level up.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("edge-cli/Cargo.toml has a parent dir")
        .to_path_buf()
}

#[test]
fn wasi_preview1_adapter_matches_wasmtime_version() {
    let cli_toml = std::fs::read_to_string(repo_root().join("edge-cli/Cargo.toml"))
        .expect("read edge-cli/Cargo.toml");
    let runtime_toml = std::fs::read_to_string(repo_root().join("edge-runtime/Cargo.toml"))
        .expect("read edge-runtime/Cargo.toml");

    let adapter =
        extract_dep_version(&cli_toml, "wasi-preview1-component-adapter-provider").unwrap_or_else(
            || {
                eprintln!("DEBUG cli_toml first 2000 chars:\n{}", &cli_toml[..cli_toml.len().min(2000)]);
                panic!(
                    "edge-cli/Cargo.toml has no `wasi-preview1-component-adapter-provider` dep; \
                     the CLI needs this to extract the wasi-preview1 reactor adapter the JS \
                     build pipeline wraps with (see edge-cli/src/commands/build.rs::resolve_wasi_adapter)."
                );
            },
        );
    let wasmtime = extract_dep_version(&runtime_toml, "wasmtime")
        .unwrap_or_else(|| panic!("edge-runtime/Cargo.toml has no `wasmtime` dep"));

    assert_eq!(
        adapter, wasmtime,
        "wasi-preview1-component-adapter-provider ({adapter}) and wasmtime ({wasmtime}) \
         must be the same version — the adapter's imports are ABI-locked to the matching \
         wasmtime linker, and a mismatch breaks `edge build --lang=js` at the \
         `wasm-tools component new --adapt` step (the linker rejects the adapter as \
         'imports don't match'). Bump both in lockstep."
    );
}
