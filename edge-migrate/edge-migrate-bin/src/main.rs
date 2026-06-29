//! edge-migrate CLI.
//!
//! Accepts a C or Rust source file (or tree), analyzes it locally, and
//! uploads to edgeCloud for transformation and compilation.
//!
//! M3 added the `--language <c|rust>` flag. Default is `c`. When
//! `rust` is selected, the bin dispatches to `RustAnalyzer` /
//! `RustTransformer` instead of the C pair.

mod report;

use anyhow::{Context, Result};
use clap::Parser;
use edge_migrate_lib::{
    analyzer::CAnalyzer,
    is_valid_deployment_app_name,
    preprocessor::{Preprocessor, PreprocessorInfo},
    report::{MigrationReport, TransformOutput, TRANSFORM_OUTPUT_VERSION},
    rust_analyzer::RustAnalyzer,
    rust_transformer::RustTransformer,
    transformer::Transformer,
    tree::{transform_tree_for_language_with_app_name, walk_tree_for_language, FileEntry},
    Language,
};
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

const DEFAULT_API_URL: &str = "https://api.edgecloud.dev";

/// Maximum bytes of a server error body the CLI will keep in memory
/// before truncating. Same value as
/// `edge-cli/src/api/client.rs::MAX_ERR_BODY` (issue #109 F9).
const MAX_ERR_BODY: usize = 4 * 1024;

/// Cap a server response body at [`MAX_ERR_BODY`] bytes. A
/// misbehaving control plane returning a multi-GB 4xx/5xx body
/// would otherwise OOM the CLI before the body is printed. Walks
/// down to a UTF-8 char boundary because `String::truncate` panics
/// mid-multibyte-char. Kept identical to
/// `edge-cli/src/api/client.rs::truncate_body` so a wire-format
/// regression in one crate is caught in the other. Issue #109 F9.
fn truncate_body(s: &str) -> String {
    if s.len() <= MAX_ERR_BODY {
        return s.to_string();
    }
    let mut end = MAX_ERR_BODY;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = String::with_capacity(end + 16);
    out.push_str(&s[..end]);
    out.push_str("... [truncated]");
    out
}

/// Output format for `--transform`. `text` (default) writes raw WASI C
/// to stdout — the contract external CLI users have always had.
/// `json` writes a `TransformOutput` envelope that bundles the
/// structured `MigrationReport` with the WASI C source — consumed by
/// the Go control plane to populate `PatternsDetected` /
/// `PatternsTransformed` / `PatternsManualReview` with real data.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum OutputFormat {
    Text,
    Json,
}

#[derive(Parser, Debug)]
#[command(name = "edge-migrate")]
#[command(version)]
struct Args {
    /// The C/Rust source file to migrate.
    #[arg(value_name = "FILE")]
    file: Option<String>,

    /// Transform the source and write WASI source to stdout.
    /// Used by the edgeCloud control-plane to pipe output to
    /// wasi-sdk clang (C) or rustc (Rust).
    #[arg(long, value_name = "SOURCE_FILE")]
    transform: Option<String>,

    /// Force upload even if the file has untransformable patterns.
    #[arg(short, long)]
    force: bool,

    /// Output format for `--transform`: `text` (default, raw WASI C on
    /// stdout) or `json` (a `TransformOutput` envelope with both the
    /// structured `MigrationReport` and the WASI C source). The Go
    /// control plane uses `json`; external users should leave this alone.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,

    /// Analyze the source and emit a JSON `MigrationReport` on stdout.
    /// Used by the Go control-plane's `MigrateTree` to extract per-file
    /// structured data (`patterns_detected` / `transformations` /
    /// `manual_review`) without re-parsing the WASI source. Conflicts
    /// with `file`, `tree`, and `transform`.
    #[arg(long, value_name = "SOURCE_FILE", conflicts_with_all = ["file", "transform"])]
    analyze_json: Option<String>,

    /// Migrate a directory of source files. Walks for `.c`/`.h` (C)
    /// or `.rs` (Rust) files (skipping `build/`, `target/`,
    /// `node_modules/`, etc.), analyzes each, and uploads the whole
    /// tree to `POST /api/migrate-tree` as a multipart form.
    /// Conflicts with `file` and `transform`.
    #[arg(long, value_name = "DIR", conflicts_with_all = ["file", "transform"])]
    tree: Option<String>,

    /// App name for the `--tree` upload. Required when `--tree` is
    /// used. Must match `^[a-z0-9][a-z0-9-]{0,62}$`. If omitted, the
    /// basename of `--tree` is used.
    #[arg(long, value_name = "NAME", requires = "tree")]
    app_name: Option<String>,

    /// Source language: `c` (default) or `rust`. Used by every mode
    /// (single-file, `--transform`, `--analyze-json`, `--tree`).
    /// The value is forwarded to the server's `language` form field.
    #[arg(long, value_name = "LANG", default_value = "c")]
    language: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Validate the language arg once, up front. An unknown value is
    // a user error; bail before reading any source.
    let language = parse_language(&args.language)?;

    // --analyze-json: analyze the file and emit a JSON MigrationReport
    // on stdout. Used by the Go control-plane's MigrateTree for
    // per-file structured data. Language-aware: C uses the preprocessor
    // + CAnalyzer; Rust uses RustAnalyzer (no preprocessor).
    if let Some(ref source_path) = args.analyze_json {
        let source = read_file(source_path).await?;
        let local_report = match language {
            Language::C => {
                let (mut analyzer, preprocessor_info) = build_c_analyzer_with_preprocessor(&source);
                let matches = analyzer.analyze(&source);
                match preprocessor_info {
                    Some(pp) => MigrationReport::from_pattern_matches_with_preprocessor(
                        &derive_app_name(source_path, Language::C),
                        matches,
                        pp,
                    ),
                    None => MigrationReport::from_pattern_matches(
                        &derive_app_name(source_path, Language::C),
                        matches,
                    ),
                }
            }
            Language::Rust => {
                let mut analyzer = RustAnalyzer::new();
                let matches = analyzer.analyze(&source);
                MigrationReport::from_pattern_matches(
                    &derive_app_name(source_path, Language::Rust),
                    matches,
                )
            }
        };
        // Emit a single JSON document on stdout. No trailing newline;
        // the Go service uses json.Unmarshal.
        let json = serde_json::to_string(&local_report)
            .context("Failed to serialize MigrationReport to JSON")?;
        print!("{}", json);
        return Ok(());
    }

    // --transform: analyze + transform, output WASI source to stdout,
    // exit immediately. Used by the Go control-plane as:
    //   edge-migrate --transform --language <c|rust> [--format json] <file>
    //
    // The Go control plane passes `--format json` to consume a
    // `TransformOutput` envelope containing both the structured
    // `MigrationReport` and the transformed source. External CLI users
    // get plain WASI source by default.
    if let Some(ref source_path) = args.transform {
        let source = read_file(source_path).await?;
        let app_name = derive_app_name(source_path, language);

        // Build the report + transformed source per language.
        let (report, wasi_source) = match language {
            Language::C => {
                // When clang is reachable, attach a preprocessor so
                // patterns hidden behind macros become visible. When
                // clang is missing, the analyzer falls back to the
                // unexpanded source silently — no user-visible error.
                let (mut analyzer, preprocessor_info) = build_c_analyzer_with_preprocessor(&source);
                let matches = analyzer.analyze(&source);
                let report = match preprocessor_info.clone() {
                    Some(pp) => MigrationReport::from_pattern_matches_with_preprocessor(
                        &app_name,
                        matches.clone(),
                        pp,
                    ),
                    None => MigrationReport::from_pattern_matches(&app_name, matches.clone()),
                };
                let result = Transformer::transform(&source, matches, preprocessor_info);
                (report, result.transformed_source)
            }
            Language::Rust => {
                // No preprocessor for Rust in v1. See the
                // rust_analyzer.rs header comment for the future
                // rustc -Zunpretty=expanded hook.
                let mut analyzer = RustAnalyzer::new();
                let matches = analyzer.analyze(&source);
                let report = MigrationReport::from_pattern_matches(&app_name, matches.clone());
                let result = RustTransformer.transform(&source, matches);
                (report, result.transformed_source)
            }
        };

        match args.format {
            OutputFormat::Text => {
                print!("{}", wasi_source);
            }
            OutputFormat::Json => {
                let envelope = TransformOutput {
                    version: TRANSFORM_OUTPUT_VERSION,
                    report,
                    wasi_c: wasi_source,
                };
                let json =
                    serde_json::to_string(&envelope).context("serializing TransformOutput")?;
                print!("{}", json);
            }
        }
        return Ok(());
    }

    // --tree DIR [--app-name NAME]: walk a directory, analyze each
    // source file, then upload the whole tree to
    // POST /api/migrate-tree.
    if let Some(ref dir) = args.tree {
        return run_tree_upload(dir, args.app_name.as_deref(), args.force, language).await;
    }

    let file = args
        .file
        .as_ref()
        .expect("FILE argument required when not using --transform");
    let source = read_file(file).await?;
    let app_name = derive_app_name(file, language);

    // Analyze locally. The preprocessor is C-only; Rust has no
    // preprocessor in v1.
    let local_report = match language {
        Language::C => {
            let (mut analyzer, preprocessor_info) = build_c_analyzer_with_preprocessor(&source);
            let matches = analyzer.analyze(&source);
            match preprocessor_info {
                Some(pp) => {
                    MigrationReport::from_pattern_matches_with_preprocessor(&app_name, matches, pp)
                }
                None => MigrationReport::from_pattern_matches(&app_name, matches),
            }
        }
        Language::Rust => {
            let mut analyzer = RustAnalyzer::new();
            let matches = analyzer.analyze(&source);
            MigrationReport::from_pattern_matches(&app_name, matches)
        }
    };

    // Display local analysis report
    report::print_analysis_report(&local_report, language_label(language));

    // Determine if we should upload
    let migratable = local_report.is_migratable();
    if !migratable && !args.force {
        println!();
        println!("❌ File contains untransformable patterns.");
        println!("  Run with --force to upload anyway.");
        std::process::exit(1);
    }

    // Upload to edgeCloud
    println!();
    println!("Uploading to edgeCloud for transformation...");
    match upload_to_edgecloud(file, &source, language).await {
        Ok(server_report) => {
            report::print_server_report(&server_report);
        }
        Err(e) => {
            eprintln!("Upload failed: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}

/// Parse and validate the `--language` clap arg. Returns the typed
/// `Language` enum. Unknown values produce a clear CLI error.
fn parse_language(s: &str) -> Result<Language> {
    match s {
        "c" => Ok(Language::C),
        "rust" => Ok(Language::Rust),
        other => anyhow::bail!("invalid --language '{}': must be 'c' or 'rust'", other),
    }
}

/// Display label for the language — used in report section headers
/// and as the `language` multipart value sent to the server.
fn language_label(lang: Language) -> &'static str {
    match lang {
        Language::C => "c",
        Language::Rust => "rust",
    }
}

async fn read_file(path: &str) -> Result<String> {
    let mut file = File::open(path).await.context("Failed to open file")?;
    let mut contents = Vec::new();
    file.read_to_end(&mut contents)
        .await
        .context("Failed to read file")?;
    String::from_utf8(contents).context("File is not valid UTF-8")
}

/// Strip the appropriate extension for the language so the derived
/// app name never has a trailing `.c` / `.rs`. `file_stem` already
/// handles both; the `language` param is reserved for future
/// per-language tweaks (e.g. stripping `Cargo.toml` for crate names).
fn derive_app_name(path: &str, lang: Language) -> String {
    let path = Path::new(path);
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("app");
    if stem.is_empty() {
        return "app".to_string();
    }
    let _ = lang;
    stem.to_string()
}

/// Read the API key. Delegates to the shared `edge-config` crate so the
/// precedence (env → config file → error) stays in lock-step with the
/// `edge` CLI.
fn read_api_key() -> Result<String> {
    edge_config::read_api_key()
}

/// Read the API URL. Delegates to `edge-config` for the same reason as
/// [`read_api_key`].
fn read_api_url() -> String {
    edge_config::read_api_url(DEFAULT_API_URL)
}

/// Build a C analyzer with a preprocessor attached if one is reachable
/// on the system. Returns the analyzer + the `PreprocessorInfo` to
/// attach to the transform result (so the report can summarize macro
/// expansion). When no clang is found, the analyzer falls back to the
/// unexpanded source and `preprocessor_info` is `None`.
///
/// `source` is used to count `#define` directives so the report can
/// display an accurate macro count. The analyzer will re-run the
/// preprocessor internally during `analyze()`; the upfront count is
/// for the user-facing summary only.
fn build_c_analyzer_with_preprocessor(source: &str) -> (CAnalyzer, Option<PreprocessorInfo>) {
    match Preprocessor::discover() {
        Some(pp) => {
            // Count #define directives in the *original* source.
            // The analyzer will re-expand and produce an authoritative
            // count internally; this is the best estimate we can give
            // before invoking clang twice.
            let macros_expanded = source
                .lines()
                .filter(|l| {
                    let t = l.trim_start();
                    t.starts_with("#define ") || t.starts_with("#define\t")
                })
                .count();
            let info = PreprocessorInfo {
                clang_version: pp.clang_version(),
                files_processed: 1,
                macros_expanded,
            };
            (CAnalyzer::with_preprocessor(pp), Some(info))
        }
        None => (CAnalyzer::new(), None),
    }
}

async fn upload_to_edgecloud(
    file_path: &str,
    source: &str,
    language: Language,
) -> Result<MigrationReport> {
    let api_url = read_api_url();
    let api_key = read_api_key()?;

    let client = reqwest::Client::new();
    let form = reqwest::multipart::Form::new()
        .text("filename", file_path.to_string())
        .text("language", language_label(language).to_string())
        .part(
            "file",
            reqwest::multipart::Part::text(source.to_string()).file_name(file_path.to_string()),
        );

    let response = client
        .post(format!("{}/api/migrate", api_url))
        .bearer_auth(api_key)
        .multipart(form)
        .send()
        .await
        .context("Failed to send request")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = truncate_body(&response.text().await.unwrap_or_default());
        anyhow::bail!("Server returned {}: {}", status, body);
    }

    let report: MigrationReport = response
        .json()
        .await
        .context("Failed to parse server response")?;

    Ok(report)
}

/// Run the `--tree DIR` flow: walk the directory, transform each
/// file locally, print the local tree report, then upload the
/// multipart form to `POST /api/migrate-tree`.
///
/// `app_name_arg` is the developer-supplied `--app-name` (if any);
/// falls back to the basename of `dir` if absent. `force` overrides
/// the "tree has untransformable patterns" guard. `language` drives
/// the analyzer + transformer + extension filter.
async fn run_tree_upload(
    dir: &str,
    app_name_arg: Option<&str>,
    force: bool,
    language: Language,
) -> Result<()> {
    let path = Path::new(dir);
    let entries = walk_tree_for_language(path, language).context("walking tree")?;
    if entries.is_empty() {
        let exts = match language {
            Language::C => ".c or .h",
            Language::Rust => ".rs",
        };
        anyhow::bail!("no {} files found in {}", exts, dir);
    }

    // Resolve the app name. CLI-supplied name takes precedence; else
    // fall back to the dir basename. Validate against the public-facing
    // regex; the server enforces the same regex.
    let derived = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("app")
        .to_string();
    let app_name = app_name_arg.map(|s| s.to_string()).unwrap_or(derived);
    if !is_valid_deployment_app_name(&app_name) {
        anyhow::bail!(
            "invalid app name '{}': must match ^[a-z0-9][a-z0-9-]{{0,62}}$",
            app_name
        );
    }

    let result = transform_tree_for_language_with_app_name(entries, &app_name, language);
    report::print_tree_report(&result.tree_report, language_label(language));

    if !result.tree_report.is_migratable() && !force {
        eprintln!();
        eprintln!("❌ Tree contains untransformable patterns.");
        eprintln!("  Run with --force to upload anyway.");
        std::process::exit(1);
    }

    eprintln!();
    eprintln!("Uploading tree to edgeCloud...");
    match upload_tree_to_edgecloud(&app_name, &result.entries, language).await {
        Ok(server_report) => {
            report::print_tree_report(&server_report, language_label(language));
            Ok(())
        }
        Err(e) => {
            eprintln!("Upload failed: {}", e);
            std::process::exit(1);
        }
    }
}

/// Build a multipart form (one `file` part per entry, plus a `tree`
/// manifest JSON, `app_name`, and `language`) and POST to
/// `POST /api/migrate-tree`. The server response is a
/// `TreeMigrationReport`.
async fn upload_tree_to_edgecloud(
    app_name: &str,
    entries: &[FileEntry],
    language: Language,
) -> Result<edge_migrate_lib::TreeMigrationReport> {
    let api_url = std::env::var("EDGE_API_URL").unwrap_or_else(|_| DEFAULT_API_URL.to_string());
    let api_key = std::env::var("EDGE_API_KEY")
        .context("EDGE_API_KEY not set — run `edge auth login` first")?;

    // Build the manifest JSON the server expects.
    let files_json = serde_json::json!({
        "files": entries.iter().map(|e| &e.path).collect::<Vec<_>>(),
    })
    .to_string();

    let mut form = reqwest::multipart::Form::new()
        .text("app_name", app_name.to_string())
        .text("language", language_label(language).to_string())
        .text("tree", files_json);

    for entry in entries {
        form = form.part(
            "file",
            reqwest::multipart::Part::text(entry.source.clone()).file_name(entry.path.clone()),
        );
    }

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/api/migrate-tree", api_url))
        .bearer_auth(api_key)
        .multipart(form)
        .send()
        .await
        .context("Failed to send request")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = truncate_body(&response.text().await.unwrap_or_default());
        anyhow::bail!("Server returned {}: {}", status, body);
    }

    let report: edge_migrate_lib::TreeMigrationReport = response
        .json()
        .await
        .context("Failed to parse server response")?;

    Ok(report)
}
