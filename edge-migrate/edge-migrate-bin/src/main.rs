//! edge-migrate CLI.
//!
//! Accepts a C source file, analyzes it locally, and uploads to edgeCloud
//! for transformation and compilation.

mod report;

use anyhow::{Context, Result};
use clap::Parser;
use edge_migrate_lib::{
    analyzer::CAnalyzer,
    is_valid_deployment_app_name,
    preprocessor::{Preprocessor, PreprocessorInfo},
    report::MigrationReport,
    transformer::Transformer,
    tree::{transform_tree_with_app_name, walk_tree},
};
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

const DEFAULT_API_URL: &str = "https://api.edgecloud.dev";

#[derive(Parser, Debug)]
#[command(name = "edge-migrate")]
#[command(version)]
struct Args {
    /// The C source file to migrate.
    #[arg(value_name = "FILE")]
    file: Option<String>,

    /// Transform the source and write WASI C to stdout.
    /// Used by the edgeCloud control-plane to pipe output to wasi-sdk clang.
    #[arg(long, value_name = "SOURCE_FILE")]
    transform: Option<String>,

    /// Force upload even if the file has untransformable patterns.
    #[arg(short, long)]
    force: bool,

    /// Analyze the source and emit a JSON `MigrationReport` on stdout.
    /// Used by the Go control-plane's `MigrateTree` to extract per-file
    /// structured data (`patterns_detected` / `transformations` /
    /// `manual_review`) without re-parsing the WASI C output. Conflicts
    /// with `file`, `tree`, and `transform`.
    #[arg(long, value_name = "SOURCE_FILE", conflicts_with_all = ["file", "transform"])]
    analyze_json: Option<String>,

    /// Migrate a directory of C source files. Walks for `.c`/`.h`
    /// files (skipping `build/`, `target/`, `node_modules/`, etc.),
    /// analyzes each, and uploads the whole tree to
    /// `POST /api/migrate-tree` as a multipart form. Conflicts with
    /// `file` and `transform`.
    #[arg(long, value_name = "DIR", conflicts_with_all = ["file", "transform"])]
    tree: Option<String>,

    /// App name for the `--tree` upload. Required when `--tree` is
    /// used. Must match `^[a-z0-9][a-z0-9-]{0,62}$`. If omitted, the
    /// basename of `--tree` is used.
    #[arg(long, value_name = "NAME", requires = "tree")]
    app_name: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // --analyze-json: analyze the file and emit a JSON MigrationReport
    // on stdout. Used by the Go control-plane's MigrateTree for
    // per-file structured data. Runs after the preprocessor (if
    // available) and reuses the same analyzer pipeline as --transform.
    if let Some(ref source_path) = args.analyze_json {
        let source = read_file(source_path).await?;
        let (mut analyzer, preprocessor_info) = build_analyzer_with_preprocessor(&source);
        let matches = analyzer.analyze(&source);
        let local_report = match preprocessor_info {
            Some(pp) => MigrationReport::from_pattern_matches_with_preprocessor(
                &derive_app_name(source_path),
                matches,
                pp,
            ),
            None => MigrationReport::from_pattern_matches(&derive_app_name(source_path), matches),
        };
        // Emit a single JSON document on stdout. No trailing newline;
        // the Go service uses json.Unmarshal.
        let json = serde_json::to_string(&local_report)
            .context("Failed to serialize MigrationReport to JSON")?;
        print!("{}", json);
        return Ok(());
    }

    // --transform: analyze + transform, output WASI C to stdout, exit immediately.
    // Used by the Go control-plane as: edge-migrate --transform <file>
    if let Some(ref source_path) = args.transform {
        let source = read_file(source_path).await?;
        // M1.C5: when clang is reachable, attach a preprocessor so
        // patterns hidden behind macros become visible. When clang is
        // missing, the analyzer falls back to the unexpanded source
        // silently — no user-visible error.
        let (mut analyzer, preprocessor_info) = build_analyzer_with_preprocessor(&source);
        let matches = analyzer.analyze(&source);
        let result = Transformer::transform(&source, matches, preprocessor_info);
        print!("{}", result.transformed_source);
        return Ok(());
    }

    // --tree DIR [--app-name NAME]: walk a directory, analyze each
    // .c/.h file, then upload the whole tree to POST /api/migrate-tree.
    if let Some(ref dir) = args.tree {
        return run_tree_upload(dir, args.app_name.as_deref(), args.force).await;
    }

    let file = args.file.as_ref().expect("FILE argument required when not using --transform");
    let source = read_file(file).await?;
    let app_name = derive_app_name(file);

    // Analyze locally
    let (mut analyzer, preprocessor_info) = build_analyzer_with_preprocessor(&source);
    let matches = analyzer.analyze(&source);
    let local_report = match preprocessor_info {
        Some(pp) => MigrationReport::from_pattern_matches_with_preprocessor(
            &app_name,
            matches,
            pp,
        ),
        None => MigrationReport::from_pattern_matches(&app_name, matches),
    };

    // Display local analysis report
    report::print_analysis_report(&local_report);

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
    match upload_to_edgecloud(file, &source).await {
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

async fn read_file(path: &str) -> Result<String> {
    let mut file = File::open(path).await.context("Failed to open file")?;
    let mut contents = Vec::new();
    file.read_to_end(&mut contents)
        .await
        .context("Failed to read file")?;
    String::from_utf8(contents).context("File is not valid UTF-8")
}

fn derive_app_name(path: &str) -> String {
    let path = Path::new(path);
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("app");
    stem.to_string()
}

/// Build an analyzer with a preprocessor attached if one is reachable
/// on the system. Returns the analyzer + the `PreprocessorInfo` to
/// attach to the transform result (so the report can summarize macro
/// expansion). When no clang is found, the analyzer falls back to the
/// unexpanded source and `preprocessor_info` is `None`.
///
/// `source` is used to count `#define` directives so the report can
/// display an accurate macro count. The analyzer will re-run the
/// preprocessor internally during `analyze()`; the upfront count is
/// for the user-facing summary only.
fn build_analyzer_with_preprocessor(
    source: &str,
) -> (CAnalyzer, Option<PreprocessorInfo>) {
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

async fn upload_to_edgecloud(file_path: &str, source: &str) -> Result<MigrationReport> {
    let api_url = std::env::var("EDGE_API_URL")
        .unwrap_or_else(|_| DEFAULT_API_URL.to_string());
    let api_key = std::env::var("EDGE_API_KEY")
        .context("EDGE_API_KEY not set — run `edge auth login` first")?;

    let client = reqwest::Client::new();
    let form = reqwest::multipart::Form::new()
        .text("filename", file_path.to_string())
        .text("language", "c".to_string())
        .part(
            "file",
            reqwest::multipart::Part::text(source.to_string())
                .file_name(file_path.to_string()),
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
        let body = response.text().await.unwrap_or_default();
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
/// the "tree has untransformable patterns" guard.
async fn run_tree_upload(dir: &str, app_name_arg: Option<&str>, force: bool) -> Result<()> {
    let path = Path::new(dir);
    let entries = walk_tree(path).context("walking tree")?;
    if entries.is_empty() {
        anyhow::bail!("no .c or .h files found in {}", dir);
    }

    // Resolve the app name. CLI-supplied name takes precedence; else
    // fall back to the dir basename. Validate against the public-facing
    // regex; the server enforces the same regex.
    let derived = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("app")
        .to_string();
    let app_name = app_name_arg
        .map(|s| s.to_string())
        .unwrap_or(derived);
    if !is_valid_deployment_app_name(&app_name) {
        anyhow::bail!(
            "invalid app name '{}': must match ^[a-z0-9][a-z0-9-]{{0,62}}$",
            app_name
        );
    }

    let result = transform_tree_with_app_name(entries, &app_name);
    report::print_tree_report(&result.tree_report);

    if !result.tree_report.is_migratable() && !force {
        eprintln!();
        eprintln!("❌ Tree contains untransformable patterns.");
        eprintln!("  Run with --force to upload anyway.");
        std::process::exit(1);
    }

    eprintln!();
    eprintln!("Uploading tree to edgeCloud...");
    match upload_tree_to_edgecloud(&app_name, &result.entries).await {
        Ok(server_report) => {
            report::print_tree_report(&server_report);
            Ok(())
        }
        Err(e) => {
            eprintln!("Upload failed: {}", e);
            std::process::exit(1);
        }
    }
}

/// Build a multipart form (one `file` part per entry, plus a `tree`
/// manifest JSON, `app_name`, and `language=c`) and POST to
/// `POST /api/migrate-tree`. The server response is a
/// `TreeMigrationReport`.
async fn upload_tree_to_edgecloud(
    app_name: &str,
    entries: &[edge_migrate_lib::tree::FileEntry],
) -> Result<edge_migrate_lib::TreeMigrationReport> {
    let api_url =
        std::env::var("EDGE_API_URL").unwrap_or_else(|_| DEFAULT_API_URL.to_string());
    let api_key = std::env::var("EDGE_API_KEY")
        .context("EDGE_API_KEY not set — run `edge auth login` first")?;

    // Build the manifest JSON the server expects.
    let files_json = serde_json::json!({
        "files": entries.iter().map(|e| &e.path).collect::<Vec<_>>(),
    })
    .to_string();

    let mut form = reqwest::multipart::Form::new()
        .text("app_name", app_name.to_string())
        .text("language", "c".to_string())
        .text("tree", files_json);

    for entry in entries {
        form = form.part(
            "file",
            reqwest::multipart::Part::text(entry.source.clone())
                .file_name(entry.path.clone()),
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
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Server returned {}: {}", status, body);
    }

    let report: edge_migrate_lib::TreeMigrationReport = response
        .json()
        .await
        .context("Failed to parse server response")?;

    Ok(report)
}
