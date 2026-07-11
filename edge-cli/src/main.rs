//! edge CLI — edgeCloud developer toolchain.

pub mod api;
mod commands;
pub mod config;
mod migrate;
mod output;
mod scaffold;
mod state;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use clap_complete::Shell;
use std::time::SystemTime;

use crate::config::EdgeToml;

/// Source language for the `edge build` and `edge init` commands
/// (issue #317 — Multi-language runtime support). Each variant maps
/// to a dedicated build pipeline; the lowercase clap value (`rust`,
/// `js`) is what the user types on the command line and what gets
/// written into `[project] language = "..."` in `edge.toml`.
///
/// Single source of truth: the `#[value(name = "...")]` attribute is
/// the canonical wire form. `Display::fmt` and `as_str()` both
/// delegate to it, so adding a new variant is a one-line change at
/// this enum (no edits to `init.rs`, `build.rs`, or `deploy.rs`).
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum LangArg {
    #[value(name = "rust")]
    Rust,
    #[value(name = "js")]
    Js,
}

impl std::fmt::Display for LangArg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl LangArg {
    /// Lowercase string form used both as the clap value and as the
    /// `edge.toml` `language` field. Kept here so `commands::build::path_for`
    /// and friends can match on a `&str` rather than re-implementing
    /// the per-variant rendering.
    pub fn as_str(&self) -> &'static str {
        // Sourced from the `#[value(name = "...")]` attribute above so
        // adding a variant here can't drift from the clap wire form.
        match self {
            LangArg::Rust => "rust",
            LangArg::Js => "js",
        }
    }
}

/// Generate a short unique suffix for preview deployments.
fn short_hash() -> String {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}", nanos >> 8)
}

#[derive(Parser)]
#[command(name = "edge", version = "0.1.0", about = "edgeCloud developer CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Path to project directory (default: current directory).
    #[arg(short, long, default_value = ".")]
    path: std::path::PathBuf,
}

/// `edge domains <add|list|check|remove>` — manage custom FQDNs bound
/// to a deployment (issue #83). The full subcommand surface is
/// defined here (clap derives the help text from it) and dispatched
/// through `commands::domains::DomainsAction::run`. Adding a new
/// subcommand means one variant here + one match arm.
#[derive(Subcommand)]
enum DomainsCommand {
    /// Bind a custom FQDN (e.g. `api.acme.com`) to an app.
    Add {
        /// App name.
        app: String,
        /// Fully-qualified domain name to bind.
        fqdn: String,
    },
    /// List all custom FQDNs bound to an app.
    List {
        /// App name.
        app: String,
    },
    /// Show a single FQDN's status (incl. any `last_error`).
    Check {
        /// App name.
        app: String,
        /// Fully-qualified domain name to inspect.
        fqdn: String,
    },
    /// Unbind a custom FQDN from an app.
    Remove {
        /// App name.
        app: String,
        /// Fully-qualified domain name to unbind.
        fqdn: String,
    },
}

impl From<DomainsCommand> for commands::domains::DomainsAction {
    fn from(cmd: DomainsCommand) -> Self {
        match cmd {
            DomainsCommand::Add { app, fqdn } => Self::Add { app, fqdn },
            DomainsCommand::List { app } => Self::List { app },
            DomainsCommand::Check { app, fqdn } => Self::Check { app, fqdn },
            DomainsCommand::Remove { app, fqdn } => Self::Remove { app, fqdn },
        }
    }
}

#[derive(Subcommand)]
enum Command {
    /// Scaffold a new project.
    Init {
        /// Name of the project to create.
        name: String,
        /// Override the control-plane URL written into edge.toml's
        /// `[deployment].api`. If omitted, the section is left empty
        /// and the runtime falls back to `EDGE_API_URL`,
        /// `~/.config/edgecloud/config.toml`, then the default.
        #[arg(long)]
        api: Option<String>,
        /// Source language for the starter template. `rust` scaffolds
        /// `Cargo.toml` + `src/main.rs`; `js` scaffolds `index.js`
        /// (a wasi:http-only handler, built by Javy). The choice is
        /// written into `[project] language = "..."` in `edge.toml`.
        #[arg(long, value_enum, default_value_t = LangArg::Rust)]
        lang: LangArg,
    },

    /// Compile the project to WebAssembly.
    Build {
        /// Source language to build. When omitted, reads `[project] language`
        /// from `edge.toml` (falling back to `rust`). If both `--lang` and
        /// the toml are set, they must agree — mismatches are rejected with
        /// a clear error so you never accidentally build the wrong artifact.
        #[arg(long, value_enum)]
        lang: Option<LangArg>,
    },

    /// Upload the artifact to the edgeCloud control plane, or activate a stored one.
    ///
    /// Use --id <deployment_id> to activate a previously-stored deployment
    /// (e.g. one produced by `edge migrate`).
    Deploy {
        /// App name. Upload mode: overrides edge.toml. Activate mode (with --id): primary source; falls back to .edge/state.json.
        #[arg(default_value = "")]
        app: String,

        /// Source language for the artifact path lookup. By default
        /// we read `[project] language` from `edge.toml`. Pass this
        /// flag to override (e.g. when you built with
        /// `edge build --lang=js` but your toml still says `rust`).
        /// `--lang` and the toml must agree; mismatches are rejected
        /// with a clear error so a stale rust deploy path can never
        /// be served for a Javy artifact.
        #[arg(long, value_enum)]
        lang: Option<LangArg>,

        /// Activate a previously-stored deployment by ID (e.g. from `edge migrate`).
        #[arg(long, value_name = "deployment_id")]
        id: Option<String>,

        /// Comma-separated list of regions to replicate this deployment to.
        /// `us-east,eu-west` fans out a TaskMessage to both regions at
        /// activate time. Omit to use the control plane's default
        /// region. Ignored when --id is set (regions are baked into
        /// the deployment row at upload time).
        #[arg(long, value_name = "REGIONS", value_delimiter = ',')]
        regions: Vec<String>,

        /// Path to the wasm artifact. Overrides the default
        /// `target/wasm32-wasip2/release/{app}.wasm`.
        #[arg(long, value_name = "FILE")]
        file: Option<std::path::PathBuf>,

        /// Opt in to auto-rollback (issue #74). When set, the server
        /// records `auto_rollback_enabled = true` on this deployment
        /// (and on the active slot at activate time). With this flag:
        ///   - If the worker hits `restart_count >= 5` and the app is
        ///     marked `crashed`, the worker POSTs to the control plane
        ///     and the last-known-good deployment is restored.
        ///   - If the currently-active deployment has been observed
        ///     `running` for ≥ `STABLE_WINDOW_SECONDS` (default 30s),
        ///     it is promoted to `last_good_deployment_id` so future
        ///     crashes roll back to it instead of an older build.
        ///
        /// Ignored when --id is set (auto-rollback is a deployment-time
        /// property, not a session toggle).
        #[arg(long)]
        auto_rollback: bool,

        /// Desired number of workers to run this deployment in each
        /// region (issue #316). 0 means no threshold. Every worker
        /// already receives every TaskMessage (fan-out) — this is a
        /// monitoring threshold, not a scheduling constraint.
        #[arg(long, default_value_t = 0)]
        replicas: usize,

        /// Deploy as a preview with a unique staging URL.
        /// The app is deployed under a suffixed name (e.g. `myapp--preview-abc123`)
        /// so it gets its own `https://{tenant}-{app}--preview-{hash}.edgecloud.dev` URL.
        /// Use `edge deploy --promote <id>` to promote a preview to production.
        #[arg(long)]
        preview: bool,

        /// Forward a GitHub PR number alongside a `--preview` deploy
        /// (issue #308). The control plane stamps
        /// `EDGE_PREVIEW_PR_NUMBER=<N>` into the guest env so the
        /// deployed service can render PR-aware UI, and the preview
        /// TTL sweeper treats the deploy as PR-linked for observability.
        /// Only meaningful with `--preview`; ignored otherwise. The
        /// GitHub composite action forwards `${{ github.event.pull_request.number }}`
        /// automatically — laptop users rarely need to set this.
        #[arg(long, value_name = "number")]
        pr_number: Option<u32>,

        /// Override the default preview TTL (168h / 7d). Go duration
        /// string: "24h", "168h", "720h". Only meaningful with
        /// `--preview`. Per-deploy overrides win over the server
        /// default, which in turn wins over `PREVIEW_RETENTION` env.
        #[arg(long, value_name = "duration")]
        preview_ttl: Option<String>,

        /// Promote a preview deployment to production.
        /// Takes a deployment ID that was deployed as a preview and activates it
        /// under the app's real name. The preview URL stops working after promotion.
        #[arg(long, value_name = "deployment_id")]
        promote: Option<String>,

        /// Forward an explicit Idempotency-Key (issue #52) on the
        /// deploy request. When omitted, `edge deploy` auto-mints a
        /// fresh UUID v4 per invocation so a CLI retry on a
        /// transient network error replays the original deployment
        /// instead of minting a duplicate. CI typically passes an
        /// explicit key to dedupe retried jobs across runs; laptop
        /// users can ignore this flag.
        ///
        /// Format: server validates as `[a-fA-F0-9-]{8,128}` — UUID
        /// v4 strings pass without modification. Replay returns the
        /// cached deployment_id with status 200 instead of 201.
        #[arg(long, value_name = "uuid")]
        idempotency_key: Option<String>,

        /// Maximum number of retries on transient failures (issue
        /// #571): 5xx, network errors, and 429. The total number of
        /// attempts is `1 + max_retries` — `--max-retries=3` (the
        /// default) means up to 4 attempts. Each retry sends the
        /// **same** `Idempotency-Key` as the first attempt so the
        /// server's replay path returns the cached `deployment_id`
        /// (200) instead of minting a duplicate row. `--max-retries=0`
        /// disables retry (single attempt, fail fast).
        #[arg(long, default_value_t = 3)]
        max_retries: u32,

        /// Base backoff in milliseconds (issue #571). The first
        /// retry sleeps `retry_base_ms × ±25%` jitter; each
        /// subsequent retry doubles the wait, capped at
        /// `retry-cap-ms`. Ignored when `--max-retries=0`.
        #[arg(long, default_value_t = 500)]
        retry_base_ms: u64,

        /// Maximum backoff in milliseconds (issue #571). Caps the
        /// exponential backoff so a sustained outage doesn't pin a
        /// CI job for minutes. Hard-capped at 60_000 (60s) by
        /// `value_parser` — pass a larger value to get a clear
        /// clap error instead of a runaway wait. Worst-case total
        /// retry budget is `retry_cap_ms × max_retries` (plus the
        /// per-attempt round-trip); defaults `8000 × 3` = 24s.
        /// Ignored when `--max-retries=0`.
        #[arg(long, default_value_t = 8_000, value_parser = clap::value_parser!(u64).range(1..=60_000))]
        retry_cap_ms: u64,
    },

    /// Inspect runtime and deployment status.
    ///
    /// `runtime` surfaces the worker-reported status (running /
    /// starting / stopping / crashed / hung / unknown). `deployment`
    /// (default for the no-arg form) is the legacy DB-row view.
    /// The subcommand is optional so bare `edge status` keeps
    /// working as a backward-compat alias for `edge status deployment`.
    Status {
        #[command(subcommand)]
        action: Option<StatusAction>,
    },

    /// Set an environment variable.
    EnvSet {
        /// App name. Defaults to `.edge/state.json` when omitted.
        #[arg(long, default_value = "")]
        app: String,
        /// Environment variable key.
        key: String,
        /// Environment variable value.
        value: String,
    },

    /// List environment variables for an app. The app name comes
    /// from `--app`, or from `.edge/state.json` when omitted.
    EnvList {
        /// App name. Defaults to `.edge/state.json` when omitted.
        #[arg(long, default_value = "")]
        app: String,
    },

    /// Delete an environment variable.
    EnvDelete {
        /// App name. Defaults to `.edge/state.json` when omitted.
        #[arg(long, default_value = "")]
        app: String,
        /// Environment variable key to delete.
        key: String,

        /// Maximum number of retries on transient failures (issue
        /// #571 propagation): 5xx, network errors, and 429. The
        /// total number of attempts is `1 + max_retries`. `edge env
        /// delete` is naturally idempotent (DELETE-by-primary-key),
        /// so retries are safe — `--max-retries=0` disables retry
        /// (single attempt, fail fast).
        #[arg(long, default_value_t = 3)]
        max_retries: u32,

        /// Base backoff in milliseconds (issue #571 propagation).
        /// First retry sleeps `retry_base_ms × ±25%` jitter; each
        /// subsequent retry doubles the wait, capped at
        /// `retry-cap-ms`. Ignored when `--max-retries=0`.
        #[arg(long, default_value_t = 500)]
        retry_base_ms: u64,

        /// Maximum backoff in milliseconds (issue #571
        /// propagation). Caps the exponential backoff. Hard-capped
        /// at 60_000 (60s) by `value_parser`. Ignored when
        /// `--max-retries=0`.
        #[arg(long, default_value_t = 8_000, value_parser = clap::value_parser!(u64).range(1..=60_000))]
        retry_cap_ms: u64,
    },

    /// Activate a specific deployment.
    Activate {
        /// Deployment ID to activate.
        deployment_id: String,
        /// Weight for canary activation (0-100). Omit for atomic cutover (weight=100).
        /// weight=0 drains the deployment; 0<weight<100 splits traffic between
        /// this deployment and the currently active one.
        #[arg(long)]
        weight: Option<u8>,
    },

    /// List all apps, create an app, or show details for one.
    ///
    /// `edge apps` lists all apps for the tenant.
    /// `edge apps create <name>` creates a new app.
    /// `edge apps get <name>` shows details for a specific app.
    Apps {
        #[command(subcommand)]
        action: Option<AppsCommand>,
    },

    /// Roll back to the previous deployment.
    ///
    /// Swaps the active deployment back to the deployment that was
    /// active before the most recent `edge activate` (or `edge deploy`).
    /// Useful for recovering from a broken release without re-uploading
    /// a known-good artifact.
    Rollback {
        /// App name. Defaults to the app in `.edge/state.json`.
        #[arg(default_value = "")]
        app: String,
    },

    /// Analyze source for WASI compatibility.
    Migrate {
        /// Path to source directory (default: path argument).
        #[arg(default_value = ".")]
        path: std::path::PathBuf,
        /// Automatically apply safe transformations in place.
        #[arg(long)]
        auto: bool,
    },

    /// Local development server with hot-reload.
    Dev {
        /// Source language to build. When omitted, reads `[project] language`
        /// from `edge.toml` (falling back to `rust`). Must match the toml
        /// when both are set — mismatches are rejected with a clear error.
        #[arg(long, value_enum)]
        lang: Option<LangArg>,
    },

    /// Open the deployed URL in a browser.
    Open {
        /// Open even if the current deployment has crashed.
        #[arg(long)]
        force: bool,
    },

    /// List all deployments for the app.
    ///
    /// Calls `GET /api/v1/list/{appName}` and prints a 4-column
    /// table (ID / STATUS / CREATED / URL). When the tenant has
    /// more deployments than fit on one page, renders a
    /// `page X of N` footer with `prev:` / `next:` hints; small
    /// lists render silently.
    Deployments {
        /// 1-indexed page number. Defaults to 1. Page numbers are
        /// validated to be `>= 1`; `edge deployments --page 0`
        /// exits non-zero with a clear error rather than
        /// silently rendering the first page.
        #[arg(long, default_value_t = 1, value_name = "N")]
        page: u32,

        /// Page size forwarded as `?limit=` on the request. The
        /// server's default (20) is used when this flag is absent
        /// (the CLI sends 0 to mean "server default" — the wire
        /// request omits the query param entirely).
        #[arg(long, default_value_t = 0, value_name = "N")]
        limit: u32,
    },

    /// Show tenant quota and usage.
    Quota,

    /// Show the ingress target (worker address and port) for a running app.
    Ingress {
        /// App name. Defaults to the app in `.edge/state.json`.
        #[arg(default_value = "")]
        app: String,
    },

    /// Read recent log entries for the app (issue #77).
    ///
    /// Calls `GET /api/v1/apps/{appName}/logs` and prints the most
    /// recent entries, newest first. With `--follow`, polls every
    /// 2s and prints new entries as they arrive. With no flags,
    /// prints the last 5 minutes; use `--since 1h` for a longer
    /// window. Pipe mode (`edge logs myapp | jq`) emits one JSON
    /// object per line.
    Logs {
        /// App name. Defaults to the app in `.edge/state.json`.
        #[arg(default_value = "")]
        app: String,

        /// Lower bound on the entry timestamp, expressed as a
        /// relative duration. Accepts `<n>s`, `<n>m`, `<n>h`,
        /// `<n>d` (e.g. `5m`, `1h`, `30s`). Default: 5m.
        #[arg(long, default_value = "5m")]
        since: String,

        /// Minimum severity filter. `warn` returns `warn` + `error`.
        /// Unknown values are rejected by the server with a 400.
        #[arg(long)]
        level: Option<String>,

        /// Poll for new entries instead of printing once and
        /// exiting. Stops on Ctrl-C, after 30 minutes, or when
        /// the process is killed.
        #[arg(short, long)]
        follow: bool,

        /// Maximum entries to return per request. Server clamps
        /// to [1, 1000]; default 100.
        #[arg(long, default_value_t = 100)]
        limit: u32,

        /// Offset for pagination. Use to page through older entries.
        /// Pagination is offset-based; the server returns a
        /// `next_offset` hint when more results exist.
        #[arg(long, value_name = "N")]
        offset: Option<u32>,
    },

    /// Manage authentication (signup, login, whoami, logout).
    Auth {
        #[command(subcommand)]
        action: crate::commands::auth::AuthAction,
    },

    /// Get or set traffic splits for canary/blue-green deployments.
    Traffic {
        #[command(subcommand)]
        action: commands::traffic::TrafficAction,
    },
    /// Manage custom FQDNs bound to a deployment (issue #83).
    #[command(subcommand)]
    Domains(DomainsCommand),

    /// Manage the outbound host allowlist (egress rules).
    Egress {
        #[command(subcommand)]
        action: commands::egress::EgressAction,
    },

    /// Emit a shell completion script to stdout (issue #506).
    /// Pipe the output to your shell's completion directory; see
    /// `edge-cli/README.md` for the per-shell install one-liners.
    Completions {
        /// Target shell: bash, zsh, fish, powershell, or elvish.
        #[arg(value_enum)]
        shell: Shell,
    },
}

#[derive(Subcommand)]
enum StatusAction {
    /// Show the worker-reported runtime status of an app.
    ///
    /// Surfaces `running` | `starting` | `stopping` | `crashed` |
    /// `hung` | `unknown`, plus the region / worker_id /
    /// `last_heartbeat` and (for `crashed`) the worker's exit code.
    /// Hits `GET /api/v1/apps/{appName}/status`; no server-side
    /// filtering of stale heartbeats — the `last_heartbeat` field
    /// is surfaced verbatim so users can decide.
    Runtime {
        /// App name. Defaults to the app in `.edge/state.json`.
        #[arg(default_value = "")]
        app: String,
    },
    /// Show the deployment-row status (DB-side: deployed / active /
    /// failed / migrated). Equivalent to the legacy `edge status`
    /// form for backward compatibility.
    Deployment,
}

/// `edge apps` — list apps or show details for one.
///
/// Bare `edge apps` lists all apps for the tenant.
/// `edge apps create <name>` creates a new app.
/// `edge apps get <name>` shows details for a specific app.
#[derive(Subcommand)]
enum AppsCommand {
    /// Show details for a specific app.
    Get {
        /// App name to fetch.
        name: String,
    },
    /// Create a new app.
    Create {
        /// Name of the app to create.
        name: String,
        /// Optional description for the app.
        #[arg(long)]
        description: Option<String>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Init { name, api, lang } => commands::init::run(&name, api.as_deref(), lang),
        Command::Build { lang } => commands::build::run(&cli.path, lang),
        Command::Deploy {
            app,
            id,
            regions,
            auto_rollback,
            replicas,
            file,
            preview,
            pr_number,
            preview_ttl,
            promote,
            lang,
            idempotency_key,
            max_retries,
            retry_base_ms,
            retry_cap_ms,
        } => {
            if let Some(dep_id) = promote {
                return commands::deploy::run_promote(&cli.path, &app, &dep_id);
            }
            // Resolve the app name once — main.rs needs it to build
            // the preview suffix, but `run_upload` re-reads edge.toml
            // to do the same job. Reading here keeps the URL mint
            // aligned with whatever edge.toml the project ships
            // (issue #308: the suffix must use the same app name the
            // server will see).
            let resolved_app = if !app.is_empty() {
                app.clone()
            } else {
                let toml = EdgeToml::from_path(&cli.path).with_context(|| {
                    format!(
                        "edge deploy requires edge.toml with [project] name in {}",
                        cli.path.display()
                    )
                })?;
                toml.project.name
            };
            let preview_suffix = if preview { Some(short_hash()) } else { None };
            let preview_app = preview_suffix
                .as_ref()
                .map(|s| format!("{}--preview-{}", &resolved_app, s));
            // issue #308: build the PreviewOpts payload for the
            // server when --preview is set. The preview-id is the
            // same hash we used to suffix the app name — that's
            // deliberate, the URL and the server-side store key
            // share the suffix so a tenant can grep their preview
            // list by URL.
            let preview_opts = preview_suffix.map(|s| {
                crate::api::PreviewOpts::new(
                    s,
                    pr_number.unwrap_or(0),
                    preview_ttl.clone().unwrap_or_default(),
                )
            });
            commands::deploy::run(
                &cli.path,
                preview_app.as_deref().unwrap_or(&app),
                id.as_deref(),
                &regions,
                auto_rollback,
                replicas,
                file.as_deref(),
                lang,
                preview_opts.as_ref(),
                idempotency_key.as_deref(),
                max_retries,
                retry_base_ms,
                retry_cap_ms,
            )
        }
        Command::Status { action } => match action.unwrap_or(StatusAction::Deployment) {
            StatusAction::Runtime { app } => commands::status::runtime(&cli.path, &app),
            StatusAction::Deployment => commands::status::run(&cli.path),
        },
        Command::EnvSet { app, key, value } => {
            commands::env::set_var(&cli.path, &app, &key, &value)
        }
        Command::EnvList { app } => commands::env::list_vars(&cli.path, &app),
        Command::EnvDelete {
            app,
            key,
            max_retries,
            retry_base_ms,
            retry_cap_ms,
        } => commands::env::delete_var(
            &cli.path,
            &app,
            &key,
            max_retries,
            retry_base_ms,
            retry_cap_ms,
        ),
        Command::Activate {
            deployment_id,
            weight,
        } => commands::activate::run(&cli.path, &deployment_id, weight),
        Command::Apps { action } => match action {
            None => commands::apps::list(&cli.path),
            Some(AppsCommand::Get { name }) => commands::apps::get(&cli.path, &name),
            Some(AppsCommand::Create { name, description }) => {
                commands::apps::create(&cli.path, &name, description.as_deref())
            }
        },
        Command::Rollback { app } => commands::rollback::run(&cli.path, &app),
        Command::Migrate { path, auto } => commands::migrate::run(&path, auto),
        Command::Dev { lang } => commands::dev::run(&cli.path, lang),
        Command::Open { force } => commands::open::run(&cli.path, force),
        Command::Deployments { page, limit } => commands::deployments::run(&cli.path, page, limit),
        Command::Quota => commands::quota::run(&cli.path),
        Command::Ingress { app } => commands::ingress::run(&cli.path, &app),
        Command::Logs {
            app,
            since,
            level,
            follow,
            limit,
            offset,
        } => {
            let since_dur = parse_since(&since)?;
            commands::logs::run(
                &cli.path,
                &app,
                since_dur,
                level.as_deref(),
                follow,
                limit,
                offset,
            )
        }
        Command::Auth { action } => action.run(),
        Command::Traffic { action } => match action {
            commands::traffic::TrafficAction::Show => commands::traffic::get(&cli.path),
            commands::traffic::TrafficAction::Set {
                splits,
                max_retries,
                retry_base_ms,
                retry_cap_ms,
            } => {
                let parsed: Vec<(String, u8)> = splits
                    .iter()
                    .filter_map(|s| {
                        let (id, w) = s.split_once('=')?;
                        let weight: u8 = w.parse().ok()?;
                        Some((id.to_string(), weight))
                    })
                    .collect();
                commands::traffic::set(&cli.path, &parsed, max_retries, retry_base_ms, retry_cap_ms)
            }
        },
        Command::Domains(cmd) => {
            let action: commands::domains::DomainsAction = cmd.into();
            action.run(&cli.path)
        }
        Command::Egress { action } => match action {
            commands::egress::EgressAction::Show => commands::egress::show(&cli.path),
            commands::egress::EgressAction::Set { hosts } => {
                commands::egress::set(&cli.path, &hosts)
            }
            commands::egress::EgressAction::Clear => commands::egress::clear(&cli.path),
        },
        Command::Completions { shell } => commands::completions::run(shell),
    }
}

/// Parse a relative duration like `30s`, `5m`, `1h`, `7d` into a
/// [`std::time::Duration`]. The server accepts the result as a
/// relative offset from "now" (computed locally before the request
/// goes out), so the wire format ends up as an absolute RFC3339
/// timestamp. Keeping the parser stdlib-only avoids adding a
/// `humantime` dep for this one command.
fn parse_since(s: &str) -> Result<std::time::Duration> {
    let s = s.trim();
    if s.is_empty() {
        anyhow::bail!("--since cannot be empty; pass e.g. 5m, 1h, 30s");
    }
    // Find the boundary between the digits and the unit suffix.
    // Iterating bytes is fine: we only accept ASCII digits and a
    // single ASCII suffix char.
    let split = s
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| anyhow::anyhow!("--since {s:?} is missing a unit (s/m/h/d)"))?;
    let (n_str, unit) = s.split_at(split);
    let n: u64 = n_str
        .parse()
        .with_context(|| format!("--since {s:?} has non-numeric magnitude {n_str:?}"))?;
    let mult: u64 = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        "d" => 60 * 60 * 24,
        other => anyhow::bail!("--since unit {other:?} not supported (use s/m/h/d)"),
    };
    Ok(std::time::Duration::from_secs(n.saturating_mul(mult)))
}
