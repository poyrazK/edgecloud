//! edge CLI — edgeCloud developer toolchain.

pub mod api;
mod commands;
pub mod config;
mod migrate;
mod output;
mod state;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use std::time::SystemTime;

/// Source language for the `edge build` and `edge init` commands
/// (issue #317 — Multi-language runtime support). Each variant maps
/// to a dedicated build pipeline; the lowercase clap render (`rust`,
/// `js`) is what the user types on the command line and what gets
/// written into `[project] language = "..."` in `edge.toml`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum LangArg {
    Rust,
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
        /// Source language to build. Must match the project's
        /// `[project] language` in `edge.toml` (the build does NOT
        /// cross-check — mismatches surface as a missing artifact
        /// at deploy time).
        #[arg(long, value_enum, default_value_t = LangArg::Rust)]
        lang: LangArg,
    },

    /// Upload the artifact to the edgeCloud control plane, or activate a stored one.
    ///
    /// Use --id <deployment_id> to activate a previously-stored deployment
    /// (e.g. one produced by `edge migrate`).
    Deploy {
        /// App name. Upload mode: overrides edge.toml. Activate mode (with --id): primary source; falls back to .edge/state.json.
        #[arg(default_value = "")]
        app: String,

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

        /// Deploy as a preview with a unique staging URL.
        /// The app is deployed under a suffixed name (e.g. `myapp--preview-abc123`)
        /// so it gets its own `https://{tenant}-{app}--preview-{hash}.edgecloud.dev` URL.
        /// Use `edge deploy --promote <id>` to promote a preview to production.
        #[arg(long)]
        preview: bool,

        /// Promote a preview deployment to production.
        /// Takes a deployment ID that was deployed as a preview and activates it
        /// under the app's real name. The preview URL stops working after promotion.
        #[arg(long, value_name = "deployment_id")]
        promote: Option<String>,
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
        /// Environment variable key.
        key: String,
        /// Environment variable value.
        value: String,
    },

    /// List environment variables.
    EnvList,

    /// Delete an environment variable.
    EnvDelete {
        /// Environment variable key to delete.
        key: String,
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
    Dev,

    /// Open the deployed URL in a browser.
    Open {
        /// Open even if the current deployment has crashed.
        #[arg(long)]
        force: bool,
    },

    /// List all deployments for the app.
    Deployments,

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
            file,
            preview,
            promote,
        } => {
            if let Some(dep_id) = promote {
                return commands::deploy::run_promote(&cli.path, &app, &dep_id);
            }
            let preview_app = if preview {
                Some(format!("{}--preview-{}", &app, &short_hash()))
            } else {
                None
            };
            commands::deploy::run(
                &cli.path,
                preview_app.as_deref().unwrap_or(&app),
                id.as_deref(),
                &regions,
                auto_rollback,
                file.as_deref(),
            )
        }
        Command::Status { action } => match action.unwrap_or(StatusAction::Deployment) {
            StatusAction::Runtime { app } => commands::status::runtime(&cli.path, &app),
            StatusAction::Deployment => commands::status::run(&cli.path),
        },
        Command::EnvSet { key, value } => commands::env::set_var(&cli.path, &key, &value),
        Command::EnvList => commands::env::list_vars(&cli.path),
        Command::EnvDelete { key } => commands::env::delete_var(&cli.path, &key),
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
        Command::Dev => commands::dev::run(&cli.path),
        Command::Open { force } => commands::open::run(&cli.path, force),
        Command::Deployments => commands::deployments::run(&cli.path),
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
            commands::traffic::TrafficAction::Set { splits } => {
                let parsed: Vec<(String, u8)> = splits
                    .iter()
                    .filter_map(|s| {
                        let (id, w) = s.split_once('=')?;
                        let weight: u8 = w.parse().ok()?;
                        Some((id.to_string(), weight))
                    })
                    .collect();
                commands::traffic::set(&cli.path, &parsed)
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
