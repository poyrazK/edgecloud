//! edge CLI — edgeCloud developer toolchain.

pub mod api;
mod commands;
pub mod config;
mod migrate;
mod output;
mod state;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "edge", version = "0.1.0", about = "edgeCloud developer CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Path to project directory (default: current directory).
    #[arg(short, long, default_value = ".")]
    path: std::path::PathBuf,
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
    },

    /// Compile the project to WebAssembly.
    Build,

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
    },

    /// Get deployment status.
    Status,

    /// Set an environment variable.
    EnvSet {
        /// Environment variable key.
        key: String,
        /// Environment variable value.
        value: String,
    },

    /// List environment variables.
    EnvList,

    /// Activate a specific deployment.
    Activate {
        /// Deployment ID to activate.
        deployment_id: String,
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

    /// Manage authentication (signup, login, whoami, logout).
    Auth {
        #[command(subcommand)]
        action: crate::commands::auth::AuthAction,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Init { name, api } => commands::init::run(&name, api.as_deref()),
        Command::Build => commands::build::run(&cli.path),
        Command::Deploy { app, id, regions, auto_rollback } => {
            commands::deploy::run(&cli.path, &app, id.as_deref(), &regions, auto_rollback)
        }
        Command::Status => commands::status::run(&cli.path),
        Command::EnvSet { key, value } => commands::env::set_var(&cli.path, &key, &value),
        Command::EnvList => commands::env::list_vars(&cli.path),
        Command::Activate { deployment_id } => commands::activate::run(&cli.path, &deployment_id),
        Command::Rollback { app } => commands::rollback::run(&cli.path, &app),
        Command::Migrate { path, auto } => commands::migrate::run(&path, auto),
        Command::Dev => commands::dev::run(&cli.path),
        Command::Open { force } => commands::open::run(&cli.path, force),
        Command::Deployments => commands::deployments::run(&cli.path),
        Command::Auth { action } => action.run(),
    }
}
