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
    },

    /// Compile the project to WebAssembly.
    Build,

    /// Upload the artifact to the edgeCloud control plane.
    Deploy,

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
    Open,

    /// List all deployments for the app.
    Deployments,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Init { name } => commands::init::run(&name),
        Command::Build => commands::build::run(&cli.path),
        Command::Deploy => commands::deploy::run(&cli.path),
        Command::Status => commands::status::run(&cli.path),
        Command::EnvSet { key, value } => commands::env::set_var(&cli.path, &key, &value),
        Command::EnvList => commands::env::list_vars(&cli.path),
        Command::Activate { deployment_id } => commands::activate::run(&cli.path, &deployment_id),
        Command::Migrate { path, auto } => commands::migrate::run(&path, auto),
        Command::Dev => commands::dev::run(&cli.path),
        Command::Open => commands::open::run(&cli.path),
        Command::Deployments => commands::deployments::run(&cli.path),
    }
}
