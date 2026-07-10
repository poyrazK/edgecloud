# edge-cli

Developer CLI for the
[edgeCloud](https://github.com/poyrazK/edgecloud) platform — scaffold,
build, and deploy WebAssembly components. The package is `edge-cli`
but the installed binary is named `edge` (`[[bin]] name = "edge"` in
`Cargo.toml`); every command in this document refers to it as `edge`.

## Install

From a clone of this monorepo:

```sh
cargo install --path . --locked
```

This drops `edge` into `~/.cargo/bin/` (or wherever `$CARGO_HOME/bin`
points). Confirm with `edge --version`.

## Quick start

```sh
# 1. Create an account (opens a browser to the signup form).
edge auth signup

# 2. Scaffold a FaaS-shaped Rust starter. The starter ships with a
#    vendored wit/ tree (issue #576) so it builds offline — you do
#    NOT need the monorepo on the same machine after `edge init`.
edge init my-app --lang=rust
cd my-app

# 3. Build the .wasm component. The CLI handles the two-step
#    cargo + wasm-tools component new recipe (issue #410) so the
#    output matches wasi:http@0.2.1 (wasmtime 45.0.3).
edge build

# 4. Deploy.
edge deploy
```

For the JS path, swap `--lang=rust` for `--lang=js` in step 2. The
JS starter pulls `@edgecloud/sdk` from npm (issue #424) and uses
esbuild + `wasm-tools` to produce the component.

## Command reference

| Command | What it does |
|---|---|
| `edge init <name> [--lang=rust\|js]` | Scaffold a project (Cargo.toml + src/lib.rs + wit/, or package.json + src/handler.js). |
| `edge build [--lang=rust\|js]` | Build the .wasm component from the current project. |
| `edge deploy [--preview]` | Upload the artifact and activate it. `--preview` stamps `EDGE_PREVIEW_PR_NUMBER` for the PR-preview workflow. |
| `edge activate <deployment-id>` | Promote a previous upload to active. |
| `edge rollback <app>` | Roll back to the last good deployment. |
| `edge dev [--lang=rust\|js]` | Local build + run + watch loop (uses the same build pipeline as `edge build`). |
| `edge open` | Open the deployed app's URL in a browser. |
| `edge status [runtime\|deployment]` | Worker-reported runtime status, or deployment-row status. |
| `edge deployments` | Paginated deployment list. |
| `edge quota` | Current quota usage (memory, deploys, requests, outbound bytes). |
| `edge logs --app <name> [--follow]` | Recent log lines for an app. `--follow` polls every ~2s. |
| `edge migrate <path>` | Migrate a C or Rust source to WASI Preview 2 (calls `edge-migrate` as a subprocess). |
| `edge env set\|list\|delete` | Per-app env vars (encrypted at rest when a secrets key is configured). |
| `edge auth signup\|login\|whoami\|logout\|keys` | Auth lifecycle + API key CRUD. |
| `edge traffic show\|set` | Canary / blue-green traffic splits. |
| `edge domains add\|list\|check\|remove` | Custom FQDN bindings. |
| `edge egress show\|set\|clear` | Outbound host allowlist. |
| `edge ingress <app>` | Inspect the per-app ingress route + rate limit. |
| `edge apps get\|create` | Tenant app metadata. |
| `edge completions <SHELL>` | Emit a shell completion script (see below). |

For the full surface (every flag, every subcommand), run
`edge --help` or `edge <command> --help`. The CLI derives all help
text from clap, so the docs never lag the binary.

## Shell completions (issue #506)

`edge completions <SHELL>` writes a completion script to stdout. Pipe
it to the install location for your shell:

```sh
# bash — ~/.local/share/bash-completion/completions/edge
edge completions bash > ~/.local/share/bash-completion/completions/edge

# zsh — any directory on $fpath; the conventional spot is a user
# autoloads dir under $XDG_DATA_HOME or ~/.zsh/completions
edge completions zsh > ~/.zsh/completions/_edge
# then ensure ~/.zsh/completions is on $fpath in your .zshrc:
#   fpath=(~/.zsh/completions $fpath); autoload -U compinit; compinit

# fish — ~/.config/fish/completions/
edge completions fish > ~/.config/fish/completions/edge.fish

# powershell — append to your $PROFILE
edge completions powershell >> $PROFILE

# elvish — ~/.config/elvish/lib/
edge completions elvish > ~/.config/elvish/lib/edge.elv
```

The script is regenerated from the live clap tree at call time, so
it always matches the current command surface — no manual upkeep
when a new subcommand lands upstream.

## Local state

The CLI persists per-project state to `.edge/state.json` and reads
global config from `~/.config/edgecloud/config.toml` (resolved via
the shared `edge-config` crate). Override the API base with
`EDGE_API_URL=<url>`; the env var wins over the config file.

## Environment variables

| Var | Effect |
|---|---|
| `EDGE_API_URL` | Override the control plane base URL. |
| `EDGE_API_KEY` | Override the API key (otherwise read from the config file). |
| `EDGE_JS_WASI_ADAPTER` | Override the vendored wasi-preview1 adapter path (JS build only). |

## Out of scope

The control plane Go service, the Rust worker (`edge-worker`), and
the Caddy ingress controller (`edge-ingress`) are separate binaries —
see the repo root `README.md` for the full layout and the `Makefile`
targets for one-shot dev setup.