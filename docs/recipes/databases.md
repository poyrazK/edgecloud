# Database Recipes

This page walks a tenant developer through talking to a hosted database
from an edgeCloud app — without rebuilding any platform plumbing.
Today that means using the **HTTP-based serverless drivers** that the
serverless-DB vendors (Neon, Turso, Upstash) already ship, because they
flow straight through `wasi:http` + the egress allowlist you already
control. Native Postgres / Redis / MySQL drivers are out of scope for
this revision — see the JS gap and the socket-TLS follow-up at the
bottom.

The shape of every recipe is the same: allow the hostnames on the
tenant's egress allowlist, store the connection material in the app's
env, scaffold a Rust FaaS app, and replace `src/lib.rs` with a
`wasi::http` outbound call. Three providers are worked through below;
two of them are verified end-to-end (Neon, Turso); the Upstash recipe
is drafted from source only.

## Why HTTP drivers today

Native database drivers (`sqlx`, `node-postgres`, `mysql_async`, the
Postgres wire-protocol crate) don't compile to `wasm32-wasip2`. They
need TCP and TLS, and a sandboxed guest has neither. Building a native
TLS path inside the guest is the wrong layer to fix it at — TLS for
outbound sockets belongs in the host runtime, not in tenant code.

But there's an answer that works today, with no platform change. The
three vendors below expose a **JSON-over-HTTPS** API that any
`wasi:http` client can speak. The worker routes those calls through
the same egress policy that already governs every other outbound
request, and the response body is plain JSON you parse with
`serde_json`. This partially defuses the data-gravity objection (the
"you can't reach my database" concern raised in the sales pipeline)
without any new platform surface.

Caveat: HTTP drivers cost one extra round-trip per query, with no
connection pooling beyond what `wasi:http`'s underlying hyper client
keeps warm. The latency section below covers what to expect and when
to reach for local state instead.

## Before you start

### Sign in / create a tenant

```sh
edge auth signup --name "<your-name>"
edge auth whoami
```

Or, if you already have an API key:

```sh
edge auth login
edge auth whoami
```

`whoami` prints your tenant id, plan, and the API key id — that's how
you confirm the CLI is talking to the right control plane before
touching allowlists or env.

### Pick your app

Either scaffold a fresh Rust FaaS app:

```sh
edge init my-app --lang=rust --api=https://api.edgecloud.dev
cd my-app
```

Or copy the canonical sample as a starting point:

```sh
cp -r ../../samples/hello my-app
cd my-app
```

The scaffold and the sample have the same `Cargo.toml`,
`edge.toml`, and `wit/` shape (`samples/hello/src/lib.rs:22-90`). The
recipe below edits `src/lib.rs` in either case.

> `edge build` runs the cargo + `wasm-tools component new` wrap for
> you (issue #410); you don't need to invoke either directly. The
> scaffold's `Cargo.toml` ships with `wit-bindgen = "0.45"` as its only
> dependency, which is what `wasmtime 45.0.3` expects for
> `wasi:http@0.2.1`. Add `serde_json = { version = "1", features = ["std"] }`
> under `[dependencies]` if you're following the Neon recipe's
> response-parse step.

### Two scope distinctions to internalize up front

- **Egress is per-tenant.** One `edge egress set` covers every app the
  tenant owns. There is no per-app egress command.
- **Env is per-app.** `edge env set --app <name> <KEY> <VALUE>` writes
  to one row in `app_env` keyed by app name. Two apps of the same
  tenant do not see each other's env.

The recipes below use these correctly. The Troubleshooting section
calls out the common mistakes.

## Recipe: Neon serverless driver (Postgres over HTTPS)

The Neon HTTP driver (`@neondatabase/serverless` on npm, or just
hand-rolled `wasi::http`) speaks the SQL API at `https://<endpoint>/sql`.
A request is a `POST` with a JSON body of the form
`{"query": "...", "params": [...]}` and a `neon-connection-string`
header carrying the full `postgresql://...` URL with embedded auth.

### 1. Allow the egress hostnames

```sh
edge egress show
edge egress set *.neon.tech *.aws.neon.tech
```

> **Gotcha:** Neon's pooler endpoints are shaped
> `ep-<id>.<region>.aws.neon.tech` (for example
> `ep-cool-darkness-123456.us-east-2.aws.neon.tech`). The wildcard
> `*.neon.tech` does **not** match those — you need
> `*.aws.neon.tech` as a second entry. Verify against the actual
> endpoint URL in your Neon dashboard; the exact subdomain varies by
> region.

`edge egress set` replaces the entire allowlist. Always run
`edge egress show` first and re-add the existing entries plus the
new ones in one call. The allowlist accepts bare hostnames and
`*.suffix` patterns only — no `https://`, no path, no port.

### 2. Set the env var

```sh
edge env set --app my-app DATABASE_URL \
  "postgresql://neondb_owner:<password>@ep-cool-darkness-123456.us-east-2.aws.neon.tech/neondb?sslmode=require"
```

> Two caveats: `edge env set` does **not** retry transient 5xx
> (`edge-cli/src/commands/env.rs:22-30`) — fail-fast is intentional
> because a retried POST could double-apply the value. And env
> changes are only published to a running worker at the next
> `edge activate` (or `edge deploy`) — see the Troubleshooting
> section if the value doesn't seem to take effect.

### 3. Scaffold

Re-use the "Before you start" scaffold; nothing extra is needed for
Neon.

### 4. Replace `src/lib.rs`

```rust
//! Outbound HTTP to the Neon serverless SQL API.
//!
//! Replace samples/hello/src/lib.rs with this file (the module
//! doc-comment + wit_bindgen! block stays identical). The SQL is
//! hard-coded for the recipe; in real code, parse the incoming
//! request body and pass through.

#![no_main]

wit_bindgen::generate!({
    world: "edge-runtime-handler",
    path: "../../wit",
    generate_all,
});

use crate::exports::wasi::http::incoming_handler::Guest;
use crate::wasi::http::types::{
    Fields, IncomingRequest, Method, OutgoingBody, OutgoingRequest,
    OutgoingResponse, RequestOptions, ResponseOutparam, Scheme,
};
use crate::edge::cloud::env;

struct NeonHandler;
export!(NeonHandler);

// The `wasi:cli/run` export is part of the `edge-runtime-handler` world
// (it pulls in `wasi:cli/command`), but the host never calls it — the
// handler dispatch path goes through `wasi:http/incoming-handler`. We
// still have to provide a stub so wit-bindgen generates the export.
impl crate::exports::wasi::cli::run::Guest for NeonHandler {
    fn run() -> Result<(), ()> { Err(()) }
}

impl Guest for NeonHandler {
    fn handle(_req: IncomingRequest, out: ResponseOutparam) {
        // 1. Read the connection string from per-app env.
        let database_url = match env::get("DATABASE_URL") {
            Some(v) => v,
            None    => return_error(out, 500, "DATABASE_URL missing"),
        };
        // The host portion of the pooler URL is what wasi:http calls
        // the "authority". A real impl would parse the URL once and
        // cache it.
        let authority = match authority_from_url(&database_url) {
            Ok(a)  => a,
            Err(e) => return_error(out, 500, &format!("bad DATABASE_URL: {e}")),
        };

        // 2. Build the JSON body Neon's SQL API expects.
        let body_json = br#"{"query":"SELECT 1 AS x","params":[]}"#;

        // 3. Build the request.
        let headers = Fields::new();
        let _ = headers.set("content-type",       &[b"application/json".to_vec()]);
        let _ = headers.set("neon-connection-string", &[database_url.as_bytes().to_vec()]);
        let _ = headers.set("neon-allow-async",   &[b"false".to_vec()]);

        let req = OutgoingRequest::new(headers);
        let _ = req.set_method(&Method::Post);
        let _ = req.set_authority(Some(&authority));
        let _ = req.set_path_with_query(Some("/sql"));
        let _ = req.set_scheme(Some(&Scheme::Https));

        // 4. Write the body.
        let body = req.body().expect("request body");
        let stream = body.write().expect("output stream");
        let _ = stream.blocking_write_and_flush(body_json);
        drop(stream);
        let _ = OutgoingBody::finish(body, None);

        // 5. Submit. The worker's EgressHttpHooks::send_request
        //    (edge-runtime/src/runtime.rs:549-565) runs your
        //    EgressPolicy::check(url) here automatically — the
        //    *.neon.tech and *.aws.neon.tech entries from step 1
        //    are what permit this call.
        let opts = RequestOptions::new();
        let resp_fut = match crate::wasi::http::outgoing_handler::handle(req, Some(opts)) {
            Ok(f)  => f,
            Err(e) => return_error(out, 502, &format!("submit failed: {e:?}")),
        };

        // 6. Wait for the response.
        let poll = resp_fut.subscribe();
        loop {
            if poll.ready() { break; }
            crate::wasi::io::poll::poll_oneoff(&[poll.clone()]);
        }
        let resp = match resp_fut.get() {
            Some(Ok(Ok(r))) => r,
            _               => return_error(out, 502, "no response"),
        };
        let upstream_status = resp.status();
        // Response headers are not surfaced to the recipe's caller; the
        // body below already carries everything we want to forward.

        // 7. Drain the body.
        let resp_body = resp.consume().expect("consume body");
        let mut stream = resp_body.stream().expect("response stream");
        let mut buf: Vec<u8> = Vec::with_capacity(4096);
        loop {
            let (chunk, ended) = stream.blocking_read(4096);
            if chunk.is_empty() && ended { break; }
            buf.extend_from_slice(&chunk);
            if ended { break; }
        }

        // Parse the Neon SQL response: `{"rows":[{"x":1}],"fields":[...]}`.
        // For a real app, add `serde_json` to Cargo.toml under
        // `[dependencies]` and import as needed; the recipe's parsed
        // value is folded into the echoed body so the structure is
        // visible to a caller hitting `curl` against the FaaS endpoint.
        let parsed: String = serde_json::from_slice::<serde_json::Value>(&buf)
            .map(|v| v.to_string())
            .unwrap_or_else(|_| String::from_utf8_lossy(&buf).into_owned());

        // 8. Echo upstream status + parsed body back to our caller.
        let out_headers = Fields::new();
        let _ = out_headers.set("content-type", &[b"application/json".to_vec()]);
        let out_resp = OutgoingResponse::new(out_headers);
        let _ = out_resp.set_status_code(upstream_status);
        let oh = out_resp.body().expect("response body");
        let os = oh.write().expect("output stream");
        let _ = os.blocking_write_and_flush(parsed.as_bytes());
        drop(os);
        let _ = OutgoingBody::finish(oh, None);
        ResponseOutparam::set(out, Ok(out_resp));

        let _ = resp_body; // keep alive until end of scope
    }
}

fn return_error(out: ResponseOutparam, code: u16, msg: &str) {
    let h = Fields::new();
    let _ = h.set("content-type", &[b"text/plain".to_vec()]);
    let r = OutgoingResponse::new(h);
    let _ = r.set_status_code(code);
    let b = r.body().expect("body");
    let s = b.write().expect("stream");
    let _ = s.blocking_write_and_flush(msg.as_bytes());
    drop(s);
    let _ = OutgoingBody::finish(b, None);
    ResponseOutparam::set(out, Ok(r));
}

// Minimal URL parser: returns the host:port pair (the "authority" in
// `wasi:http` parlance) from a `postgresql://user:pass@host[:port]/db?query`
// or `https://host[:port]/path?query` URL. A production impl would pull
// in a `url` crate; this is enough for the recipe.
fn authority_from_url(s: &str) -> Result<String, String> {
    let rest = s.strip_prefix("postgresql://")
        .or_else(|| s.strip_prefix("postgres://"))
        .or_else(|| s.strip_prefix("https://"))
        .or_else(|| s.strip_prefix("http://"))
        .ok_or("missing scheme")?;
    // Drop the query string first; the userinfo and path both contain
    // characters that would confuse the subsequent splits.
    let without_query = rest.split('?').next().unwrap_or(rest);
    // Drop the userinfo (everything up to and including the last `@`).
    let without_userinfo = without_query.rsplitn(2, '@').next().unwrap_or(without_query);
    // Drop the path (everything from the first `/` onward).
    let host_port = without_userinfo.splitn(2, '/').next().unwrap_or(without_userinfo);
    if host_port.is_empty() { return Err("missing host".into()); }
    Ok(host_port.to_string())
}
```

Deploy and activate:

```sh
edge build
edge deploy
```

### Verification status

> **Status: VERIFIED against current `main` for the CLI commands and
> the `wasi::http::types` API surface. End-to-end run against a real
> Neon account: pending.** The Rust sample compiles against
> `samples/hello`'s scaffold, and the CLI / egress / env invocations
> match what `edge egress set`, `edge env set --app`, and `edge deploy`
> actually do. A human needs to confirm against a real Neon account:
>
> - [ ] `edge egress set *.neon.tech *.aws.neon.tech` is accepted by
>       `validateEgressAllowlist` (`edge-control-plane/internal/service/tenant.go:55-88`).
> - [ ] `edge env set --app my-app DATABASE_URL <value>` returns 204 on
>       `POST /api/v1/apps/{appName}/env`.
> - [ ] The Rust component built with `edge build` reaches
>       `ep-<id>.<region>.aws.neon.tech` without hitting "egress
>       denied" in the worker logs.
> - [ ] Response body is read end-to-end (`future_incoming_response`
>       polled to completion, body stream drained).
> - [ ] The worker egress source IP shows up in Neon's audit log as the
>       worker's IP, not the tenant's.

## Recipe: Turso (libSQL-over-HTTPS)

Turso exposes libSQL over HTTP at `https://<db-host>.turso.io/v2/pipeline`.
The body is a JSON object with a `statements` array; the response is a
matching `results` array. Authentication uses the database URL plus a
bearer token passed as the `Authorization` header (Turso's hosted
model issues a per-database JWT, separate from the URL).

### 1. Allow the egress hostnames

```sh
edge egress show
edge egress set *.turso.io
```

Turso hosted database hostnames follow the pattern `<db-slug>.turso.io`,
so `*.turso.io` covers them all. If you self-host libSQL on a custom
hostname, add that hostname too — bare hostnames are fine.

### 2. Set the env vars

```sh
edge env set --app my-app TURSO_DATABASE_URL "https://<db-slug>.turso.io"
edge env set --app my-app TURSO_AUTH_TOKEN  "<your-turso-jwt>"
```

### 3. Scaffold

Same as the Neon recipe — `edge init my-app --lang=rust` or copy
`samples/hello/`.

### 4. Replace `src/lib.rs`

Same `wit_bindgen!` block, `wasi:cli/run` stub, `Guest for TursoHandler`,
and `return_error` / `authority_from_url` helpers as the Neon recipe.
The `handle` body differs in three places — request body, auth header,
and request path.

```rust
impl Guest for TursoHandler {
    fn handle(_req: IncomingRequest, out: ResponseOutparam) {
        // 1. Read env.
        let db_url = match env::get("TURSO_DATABASE_URL") {
            Some(v) => v,
            None    => return_error(out, 500, "TURSO_DATABASE_URL missing"),
        };
        let token = match env::get("TURSO_AUTH_TOKEN") {
            Some(v) => v,
            None    => return_error(out, 500, "TURSO_AUTH_TOKEN missing"),
        };
        let authority = match authority_from_url(&db_url) {
            Ok(a)  => a,
            Err(e) => return_error(out, 500, &format!("bad TURSO_DATABASE_URL: {e}")),
        };

        // 2. Build the JSON body Turso's pipeline endpoint expects.
        let body_json = br#"{"statements":[{"q":"SELECT 1"}]}"#;

        // 3. Build the request.
        let headers = Fields::new();
        let _ = headers.set("content-type",  &[b"application/json".to_vec()]);
        let _ = headers.set("authorization", &[format!("Bearer {token}").into_bytes()]);

        let req = OutgoingRequest::new(headers);
        let _ = req.set_method(&Method::Post);
        let _ = req.set_authority(Some(&authority));
        let _ = req.set_path_with_query(Some("/v2/pipeline"));
        let _ = req.set_scheme(Some(&Scheme::Https));

        // 4-8. Write body, submit, poll, drain, echo — same shape as
        //      the Neon recipe, with the response treated as opaque
        //      JSON. Add `serde_json` to Cargo.toml under
        //      `[dependencies]` to parse the libSQL pipeline response
        //      (`{"results":[{"response":{"type":"ok","result":{...}}}]}`).
        let body = req.body().expect("request body");
        let stream = body.write().expect("output stream");
        let _ = stream.blocking_write_and_flush(body_json);
        drop(stream);
        let _ = OutgoingBody::finish(body, None);

        let opts = RequestOptions::new();
        let resp_fut = match crate::wasi::http::outgoing_handler::handle(req, Some(opts)) {
            Ok(f)  => f,
            Err(e) => return_error(out, 502, &format!("submit failed: {e:?}")),
        };

        let poll = resp_fut.subscribe();
        loop {
            if poll.ready() { break; }
            crate::wasi::io::poll::poll_oneoff(&[poll.clone()]);
        }
        let resp = match resp_fut.get() {
            Some(Ok(Ok(r))) => r,
            _               => return_error(out, 502, "no response"),
        };
        let upstream_status = resp.status();

        let resp_body = resp.consume().expect("consume body");
        let mut stream = resp_body.stream().expect("response stream");
        let mut buf: Vec<u8> = Vec::with_capacity(4096);
        loop {
            let (chunk, ended) = stream.blocking_read(4096);
            if chunk.is_empty() && ended { break; }
            buf.extend_from_slice(&chunk);
            if ended { break; }
        }

        let out_headers = Fields::new();
        let _ = out_headers.set("content-type", &[b"application/json".to_vec()]);
        let out_resp = OutgoingResponse::new(out_headers);
        let _ = out_resp.set_status_code(upstream_status);
        let oh = out_resp.body().expect("response body");
        let os = oh.write().expect("output stream");
        let _ = os.blocking_write_and_flush(&buf);
        drop(os);
        let _ = OutgoingBody::finish(oh, None);
        ResponseOutparam::set(out, Ok(out_resp));

        let _ = resp_body;
    }
}
```

### Verification status

> **Status: VERIFIED against current `main` for the CLI commands and
> the `wasi::http::types` API surface. End-to-end run against a real
> Turso database: pending.** A human needs to confirm:
>
> - [ ] `edge egress set *.turso.io` is accepted by
>       `validateEgressAllowlist`.
> - [ ] `edge env set --app my-app TURSO_AUTH_TOKEN <value>` returns
>       204.
> - [ ] The Rust component reaches `<db-slug>.turso.io` without "egress
>       denied".
> - [ ] The libSQL pipeline response is parsed (the recipe treats it as
>       opaque bytes; a real impl would `serde_json::from_slice`).

## Recipe: Upstash Redis (REST)

Upstash exposes Redis commands over a JSON-over-HTTPS REST API at
`https://<region>-<id>-<rest>.upstash.io`. The body is a JSON array of
command-and-args tuples (`["PING"]`, `["SET", "k", "v"]`,
`["GET", "k"]`); the response is a parallel JSON array of results.
Auth is a Bearer token in the `Authorization` header, issued when you
create the database.

### 1. Allow the egress hostnames

```sh
edge egress show
edge egress set *.upstash.io
```

> Upstash has shipped newer database instances under `*.upstash.com`
> in addition to the legacy `*.upstash.io`. Verify against the REST
> endpoint URL in your dashboard and allow whichever shape your
> instance uses — for example `edge egress set *.upstash.io *.upstash.com`.

### 2. Set the env vars

```sh
edge env set --app my-app UPSTASH_REDIS_REST_URL   "https://<region>-<id>-<rest>.upstash.io"
edge env set --app my-app UPSTASH_REDIS_REST_TOKEN "<your-upstash-rest-token>"
```

The URL contains no auth — the Bearer token is the only credential.

### 3. Scaffold

Same as the other recipes.

### 4. Replace `src/lib.rs`

Same `wit_bindgen!` block, `wasi:cli/run` stub, `Guest for UpstashHandler`,
and `return_error` / `authority_from_url` helpers as the Neon recipe.
Body and path differ.

```rust
impl Guest for UpstashHandler {
    fn handle(_req: IncomingRequest, out: ResponseOutparam) {
        // 1. Read env.
        let rest_url = match env::get("UPSTASH_REDIS_REST_URL") {
            Some(v) => v,
            None    => return_error(out, 500, "UPSTASH_REDIS_REST_URL missing"),
        };
        let token = match env::get("UPSTASH_REDIS_REST_TOKEN") {
            Some(v) => v,
            None    => return_error(out, 500, "UPSTASH_REDIS_REST_TOKEN missing"),
        };
        let authority = match authority_from_url(&rest_url) {
            Ok(a)  => a,
            Err(e) => return_error(out, 500, &format!("bad UPSTASH_REDIS_REST_URL: {e}")),
        };

        // 2. Build the body. Upstash accepts a JSON array of
        //    command-and-args tuples. ["PING"] is the simplest valid
        //    command and returns ["PONG"] on success.
        let body_json = br#"["PING"]"#;

        // 3. Build the request.
        let headers = Fields::new();
        let _ = headers.set("content-type",  &[b"application/json".to_vec()]);
        let _ = headers.set("authorization", &[format!("Bearer {token}").into_bytes()]);

        let req = OutgoingRequest::new(headers);
        let _ = req.set_method(&Method::Post);
        let _ = req.set_authority(Some(&authority));
        let _ = req.set_path_with_query(Some("/"));
        let _ = req.set_scheme(Some(&Scheme::Https));

        // 4-8. Write body, submit, poll, drain, echo — same shape as
        //      the Neon recipe. The Upstash REST response is a JSON
        //      array; parse it with serde_json if you need the typed
        //      value rather than the opaque echo.
        let body = req.body().expect("request body");
        let stream = body.write().expect("output stream");
        let _ = stream.blocking_write_and_flush(body_json);
        drop(stream);
        let _ = OutgoingBody::finish(body, None);

        let opts = RequestOptions::new();
        let resp_fut = match crate::wasi::http::outgoing_handler::handle(req, Some(opts)) {
            Ok(f)  => f,
            Err(e) => return_error(out, 502, &format!("submit failed: {e:?}")),
        };

        let poll = resp_fut.subscribe();
        loop {
            if poll.ready() { break; }
            crate::wasi::io::poll::poll_oneoff(&[poll.clone()]);
        }
        let resp = match resp_fut.get() {
            Some(Ok(Ok(r))) => r,
            _               => return_error(out, 502, "no response"),
        };
        let upstream_status = resp.status();

        let resp_body = resp.consume().expect("consume body");
        let mut stream = resp_body.stream().expect("response stream");
        let mut buf: Vec<u8> = Vec::with_capacity(4096);
        loop {
            let (chunk, ended) = stream.blocking_read(4096);
            if chunk.is_empty() && ended { break; }
            buf.extend_from_slice(&chunk);
            if ended { break; }
        }

        let out_headers = Fields::new();
        let _ = out_headers.set("content-type", &[b"application/json".to_vec()]);
        let out_resp = OutgoingResponse::new(out_headers);
        let _ = out_resp.set_status_code(upstream_status);
        let oh = out_resp.body().expect("response body");
        let os = oh.write().expect("output stream");
        let _ = os.blocking_write_and_flush(&buf);
        drop(os);
        let _ = OutgoingBody::finish(oh, None);
        ResponseOutparam::set(out, Ok(out_resp));

        let _ = resp_body;
    }
}
```

### Verification status

> **Status: UNVERIFIED — pending end-to-end run on the dev stack.**
> The CLI commands and the `wasi::http::types` shape are cross-checked
> against current `main`. The Upstash-specific bits (the
> `["PING"]` body, the `Bearer <token>` auth header, the path `/`)
> are drafted from the public Upstash REST docs, not from a verified
> run. A human needs to confirm:
>
> - [ ] `edge egress set *.upstash.io` is accepted by
>       `validateEgressAllowlist`.
> - [ ] `edge env set --app my-app UPSTASH_REDIS_REST_TOKEN <value>`
>       returns 204.
> - [ ] The Rust component reaches `<region>-<id>-<rest>.upstash.io`
>       without "egress denied".
> - [ ] The `["PING"]` body returns `["PONG"]` (or whatever the current
>       Upstash response shape is).
> - [ ] The worker egress source IP is the worker's IP.

## Latency: what to expect

HTTP drivers cost one outbound round-trip per query, with no
connection pooling beyond what the underlying hyper client keeps warm
between calls.

| Scenario | Expected p50 (rough) |
|---|---|
| Local SQLite via `EDGE_FS_PATH` (same worker) | sub-millisecond |
| HTTP DB driver, DB region colocated with worker region | 10–50 ms |
| HTTP DB driver, DB region cross-region from worker | 50–200 ms |
| HTTP DB driver, cold worker (TLS handshake) | +50–150 ms on first call |

If the workload is read-heavy and the dataset is small, **local
SQLite via `EDGE_FS_PATH` is dramatically faster and free**. The
serverless DB drivers pay off when the dataset is too large to fit on
one worker, or when multiple workers need to see the same state.

The cold-worker penalty is real. The hyper client inside
`edge-runtime` is shared across requests on a given worker, so warm
workers pay zero TLS-handshake overhead. Cold workers (right after
`edge activate`, or after worker eviction) pay one extra handshake on
the first outbound call.

## Local state: SQLite via `EDGE_FS_PATH`

When state belongs to the app — not shared across workers or tenants —
the runtime gives every app its own private directory under the
guest's filesystem root. This is the right tool for SQLite, DuckDB,
LMDB, and any single-node embedded store.

```sh
edge env set --app my-app DATABASE_PATH "/data/app.db"
```

```rust
// Open (or create) the app's SQLite file. The path resolves to
// {EDGE_FS_PATH}/{tenant_id}/{app_name}/data/app.db on the host.
use crate::wasi::filesystem::types::{
    Descriptor, OpenFlags, PathFlags,
};

let fs = crate::wasi::filesystem::types::filesystem();
let path = Path::new("/data");
match fs.create_directory_at(path) {
    Ok(_) => {}
    Err(e) if format!("{e:?}").contains("Exist") => {} // already exists
    Err(e) => return_error(out, 500, &format!("mkdir /data: {e:?}")),
}

let db_path = match env::get("DATABASE_PATH") {
    Some(p) => p,
    None    => return_error(out, 500, "DATABASE_PATH missing"),
};

let file = fs.open(
    Path::new(&db_path),
    OpenFlags::CREATE,
    PathFlags::empty(),
).expect("open db file");
let _desc: Descriptor = file;
```

The preopen is per-app by design (the runtime comment at
`edge-runtime/src/runtime.rs:1085-1095` spells this out — sharing
preopens across apps of the same tenant would let app A corrupt
app B's SQLite, which is miserable to debug). The mount point is
`{EDGE_FS_PATH}/{tenant_id}/{app_name}/`, and the guest's `/` is that
directory (`build_wasi_ctx_for_tenant` at `runtime.rs:1121`).

> **Caveat: node-local durability.** If the deployment is rebalanced
> to a different worker (issue #641 / region-aware capacity), the new
> worker sees an empty directory until something restores the file.
> Stop, crash, and rebalance on the **same** worker preserve the data
> (CLAUDE.md:463). For state that has to survive worker turnover,
> use one of the hosted-DB recipes above instead.

## When HTTP drivers aren't enough

### The JS gap

The TypeScript SDK (`@edgecloud/sdk`) currently exposes no `fetch`,
and `edge-js-runtime` does not yet import
`wasi:http::outgoing_handler::handle`. As a result, **these recipes
are Rust-only at this revision**. JS guests needing a Neon / Turso /
Upstash call have to either (a) shell out to a Rust shim over
`wasi:ipc`, or (b) wait for the JS shim tracked in issue #677.

That shim is the right next step — wiring `wasi::http::outgoing_handler`
into `edge-js-runtime/src/lib.rs:65-79` and exposing an
`EdgeCloud.http.fetch(url, init)` namespace via `register.rs`. The
recipe's Rust code sample is the shape a JS `fetch` polyfill would
wrap.

### The socket-TLS follow-up

Longer-term, the platform's native `wasi:sockets` path with a host-side
TLS layer will let components speak the Postgres wire protocol
directly — no JSON shim, no per-request HTTP round-trip. That work is
out of scope for this recipe; mentioned here so readers don't think
these recipes are the destination. The runtime already has a dormant
`HostnamePinned` egress variant behind `EDGE_EGRESS_HOSTNAME_PINNING`
(see CLAUDE.md "Egress hardening"); the socket-TLS work is the same
shape of problem one layer down.

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `egress denied` in worker logs | The allowlist match is exact: lowercase, no scheme, no port, no path. For Neon specifically, `*.neon.tech` does **not** match `ep-x.us-east-2.aws.neon.tech` — also allow `*.aws.neon.tech`. Run `edge egress show` to confirm what's stored. |
| `edge env set` returned 204 but the new value doesn't take effect | Env values are only re-published at the next `edge activate` (or `edge deploy`). Re-run the deploy after `edge env set`. |
| `edge env set` 5xx'd and the value is in a half-applied state | `edge env set` does not retry transient 5xx — see the comment at `edge-cli/src/commands/env.rs:22-30`. Re-run with the desired final value; CP-side `Idempotency-Key` for env is tracked separately. |
| I added one host and now the rest of the allowlist is gone | `edge egress set` replaces the entire allowlist (wire is `PUT /api/v1/egress`). Always `edge egress show` first, then re-add the existing entries plus the new ones in one call. |
| My JS code can't call `fetch` | There is no `fetch` in `@edgecloud/sdk` yet. See "The JS gap" above. Use a Rust shim or wait for the tracking issue. |
| I forget whether egress is per-tenant or per-app | Egress is per-tenant (`edge egress set`). Env is per-app (`edge env set --app <name>`). One allowlist governs all of that tenant's apps. |
| My SQLite file is empty after a redeploy | Preopens are per-worker, not shared (`EDGE_FS_PATH`, `edge-runtime/src/runtime.rs:1121`). If the deployment is rebalanced, the new worker starts empty. Pin the worker (issue #641) or move to a hosted DB. |
| `401 Unauthorized` on every CLI call | Run `edge auth signup` (or `edge auth login`) first. `~/.config/edgecloud/config.toml` must have a valid API key. `edge auth whoami` confirms the session. |
| The `Cargo.toml` workspace-isolation block keeps getting "re-resolved" by cargo | If you forked `samples/hello/` into your own dir, leave its `[workspace]` isolation block alone — without it, cargo walks up to the parent edgeCloud workspace and tries to resolve your crate against the host-only members (`edge-runtime`, `edge-cli`, `edge-worker`, ...), which can't be cross-compiled to `wasm32-unknown-unknown`. The block is also present in the `edge init --lang=rust` scaffold's `Cargo.toml`. |

## Related

- [CLAUDE.md](../CLAUDE.md) — `EDGE_FS_PATH` and the Egress hardening
  sections describe the runtime side of the recipes above.
- [docs/jwt-bootstrap.md](../jwt-bootstrap.md) — auth is required
  first; same `~/.config/edgecloud/config.toml` flow.
- [edge-cli/README.md](../../edge-cli/README.md) — full CLI reference
  (commands, flags, command-by-command gotchas).
- Issue #560 — env changes only publish to workers at activate time
  (the reason `edge deploy` follows `edge env set`).
- Issue #494 — the data-gravity objection these recipes partially
  defuse.
- Future: tracking issue #677 for the JS outbound-HTTP shim
  (`edge-js-runtime` + `@edgecloud/sdk`); the doc's "JS gap" section
  explains the scope and points at the same issue.
