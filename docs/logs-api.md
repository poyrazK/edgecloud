# Logs API (`GET /api/v1/apps/{appName}/logs`)

The tenant-facing log read surface. This doc is the human companion to the
canonical spec at [`edge-control-plane/docs/api/openapi.yaml`](../edge-control-plane/docs/api/openapi.yaml)
(LogEntry / LogListResponse / `listLogsForApp`); when the two disagree, the
OpenAPI is authoritative.

## Auth and tenant isolation

The route is **tenant-authenticated**: callers present an API key (`Bearer`),
the auth middleware (`edge-control-plane/internal/middleware/auth.go`)
SHA-256-hashes it, looks up the row in `api_keys`, and stamps `tenant_id` on
the request context. The handler (`edge-control-plane/internal/handler/logs.go:56`)
reads the tenant off the context, not the request, and forwards it as the
first SQL parameter — `WHERE tenant_id = $1 AND app_name = $2`
(`edge-control-plane/internal/repository/log_entry.go:222`). Two apps that
share a name under different tenants live in disjoint result sets.

The cursor is **not signed**. Authorization continues to come from the
authenticated `tenant_id` plus the SQL boundary; a forged or replayed cursor
cannot cross tenants or apps. The cursor is treated purely as an ordering
hint (see "Cursor contract" below).

## Retention

Tenant logs are TTL'd by `LogGC` (`edge-control-plane/internal/service/log_gc.go`)
which deletes rows older than `LOG_RETENTION` (default 7 days) every
`LOG_GC_INTERVAL`. The retention GC is independent of the read endpoint —
clients cannot request rows beyond the retention window.

## Parameters

| Name      | Type        | Required | Notes |
|-----------|-------------|----------|-------|
| `appName` | path        | yes      | App name within the caller's tenant. |
| `since`   | RFC3339     | no       | Lower bound on `ts`. Absent → server default (5 minutes). Future-dated → 400. Malformed → 400. |
| `until`   | RFC3339     | no       | Optional upper bound on `ts` (`ts <= until`). Future-dated → 400. `until < since` → 400. Absent → unbounded (live). |
| `level`   | enum        | no       | Minimum severity, inclusive (`trace`, `debug`, `info`, `warn`, `error`). Unknown → 400. |
| `limit`   | int         | no       | [1, 1000], clamped. Default 100. |
| `cursor`  | opaque str  | no       | Preferred pagination token. Mutually exclusive with `offset` (including `offset=0`). Malformed / unsupported-version → 400. |
| `offset`  | int         | no       | **Deprecated.** Mutually exclusive with `cursor`. See "Deprecation schedule" below. |

The cutoff for `since` is computed server-side as `NOW() - make_interval(secs => $1)`
(`edge-control-plane/internal/repository/log_entry.go:235`) so a wall-clock
skew between the control plane and the database does not affect what is
returned. The handler captures `now := time.Now()` once per request and uses
the same value when re-rendering the RFC3339 echo so parsing and echoing
never drift.

The `since` echo in the response reflects the **application server's clock**,
not the DB clock — they are different `time.Now()` instantiations across two
processes (the API and Postgres). A client that pins a follow-up request to
that string should expect drift bounded by network RTT and NTP sync, which
is fine for human-driven follow but not for tight machine loops. For those,
prefer `--cursor`/`next_cursor`, which is server-relative and stable.

## Level ordering

Severity ordering is defined in `service.LogLevelOrdinal`
(`edge-control-plane/internal/service/logs.go`). `trace` is the lowest
severity and is honored end-to-end. `level=warn` is inclusive: it returns
`warn` and `error`. The repository applies the level filter as an
`AND level = ANY($N::text[])` recheck on the rows the index already returned
— adding `level` to the index columns would slow ingest for marginal
read-path gain.

## Response envelope

```jsonc
{
  "items": [ /* LogEntry, newest first */ ],
  "limit": 100,                    // effective ceiling after clamp
  "since": "2026-06-24T11:55:00Z", // echo of the lower bound (empty when
                                   //   no `since` was supplied AND no
                                   //   default was applied)
  "next_cursor": "eyJ2I...",       // null on final page
  "next_offset": 100               // null on final page; null in cursor mode
                                   //   (DEPRECATED — see below)
}
```

Each `LogEntry` carries: `id`, `tenant_id`, `deployment_id`, `app_name`,
`worker_id`, `region`, `level`, `message`, `labels`, `ts`. The list is
ordered by `(ts DESC, id DESC)`. Rows that share a timestamp have a
deterministic tie-break, which is what makes keyset pagination stable.

## Cursor contract

The cursor is opaque to clients — encoded with
`base64.RawURLEncoding` (URL-safe, no padding) of a private v1 JSON payload:

```jsonc
{ "v": 1, "ts": "2026-06-24T12:00:00.123456Z", "id": 12345 }
```

Codec lives at `edge-control-plane/internal/service/logs_cursor.go`. The
service layer (`LogService.ListByTenantApp`, `edge-control-plane/internal/service/logs.go:148`)
decodes the cursor and threads `(CursorTS, CursorID)` into the repository;
the repository applies the strict row-value predicate
`AND (ts, id) < ($N::timestamptz, $N::bigint)`
(`edge-control-plane/internal/repository/log_entry.go:252`).

A cursor is **scoped to an unchanged `(tenant, app, since, until, level)`
query**. Clients that reuse a cursor with a different filter set may
silently skip rows that fall outside the new filter — the strict-tuple
predicate only narrows, it never broadens.

`cursor` and `offset` are mutually exclusive at the handler
(`edge-control-plane/internal/handler/logs.go`) — any request that supplies
both, including `offset=0`, returns 400. The handler also returns 400 on
malformed or unsupported-version cursors.

A cursor whose `ts` is **strictly after** the supplied `until` returns 400
(`internal/service/logs.go` — `if !q.Until.IsZero() && cursorTS.After(q.Until)`,
typed as `ErrInvalidLogCursor`). Without this check the strict-tuple
predicate and the `ts <= until` clause would silently produce an empty page,
which is indistinguishable from "no more rows" — the empty page is the
correct response to a stale cursor, but a cursor-supplied-after-`until`
request is a client bug that should be flagged loudly. Cursors whose `ts`
equals `until` are accepted (the `ts <= until` clause still has rows to return
when `(ts,id)` is strictly less).

## Pagination semantics

`LogService.ListByTenantApp` requests `effectiveLimit + 1` rows from the
repository, trims to the effective limit, and derives `hasMore` from the
extra row (`edge-control-plane/internal/service/logs.go:189`). This fixes
the legacy bug where `len(entries) == limit` would promise another page
when the final page was exactly full.

| Request type      | `next_cursor`              | `next_offset` (deprecated) |
|-------------------|----------------------------|----------------------------|
| First page, more  | last row's cursor          | `limit`                    |
| First page, final | `null`                     | `null`                     |
| `offset=N`, more  | last row's cursor          | `N + limit`                |
| `offset=N`, final | `null`                     | `null`                     |
| `cursor=…`, more  | last row's cursor          | `null` (suppressed)        |
| `cursor=…`, final | `null`                     | `null`                     |

## Deprecation schedule

`offset` / `next_offset` are retained for one release to give clients time to
migrate. The follow-up issue (open at the time of writing) will remove the
`offset` query parameter, the `next_offset` response field, the server's
offset branch in `LogService.ListByTenantApp` and
`LogEntryRepository.ListByTenantApp`, the `LogListFilter.Offset` field, and
the CLI's `--offset` flag plus its offset-fallback next-page hint. Until
then, clients using `offset` keep working unchanged.

## Index decision (and why no new migration)

The legacy `idx_logs_tenant_app_ts (tenant_id, app_name, ts DESC)` from
[`migrations/005_logs.up.sql`](../edge-control-plane/migrations/005_logs.up.sql)
is sufficient. We measured against PostgreSQL 16 with ~100k seeded rows
(including a high-volume tenant/app and many equal/near-equal timestamps)
and captured `EXPLAIN (ANALYZE, BUFFERS)` for the first-page and cursor-page
plans with and without the `level`/`time` filters. The plan is a bounded
range scan on `idx_logs_tenant_app_ts` plus a small incremental sort on
`(ts, id)` for the `LIMIT` window — no sequential scan, no full
matching-partition sort. The new build-tagged integration tests pin the
plan invariants (no `Seq Scan on logs`, no `Sort`, must contain
`idx_logs_tenant_app_ts`) so the planner can never silently regress.

Adding `(id DESC)` as a fourth index column would force every worker
ingest write (PR #98) to touch an extra sort-key column for marginal
read-path gain — not worth the ingest cost.

## CLI examples

```bash
# Default 5-minute window
edge logs myapp

# Explicit time window (RFC3339, server rejects malformed/future values)
edge logs myapp --since 2026-06-24T11:00:00Z --until 2026-06-24T12:00:00Z

# Cursor-based pagination (preferred)
edge logs myapp --limit 50
# … response contains `next_cursor`; follow with:
edge logs myapp --limit 50 --cursor 'eyJ2I…'

# Level filter (inclusive minimum)
edge logs myapp --level warn

# Live follow (SIGINT-aware, drains cursor chain before advancing the
# watermark so bursts larger than one page are not truncated)
edge logs myapp --follow

# Legacy offset — still works for one release
edge logs myapp --offset 100
```

### <a id="follow-burst-draining"></a>Follow burst-draining

`edge logs --follow` polls every 2s. Each tick it issues one request, then
**drains the cursor chain** by following `next_cursor` until the server
returns a final page. Only after the chain is fully drained does the
`since` watermark advance to the largest `ts` observed in this tick. The
`boundary_ids` set captures exactly the rows sharing the new watermark —
every other row in the window has a strictly greater `ts` and will not be
re-served, so dedupe is reduced to a tiny set per tick instead of every
row ever printed (which would be unbounded for a long follow).

The drain-then-advance discipline matters when one tick contains a burst
larger than `--limit`. A burst of 500 rows with `--limit=100` produces 5
pages in the same tick; printing only the first page would advance the
watermark past 400 unread rows. Draining first keeps the watermark grounded
in the last row actually printed.

Cross-reference: the implementation is at
[`edge-cli/src/commands/logs.rs`](../edge-cli/src/commands/logs.rs) — see
the `run_follow` watermark-advance block and the `entry.ts > *cur` strict
comparison comment above it.

## Test coverage

- **Go unit**:
  [`internal/handler/logs_test.go`](../edge-control-plane/internal/handler/logs_test.go)
  pins handler-level invariants (explicit `since` produces a positive
  lookback; `until` validation; cursor+offset including `offset=0`; rich
  envelope retained; JWT/context tenant, not a client-provided field, is
  forwarded to the service).
- **Go unit**:
  [`internal/service/logs_test.go`](../edge-control-plane/internal/service/logs_test.go)
  pins `limit+1` semantics, cursor-mode suppression of `next_offset`,
  cursor generation from the last visible row, `until` propagation,
  **cursor + level combined propagation**, **cursor-after-`until`
  rejection** (`TestLogService_RejectsCursorAfterUntil` — the typed
  `ErrInvalidLogCursor` short-circuits before any repo call), and the
  existing defaults / severity translation.
- **Go unit**:
  [`internal/service/logs_cursor_test.go`](../edge-control-plane/internal/service/logs_cursor_test.go)
  pins the v1 roundtrip, URL-safe/no-padding encoding, malformed
  base64/JSON, unsupported version, zero timestamp, and non-positive ID.
- **Go unit**:
  [`internal/repository/log_entry_test.go`](../edge-control-plane/internal/repository/log_entry_test.go)
  pins the SQL shape (`tenant_id`/`app_name` first, upper/lower time
  bounds, level, cursor tuple predicate, `ORDER BY ts DESC, id DESC`,
  `LIMIT limit+1`, offset absent in cursor mode, retained otherwise).
- **PostgreSQL integration** (build tag `integration`,
  [`internal/repository/log_entry_integration_test.go`](../edge-control-plane/internal/repository/log_entry_integration_test.go)):
  pins tenant isolation, equal-timestamp cursor stability, concurrent-insert
  stability, combined filter propagation, and `EXPLAIN` evidence that the
  planner uses `idx_logs_tenant_app_ts` without seq scan or full sort.
- **Rust CLI** ([`edge-cli/tests/logs.rs`](../edge-cli/tests/logs.rs)):
  pins URL query encoding, cursor-preferred / offset-fallback hints, rich
  JSON output, clap mutual exclusion (`--cursor`/`--offset`, `--follow`
  with `--until`/`--cursor`/`--offset`), `--until` forwarding, stable
  filter preservation, burst draining over multiple pages,
  equal-timestamp boundary dedupe, Ctrl-C between pages, **stale-cursor
  empty-page exit** (`logs_stale_cursor_returns_empty_page_exits_cleanly`),
  and pure `interruptible_sleep` SIGINT-flag semantics
  (`interruptible_sleep_returns_early_when_stop_is_set`).

## What is explicitly out of scope

- Cursor signing / HMAC. The cursor is a hint; authorization comes from the
  tenant-scoped SQL boundary. Adding a signature would force every server
  bump to coordinate key roll with clients.
- SSE / WebSocket streaming. Pagination already supports `edge logs
  --follow` without long-lived connections.
- Label / regex filters. Add a follow-up issue if needed.
- Removing `offset`. Tracked in a separate follow-up issue.
