# `@edgecloud/sdk`

JS-side shim package for [edgeCloud](https://github.com/poyrazK/edgecloud) JavaScript guests.
The SDK is a thin layer of JS modules that delegate to `globalThis.EdgeCloud.*` host
functions injected by the `edge-js-runtime` QuickJS host at request time. There are **no
network calls, no native bindings, no Javy dependency** — the package is a pure ES module
shim that exists so JS guests can `import { kv, time, ... } from "@edgecloud/sdk"`
idiomatically.

## Versioning

This package follows **strict 0.2.x compatibility** while the SDK is in the v0.2 line.
Every `0.2.x` release is wire-compatible; the next break will land as **0.3.0**. The
[`edge-cli`](../edge-cli) `edge init --lang=js` scaffold pins `"@edgecloud/sdk": "^0.2.0"`,
which under npm's semver rules means any `0.2.x` (the `^0.2.0` syntax is special-cased for
the `0.x.y` range; it does NOT allow `0.3.0`).

When you cut a breaking change:

1. Bump the `version` field in [`package.json`](./package.json) to `0.3.0`.
2. Publish to npm: `cd edge-js-sdk && npm publish --access public`.
3. Update `edge-cli/src/commands/init.rs::PACKAGE_JSON_TEMPLATE` from `^0.2.0` to `^0.3.0`
   (or `~0.3.0` if you want a tighter pin).
4. Open a PR that bundles the version bumps.

## Modules

| Module | WIT interface | Notes |
|---|---|---|
| `kv` | `edge:cloud/kv-store@0.2.0` | argument order is intentionally swapped in the shim to match the rquickjs closure arity |
| `cache` | `edge:cloud/cache@0.2.0` | same shape as `kv` |
| `observe` | `edge:cloud/observe@0.2.0` | exposes `emitLog` and the metrics counters |
| `time` | `edge:cloud/time@0.2.0` | `now`/`sleep` |
| `scheduling` | `edge:cloud/scheduling@0.2.0` | delayed + repeating tasks |
| `process` | `edge:cloud/process@0.2.0` | env vars + args + cwd |
| `websocket` | `edge:cloud/websocket@0.2.0` | per RFC 6455; uses `{ok}/{err}` result shapes |

## Guest contract

The QuickJS host (`edge-js-runtime`) registers `globalThis.EdgeCloud.*` once per request
and then evaluates the guest's bundled JS. The guest is expected to define:

```js
globalThis.handleRequest = (req) => ({
  status: 200,
  body: "...",
  contentType: "application/json", // optional; defaults to text/plain (issue #428)
});
```

…where `req` is `{ method, path, headers, body }` (plain object, not a `Request`).

## Local development

```sh
# In a monorepo checkout, link the in-tree SDK into a sample:
(cd edge-js-sdk && npm link)
(cd samples/hello-js && npm link @edgecloud/sdk && edge build --lang=js)
```

Outside-the-monorepo users install via npm directly: `npm install @edgecloud/sdk` after
running `edge init my-project --lang=js`.

## Layout

```
edge-js-sdk/
├── package.json      # @edgecloud/sdk, version 0.2.x
├── README.md         # this file
└── src/
    ├── index.js      # barrel: re-exports all 7 modules
    ├── index.d.ts    # TypeScript declarations
    ├── kv.js
    ├── cache.js
    ├── observe.js
    ├── time.js
    ├── scheduling.js
    ├── process.js
    └── websocket.js
```
