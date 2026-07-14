// Thin shim over `globalThis.EdgeCloud.http.fetch`. Issue #550 added
// `EdgeCloud.http.fetch` to edge-js-runtime so JS guests can make
// outbound `wasi:http` calls (Neon / Turso / Upstash HTTP drivers).
// This file mirrors the pattern of `process.js` — re-shape the
// host-side error envelope so JS callers get a real `Error` instead of
// the host's `{ code, message }` JSON string.
//
// Host surface (see edge-js-runtime/src/register.rs::register_http):
//   globalThis.EdgeCloud.http.fetch(url, init?)
//   → { status: number, headers: Record<string,string>, body: string }
// Throws with a JS `Error` whose `.message` carries the host's
// `code: message` pair (e.g. `egress-denied: not on allowlist`).
//
// Sync; no streams, no ReadableStream. Bodies are returned as strings
// (UTF-8 lossy on the host side). For binary responses, the caller
// should base64-decode or hex-decode `body` itself.
export const http = {
  fetch: (url, init) => {
    let res;
    try {
      res = globalThis.EdgeCloud.http.fetch(url, init);
    } catch (e) {
      // Host emits a JSON-string message like
      //   {"code":"egress-denied","message":"..."}
      // Re-shape as Error so the caller's `instanceof Error` and
      // `.code` lookups behave the way every other namespace does.
      const raw = e && typeof e.message === "string" ? e.message : String(e);
      const parsed = tryParseHostError(raw);
      const err = new Error(parsed.message);
      err.code = parsed.code;
      err.cause = e;
      throw err;
    }
    return res;
  },
};

function tryParseHostError(raw) {
  try {
    const o = JSON.parse(raw);
    if (o && typeof o === "object" && typeof o.code === "string") {
      return { code: o.code, message: typeof o.message === "string" ? o.message : raw };
    }
  } catch {
    // fall through
  }
  return { code: "unknown", message: raw };
}