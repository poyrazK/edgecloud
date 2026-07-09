import { kv, time, observe } from '@edgecloud/sdk';

function handleRequest(req) {
  const now = time.now();

  observe.emitLog("info", `JS handler hit: ${req.path}`, [
    ["method", req.method],
  ]);

  if (req.method === "POST" && req.path === "/kv") {
    const body = JSON.parse(req.body || "{}");
    if (body.key && body.value) {
      kv.set(body.key, new TextEncoder().encode(body.value));
      return { 
        status: 201, 
        body: JSON.stringify({ stored: body.key }),
        contentType: "application/json"
      };
    }
    return { 
      status: 400, 
      body: JSON.stringify({ error: "missing key or value" }),
      contentType: "application/json"
    };
  }

  if (req.path.startsWith("/kv/")) {
    const key = req.path.slice(4);
    const val = kv.get(key);
    if (val) {
      return { 
        status: 200, 
        body: new TextDecoder().decode(val),
        contentType: "text/plain"
      };
    }
    return { 
      status: 404, 
      body: JSON.stringify({ error: "not found" }),
      contentType: "application/json"
    };
  }

  if (req.path === "/ws") {
    // Issue #448 — the FaaS (Handler) execution model can't accept
    // WebSocket upgrades because the host owns the TCP listener and
    // the request-scoped JS runtime is destroyed between requests.
    // Return 426 Upgrade Required so clients see a clear error
    // rather than guessing why the connection failed; the actual WS
    // echo is in `samples/hello-js-ws/` (long-running model).
    return {
      status: 426,
      body: JSON.stringify({
        error: "websocket unavailable on FaaS handler — deploy a long-running sample instead",
        sample: "hello-js-ws",
      }),
      contentType: "application/json",
    };
  }

  return {
    status: 200,
    body: JSON.stringify({ hello: "world", path: req.path, now: Number(now) }),
    contentType: "application/json"
  };
}

globalThis.handleRequest = handleRequest;
