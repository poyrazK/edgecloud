// Issue #448 — long-running JS WebSocket echo sample.
//
// The Rust shim (samples/hello-js-ws/src/lib.rs) generates the
// `edge-runtime` (long-running) world's WIT bindings, builds a QuickJS
// runtime in-process, evaluates this bundle once, then calls
// `globalThis.start({ wsPort })`. The shim then loops
// `runtime.idle()` to keep the long-running world alive.
//
// `wsPort` is the worker's `EDGE_WS_PORT` env value — the supervisor
// allocates it from the PortPool and threads it in
// (edge-worker/src/supervisor.rs:972-978, 1217-1218). The
// `EdgeCloud.websocket.listen(wsPort)` call below delegates to the
// host's `WebSocket::listen` (edge-runtime/src/interfaces/websocket.rs:158-168),
// which binds the actual TcpListener.

import { websocket, observe } from '@edgecloud/sdk';

globalThis.start = function ({ wsPort }) {
  observe.emitLog('info', `hello-js-ws: listening on ${wsPort}`, []);
  const listener = websocket.listen(Number(wsPort));
  observe.emitLog('info', `hello-js-ws: listener id=${listener}`, []);

  for (;;) {
    let conn;
    try {
      conn = websocket.accept(listener);
    } catch (e) {
      observe.emitLog('error', `accept failed: ${e}`, []);
      // Don't busy-loop on persistent accept failures.
      continue;
    }
    try {
      const res = websocket.receive(conn);
      if (res.close) {
        observe.emitLog('info', `peer closed: ${res.close.code}`, []);
        continue;
      }
      websocket.send(conn, res.data, res.kind);
    } catch (e) {
      observe.emitLog('warn', `echo error: ${e}`, []);
    } finally {
      try {
        websocket.close(conn, { code: 1000, reason: 'ok' });
      } catch (_) {
        // peer may have already closed; ignore.
      }
    }
  }
};
