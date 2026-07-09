// @edgecloud/sdk — websocket module.
//
// Sync surface only. WIT v0.2 has no async ABI (deferred to v0.3 per
// `edge-runtime/src/wit/edge-cloud.wit:104-113`), so every
// `EdgeCloud.websocket.*` call blocks the QuickJS event loop. The
// `accept` and `receive` methods are inherently blocking at the
// protocol level too — a healthy WebSocket handler is expected to be
// structured around that, not to share runtime with concurrent work.
//
// Tokens (listener ids, connection ids) are opaque u32 handles
// allocated by the host. They are NOT file descriptors and should
// never be passed across requests — each component instance owns its
// own lifetime.

/**
 * Listen for WebSocket connections on the given port.
 * Returns an opaque listener handle.
 * @param {number} port Port to bind (0 = ephemeral).
 * @returns {number} Listener id; throws on bind failure (e.g. port in
 *   use) with the host-reported reason.
 */
function listen(port) {
  return globalThis.EdgeCloud.websocket.listen(port);
}

/**
 * Accept the next incoming WebSocket connection on a listener.
 * Blocks until a client connects.
 * @param {number} listenerId Listener handle from `listen()`.
 * @returns {number} Connection id; throws on accept failure.
 */
function accept(listenerId) {
  return globalThis.EdgeCloud.websocket.accept(listenerId);
}

/**
 * Send a single WebSocket message frame.
 *
 * `messageType` is one of `"text" | "binary" | "ping" | "pong" | "close"`.
 * For terminating a connection, prefer the dedicated `close(connId,
 * info)` method — it sets a close code/reason, which `send(..., "close")`
 * does not.
 *
 * Throws on bad `messageType` or send failure (WIT v0.2 limitation:
 * errors from `send` are bare unit on the host side; see
 * `edge-js-runtime/src/edge_modules.rs::register_websocket` notes).
 *
 * @param {number} connId Connection handle from `accept()`.
 * @param {Uint8Array | string} data Frame payload. Strings are encoded
 *   as UTF-8 text frames.
 * @param {"text"|"binary"|"ping"|"pong"|"close"} messageType
 */
function send(connId, data, messageType) {
  const bytes =
    typeof data === "string" ? new TextEncoder().encode(data) : data;
  globalThis.EdgeCloud.websocket.send(bytes, connId, messageType);
}

/**
 * Receive the next complete WebSocket message.
 *
 * Returns either `{ data, kind }` on a normal message, or `{ close:
 * { code, reason } }` on a peer-initiated close frame. JS callers
 * should check `if (res.close)` first.
 *
 * @param {number} connId Connection handle from `accept()`.
 * @returns {{ data: Uint8Array; kind: "text"|"binary"|"ping"|"pong" } |
 *          { close: { code: number; reason: string } }}
 */
function receive(connId) {
  return globalThis.EdgeCloud.websocket.receive(connId);
}

/**
 * Close the WebSocket connection with a code + reason. Throws on close
 * failure (WIT v0.2 limitation; reason not surfaced).
 *
 * @param {number} connId Connection handle from `accept()`.
 * @param {{ code: number; reason: string }} info Close code and reason.
 *   Standard close codes are documented in RFC 6455 §7.4 — `1000` for
 *   normal closure, `1001` for going-away, etc.
 */
function close(connId, info) {
  globalThis.EdgeCloud.websocket.close(connId, info);
}

export const websocket = {
  listen,
  accept,
  send,
  receive,
  close,
};
