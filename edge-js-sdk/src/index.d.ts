export interface KvStore {
  get(key: string): Uint8Array | null;
  set(key: string, value: Uint8Array, ttlSecs?: number): void;
  delete(key: string): void;
  listKeys(prefix: string): string[];
  getMany(keys: string[]): (Uint8Array | null)[];
  setMany(items: [string, Uint8Array, number?][]): void;
  deleteMany(keys: string[]): void;
  exists(key: string): boolean;
  clear(): void;
}

export interface Cache {
  get(key: string): Uint8Array | null;
  set(key: string, value: Uint8Array, ttlSecs?: number): void;
  delete(key: string): void;
  clear(): void;
  size(): number;
  exists(key: string): boolean;
  listKeys(prefix: string): string[];
  getMany(keys: string[]): (Uint8Array | null)[];
  setMany(items: [string, Uint8Array, number?][]): void;
  deleteMany(keys: string[]): void;
}

export interface LogRecord {
  timestampMs?: number;
  level?: 'error' | 'warn' | 'info' | 'debug' | 'trace';
  message: string;
  labels?: Record<string, string> | [string, string][];
}

export interface Observe {
  incrementCounter(name: string, labels?: Record<string, string> | [string, string][]): void;
  recordGauge(name: string, value: number, labels?: Record<string, string> | [string, string][]): void;
  recordHistogram(name: string, value: number, labels?: Record<string, string> | [string, string][]): void;
  emitLog(level: 'error' | 'warn' | 'info' | 'debug' | 'trace' | string, message: string, labels?: Record<string, string> | [string, string][]): void;
  emitLogRecord(record: LogRecord): void;
}

export interface Time {
  now(): bigint;
  sleep(durationMs: bigint | number): void;
  resolution(): bigint;
}

export interface Scheduling {
  scheduleOnce(delayMs: bigint | number, payload: Uint8Array | string): string;
  scheduleRepeating(intervalMs: bigint | number, payload: Uint8Array | string): string;
  cancelScheduled(id: string): void;
}

export interface Process {
  getEnv(key: string): string | null;
  getAllEnv(): Record<string, string>;
  getArgs(): string[];
  cwd(): string;
  exit(code: number): never;
}

export const kv: KvStore;
export const cache: Cache;
export const observe: Observe;
export const time: Time;
export const scheduling: Scheduling;
export const process: Process;

// ─── websocket (issue #422) ────────────────────────────────────────
//
// Sync surface; the WIT v0.2 has no async ABI. `accept` and `receive`
// block the QuickJS event loop — that is by design at v0.2 and matches
// every other `EdgeCloud.*` method. Async ABI migration to v0.3 is
// tracked separately.

export type MessageType = "text" | "binary" | "ping" | "pong" | "close";

export interface CloseInfo {
  code: number;
  reason: string;
}

export interface WebSocketReceiveResult {
  data?: Uint8Array;
  kind?: MessageType;
  close?: CloseInfo;
}

export interface Websocket {
  /**
   * Sync. Binds the WebSocket listener. Throws with the host-reported
   * reason on bind failure (e.g. port already in use).
   */
  listen(port: number): number;

  /**
   * Sync. Blocks until a client connects on the listener. Throws on
   * accept failure.
   */
  accept(listenerId: number): number;

  /**
   * Sync. `data` may be a Uint8Array or string (UTF-8 encoded). Throws
   * on bad `messageType` or send failure. The send-failure reason is
   * NOT surfaced at the JS layer in v0.2 — WIT `send` is declared as
   * bare `result`, and the bindgen-shadowed Host impl in
   * `edge-runtime/src/runtime.rs:1122` `map_err(|_| ())` the reason
   * away. v0.3 work will fix this.
   */
  send(
    connId: number,
    data: Uint8Array | string,
    messageType: MessageType,
  ): void;

  /**
   * Sync. Blocks until the next complete message arrives, or the peer
   * closes. Returns `{ data, kind }` on a normal message; `{ close: {
   * code, reason } }` on a peer close frame. Callers should check
   * `if (res.close)` first.
   */
  receive(connId: number): WebSocketReceiveResult;

  /**
   * Sync. Throws on close failure (reason not surfaced; same caveat
   * as `send`).
   */
  close(connId: number, info: CloseInfo): void;
}

export const websocket: Websocket;
