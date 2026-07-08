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
