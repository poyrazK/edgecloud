export const kv = {
  get: (key) => globalThis.EdgeCloud.kv.get(key),
  set: (key, value, ttlSecs) => globalThis.EdgeCloud.kv.set(value, key, ttlSecs ?? null),
  delete: (key) => globalThis.EdgeCloud.kv.delete(key),
  listKeys: (prefix) => globalThis.EdgeCloud.kv.listKeys(prefix),
  getMany: (keys) => globalThis.EdgeCloud.kv.getMany(keys),
  setMany: (items) => globalThis.EdgeCloud.kv.setMany(items.map(([k, v, t]) => [k, v, t ?? null])),
  deleteMany: (keys) => globalThis.EdgeCloud.kv.deleteMany(keys),
  exists: (key) => globalThis.EdgeCloud.kv.exists(key),
  clear: () => globalThis.EdgeCloud.kv.clear(),
};
