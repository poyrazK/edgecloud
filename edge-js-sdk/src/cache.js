export const cache = {
  get: (key) => globalThis.EdgeCloud.cache.get(key),
  set: (key, value, ttlSecs) => globalThis.EdgeCloud.cache.set(value, key, ttlSecs ?? null),
  delete: (key) => globalThis.EdgeCloud.cache.delete(key),
  clear: () => globalThis.EdgeCloud.cache.clear(),
  size: () => globalThis.EdgeCloud.cache.size(),
  exists: (key) => globalThis.EdgeCloud.cache.exists(key),
  listKeys: (prefix) => globalThis.EdgeCloud.cache.listKeys(prefix),
  getMany: (keys) => globalThis.EdgeCloud.cache.getMany(keys),
  setMany: (items) => globalThis.EdgeCloud.cache.setMany(items.map(([k, v, t]) => [k, v, t ?? null])),
  deleteMany: (keys) => globalThis.EdgeCloud.cache.deleteMany(keys),
};
