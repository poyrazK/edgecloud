export const time = {
  now: () => globalThis.EdgeCloud.time.now(),
  sleep: (durationMs) => globalThis.EdgeCloud.time.sleep(durationMs),
  resolution: () => globalThis.EdgeCloud.time.resolution(),
};
