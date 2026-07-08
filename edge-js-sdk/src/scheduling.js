export const scheduling = {
  scheduleOnce: (delayMs, payload) => {
    const uint8Payload = typeof payload === "string" ? new TextEncoder().encode(payload) : payload;
    return globalThis.EdgeCloud.scheduling.scheduleOnce(delayMs, uint8Payload);
  },
  scheduleRepeating: (intervalMs, payload) => {
    const uint8Payload = typeof payload === "string" ? new TextEncoder().encode(payload) : payload;
    return globalThis.EdgeCloud.scheduling.scheduleRepeating(intervalMs, uint8Payload);
  },
  cancelScheduled: (id) => globalThis.EdgeCloud.scheduling.cancelScheduled(id),
};
