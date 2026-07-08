function normalizeLabels(labels) {
  if (!labels) return [];
  if (Array.isArray(labels)) return labels;
  return Object.entries(labels);
}

export const observe = {
  incrementCounter: (name, labels) => 
    globalThis.EdgeCloud.observe.incrementCounter(name, normalizeLabels(labels)),
  recordGauge: (name, value, labels) => 
    globalThis.EdgeCloud.observe.recordGauge(name, value, normalizeLabels(labels)),
  recordHistogram: (name, value, labels) => 
    globalThis.EdgeCloud.observe.recordHistogram(name, value, normalizeLabels(labels)),
  emitLog: (level, message, labels) => 
    globalThis.EdgeCloud.observe.emitLog(level, message, normalizeLabels(labels)),
  emitLogRecord: (record) => {
    const timestampMs = record.timestampMs ?? Date.now();
    const level = record.level ?? "info";
    const message = record.message ?? "";
    const labels = normalizeLabels(record.labels);
    globalThis.EdgeCloud.observe.emitLogRecord(timestampMs, level, message, labels);
  },
};
