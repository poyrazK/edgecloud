export const process = {
  getEnv: (key) => globalThis.EdgeCloud.process.getEnv(key),
  getAllEnv: () => Object.fromEntries(globalThis.EdgeCloud.process.getAllEnv()),
  getArgs: () => globalThis.EdgeCloud.process.getArgs(),
  cwd: () => {
    const res = globalThis.EdgeCloud.process.getCwd();
    if (res.err) throw new Error(res.err);
    return res.ok;
  },
  exit: (code) => globalThis.EdgeCloud.process.exit(code),
};
