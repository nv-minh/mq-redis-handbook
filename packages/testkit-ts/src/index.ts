import net from "node:net";
import { randomBytes } from "node:crypto";

const DEFAULT_INTERVAL_MS = 100;

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function tryConnect(host: string, port: number, timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const socket = net.connect({ host, port });
    const fail = (error: Error) => {
      socket.destroy();
      reject(error);
    };
    socket.setTimeout(timeoutMs, () => fail(new Error("connect attempt timed out")));
    socket.once("error", fail);
    socket.once("connect", () => {
      socket.destroy();
      resolve();
    });
  });
}

/**
 * Wait until a TCP listener accepts connections on host:port.
 * Rejects (never hangs) after timeoutMs, with host:port and the last error in the message.
 */
export async function waitForPort(host: string, port: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError = "no connection attempt made";
  for (;;) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) break;
    try {
      await tryConnect(host, port, Math.min(remaining, 1000));
      return;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }
    await sleep(Math.min(DEFAULT_INTERVAL_MS, Math.max(0, deadline - Date.now())));
  }
  throw new Error(
    `timed out after ${timeoutMs}ms waiting for ${host}:${port} (last error: ${lastError}). ` +
      "Is Docker running and the service up? Try: make up",
  );
}

/**
 * Unique resource name per call, so re-running a lab on a dirty broker never collides
 * with keys, queues or topics left by an earlier run.
 */
export function uniqueName(prefix: string): string {
  const time = Date.now().toString(36);
  const random = randomBytes(6)
    .toString("base64url")
    .replace(/[-_]/g, "x")
    .slice(0, 6)
    .toLowerCase();
  return `${prefix}-${time}-${random}`;
}

/**
 * Poll fn until it returns something other than undefined or false (a thrown error counts as
 * "not yet"). Rejects after timeoutMs with the last error. Use this instead of fixed sleeps.
 */
export async function eventually<T>(
  fn: () => Promise<T | undefined | false>,
  opts: { timeoutMs: number; intervalMs?: number },
): Promise<T> {
  const intervalMs = opts.intervalMs ?? DEFAULT_INTERVAL_MS;
  const deadline = Date.now() + opts.timeoutMs;
  let lastError: unknown;
  for (;;) {
    try {
      const value = await fn();
      if (value !== undefined && value !== false) return value;
      lastError = undefined;
    } catch (error) {
      lastError = error;
    }
    if (Date.now() >= deadline) break;
    await sleep(Math.min(intervalMs, Math.max(0, deadline - Date.now())));
  }
  const detail =
    lastError === undefined
      ? "condition never became true"
      : `last error: ${lastError instanceof Error ? lastError.message : String(lastError)}`;
  throw new Error(`eventually timed out after ${opts.timeoutMs}ms (${detail})`, {
    cause: lastError,
  });
}
