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
    socket.setTimeout(timeoutMs, () => fail(new Error("lần kết nối bị timeout")));
    socket.once("error", fail);
    socket.once("connect", () => {
      socket.destroy();
      resolve();
    });
  });
}

/**
 * Chờ tới khi một TCP listener nhận kết nối trên host:port.
 * Reject (không bao giờ treo) sau timeoutMs, message có host:port và lỗi gần nhất.
 */
export async function waitForPort(host: string, port: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError = "chưa thử kết nối lần nào";
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
    `hết ${timeoutMs}ms vẫn chưa kết nối được tới ${host}:${port} (lỗi gần nhất: ${lastError}). ` +
      "Docker đã chạy và service đã lên chưa? Thử: make up",
  );
}

/**
 * Tên resource duy nhất cho mỗi lần gọi, để chạy lại lab trên broker còn dữ liệu cũ
 * không bao giờ đụng key, queue hay topic mà lần chạy trước để lại.
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
 * Poll fn tới khi nó trả về giá trị khác undefined và false (ném lỗi được tính là
 * "chưa xong"). Reject sau timeoutMs kèm lỗi gần nhất. Dùng hàm này thay cho sleep cố định.
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
      ? "điều kiện chưa bao giờ đúng"
      : `lỗi gần nhất: ${lastError instanceof Error ? lastError.message : String(lastError)}`;
  throw new Error(`eventually hết ${opts.timeoutMs}ms mà điều kiện chưa đạt (${detail})`, {
    cause: lastError,
  });
}
