import type { Redis } from "ioredis";

/**
 * Check-and-decrement trong MỘT script, nên không client nào chen vào được giữa lúc kiểm tra và DECR.
 * Script trả về 1 khi đã giảm và 0 khi key không tồn tại hoặc không lớn hơn 0.
 */
const DECR_IF_POSITIVE_LUA = `
local value = tonumber(redis.call('GET', KEYS[1]))
if value and value > 0 then
  redis.call('DECR', KEYS[1])
  return 1
end
return 0
`;

type WithLua = Redis & { decrIfPositiveLua(key: string): Promise<number> };
const defined = new WeakSet<Redis>();

/**
 * Giảm `key` một cách atomic, chỉ khi nó đang chứa một số lớn hơn 0.
 * Resolve true khi lần gọi này lấy được một đơn vị, ngược lại false. Không bao giờ tạo key và
 * không bao giờ để nó xuống dưới 0, dù có bao nhiêu caller tranh nhau.
 *
 * `defineCommand` của ioredis gửi EVALSHA và nạp lại script khi gặp NOSCRIPT.
 */
export async function decrIfPositive(redis: Redis, key: string): Promise<boolean> {
  if (!defined.has(redis)) {
    redis.defineCommand("decrIfPositiveLua", { numberOfKeys: 1, lua: DECR_IF_POSITIVE_LUA });
    defined.add(redis);
  }
  return (await (redis as WithLua).decrIfPositiveLua(key)) === 1;
}

/**
 * Phiên bản sai, chỉ dùng cho demo: GET, quyết định ở client, rồi DECR.
 * Client khác có thể DECR giữa lúc GET và DECR, nên counter có thể xuống dưới 0.
 */
export async function decrIfPositiveNaive(redis: Redis, key: string): Promise<boolean> {
  const value = Number(await redis.get(key));
  if (value > 0) {
    await redis.decr(key);
    return true;
  }
  return false;
}

/** `n` lệnh SET, mỗi lệnh được await xong mới gửi lệnh tiếp theo: n round trip. */
export async function setSequential(redis: Redis, prefix: string, n: number): Promise<void> {
  for (let i = 0; i < n; i++) await redis.set(`${prefix}:${i}`, "1");
}

/** Cũng `n` lệnh SET đó nhưng xếp vào một pipeline và gửi cùng lúc: 1 round trip. */
export async function setPipelined(redis: Redis, prefix: string, n: number): Promise<void> {
  const pipeline = redis.pipeline();
  for (let i = 0; i < n; i++) pipeline.set(`${prefix}:${i}`, "1");
  const results = await pipeline.exec();
  const failed = results?.find(([error]) => error !== null);
  if (failed) throw failed[0];
}

export interface WriteCounter {
  writes(): number;
  reset(): void;
}

/**
 * Đếm số lần ioredis write trên socket. Đây là cách lab đo round trip mà không cần đồng hồ bấm giờ:
 * một lệnh gửi riêng là một write, còn một pipeline được flush bằng một write duy nhất, nên
 * "số lần write trên socket" bằng "số round trip request/response" ở đây.
 * Resolve sau khi đã kết nối, vì socket chỉ tồn tại khi client đã kết nối.
 */
export async function countSocketWrites(redis: Redis): Promise<WriteCounter> {
  await redis.ping();
  let count = 0;
  const socket = redis.stream;
  const write = socket.write.bind(socket) as (...args: unknown[]) => boolean;
  (socket as { write: unknown }).write = (...args: unknown[]) => {
    count++;
    return write(...args);
  };
  return {
    writes: () => count,
    reset: () => {
      count = 0;
    },
  };
}
