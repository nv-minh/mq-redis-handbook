import { randomBytes, randomUUID } from "node:crypto";
import type { Redis } from "ioredis";

// Ba mảnh độc lập dùng Redis làm hạ tầng: rate limiter, distributed lock, cache-aside.
// Không có framework và không có HTTP: mỗi mảnh là vài hàm nhỏ nhận một client ioredis.

/** Tham số sai (limit, window, ttl): ném RangeError với message nói rõ tham số nào và vì sao. */
function requireInteger(name: string, value: number, min: number): void {
  if (!Number.isInteger(value) || value < min) {
    throw new RangeError(`${name} phải là số nguyên >= ${min}, nhận được ${String(value)}`);
  }
}

// ---------------------------------------------------------------------------------------------
// 1. Rate limiter: sliding window log trên sorted set, trong MỘT Lua script
// ---------------------------------------------------------------------------------------------

/**
 * Read - decide - write phải chạy nguyên tử, nên cả bốn bước nằm trong một script:
 * bỏ entry cũ, đếm, nếu còn chỗ thì ghi entry mới và đặt TTL.
 *
 * - Thời gian lấy từ `TIME` của SERVER, nên client lệch đồng hồ vẫn đồng ý với nhau.
 *   Score là micro giây (khoảng 1.8e15, nhỏ hơn 2^53 nên double biểu diễn chính xác).
 * - Entry nằm trong cửa sổ khi tuổi của nó nhỏ hơn window, nên entry đúng bằng `now - window` bị bỏ.
 * - Member là uuid do client sinh (ARGV[3]), nên hai request trong cùng một micro giây vẫn là hai entry.
 * - Request bị từ chối KHÔNG ghi gì, nên cửa sổ không bị đẩy lùi vì những lần thử bị từ chối.
 * - PEXPIRE theo window: mọi entry trong key đều già hơn window sau thời gian đó, key idle tự biến mất.
 */
const SLIDING_WINDOW_LUA = `
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000000 + tonumber(time[2])
local limit = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - windowMs * 1000)
if redis.call('ZCARD', KEYS[1]) < limit then
  redis.call('ZADD', KEYS[1], now, ARGV[3])
  redis.call('PEXPIRE', KEYS[1], windowMs)
  return 1
end
return 0
`;

/**
 * Compare-and-delete: chỉ xóa lock khi value đúng bằng token của caller.
 * Từ Redis 8.4 có thể thay bằng một lệnh `DELEX key IFEQ token`, không cần Lua.
 */
const RELEASE_LOCK_LUA = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;

type WithScripts = Redis & {
  slidingWindowAllowLua(
    key: string,
    limit: number,
    windowMs: number,
    member: string,
  ): Promise<number>;
  releaseLockLua(key: string, token: string): Promise<number>;
};
const defined = new WeakSet<Redis>();

/**
 * `defineCommand` của ioredis gửi EVALSHA và tự nạp lại script khi server báo NOSCRIPT.
 * Lệnh được định nghĩa trên từng client, nên mỗi client chỉ định nghĩa một lần.
 */
function withScripts(rdb: Redis): WithScripts {
  if (!defined.has(rdb)) {
    rdb.defineCommand("slidingWindowAllowLua", { numberOfKeys: 1, lua: SLIDING_WINDOW_LUA });
    rdb.defineCommand("releaseLockLua", { numberOfKeys: 1, lua: RELEASE_LOCK_LUA });
    defined.add(rdb);
  }
  return rdb as WithScripts;
}

/**
 * Cho phép tối đa `limit` request trong BẤT KỲ khoảng `windowMs` nào của `key`.
 * Resolve true khi request được chấp nhận (và được ghi lại), false khi bị từ chối (không ghi gì).
 * `limit = 0` luôn từ chối. Ném RangeError nếu limit không phải số nguyên >= 0
 * hoặc windowMs không phải số nguyên > 0.
 *
 * Bộ nhớ là O(limit) cho mỗi key, vì sorted set giữ từng request trong cửa sổ.
 */
export async function slidingWindowAllow(
  rdb: Redis,
  key: string,
  limit: number,
  windowMs: number,
): Promise<boolean> {
  requireInteger("limit", limit, 0);
  requireInteger("windowMs", windowMs, 1);
  const allowed = await withScripts(rdb).slidingWindowAllowLua(key, limit, windowMs, randomUUID());
  return allowed === 1;
}

/**
 * Phiên bản sai, chỉ dùng cho demo: đếm rồi mới ghi bằng các lệnh riêng lẻ và dùng đồng hồ của client.
 * Hai caller cùng đọc `count < limit` rồi cùng ghi, nên số request được chấp nhận vượt limit.
 */
export async function slidingWindowAllowNaive(
  rdb: Redis,
  key: string,
  limit: number,
  windowMs: number,
): Promise<boolean> {
  const now = Date.now();
  await rdb.zremrangebyscore(key, "-inf", now - windowMs);
  if ((await rdb.zcard(key)) >= limit) return false;
  await rdb.zadd(key, now, randomUUID());
  await rdb.pexpire(key, windowMs);
  return true;
}

// ---------------------------------------------------------------------------------------------
// 2. Distributed lock trên MỘT node Redis: SET NX PX với token, release bằng Lua
// ---------------------------------------------------------------------------------------------

/**
 * Lấy lock bằng `SET key token NX PX ttlMs`. Resolve token (128 bit crypto-random, hex) nếu lấy được,
 * `null` nếu lock đang do người khác giữ. Ném RangeError nếu ttlMs không phải số nguyên > 0.
 *
 * Lock chỉ đúng trong `ttlMs`: sau đó Redis tự xóa key và người khác có thể lấy lock, kể cả khi
 * holder cũ vẫn đang làm việc (GC pause, network delay). Đây là lock cho EFFICIENCY, không phải CORRECTNESS.
 */
export async function acquireLock(rdb: Redis, key: string, ttlMs: number): Promise<string | null> {
  requireInteger("ttlMs", ttlMs, 1);
  const token = randomBytes(16).toString("hex");
  const reply = await rdb.set(key, token, "PX", ttlMs, "NX");
  return reply === "OK" ? token : null;
}

/**
 * Nhả lock chỉ khi `token` còn là value của key. Resolve true nếu đã xóa, false nếu lock đã hết hạn
 * hoặc đã thuộc người khác (khi đó không xóa gì). Chỉ dùng `DEL` sẽ xóa nhầm lock của người khác.
 */
export async function releaseLock(rdb: Redis, key: string, token: string): Promise<boolean> {
  return (await withScripts(rdb).releaseLockLua(key, token)) === 1;
}

// ---------------------------------------------------------------------------------------------
// 3. Cache-aside với chống stampede trong một process
// ---------------------------------------------------------------------------------------------

/**
 * Đọc cache, miss thì gọi loader rồi ghi lại dạng JSON với EX `ttlSec`.
 *
 * - Redis lỗi thì lỗi được ném ra (đơn giản và dễ kiểm chứng). Production có thể fall back sang loader.
 * - Giá trị `null` được cache (negative caching): value trong Redis là chuỗi JSON `"null"`,
 *   còn miss là key không tồn tại (GET trả nil), nên hai trường hợp không lẫn nhau.
 * - `undefined` (và mọi giá trị JSON.stringify không biểu diễn được) bị từ chối bằng TypeError
 *   và không được cache: loader phải trả `null` cho "không tìm thấy".
 */
async function readThrough<T>(
  rdb: Redis,
  key: string,
  ttlSec: number,
  loader: () => Promise<T>,
): Promise<T> {
  requireInteger("ttlSec", ttlSec, 1);
  const cached = await rdb.get(key);
  if (cached !== null) return JSON.parse(cached) as T;
  const value = await loader();
  const json = JSON.stringify(value) as string | undefined;
  if (json === undefined) {
    throw new TypeError(
      "loader trả về giá trị không biểu diễn được bằng JSON (undefined); hãy trả null",
    );
  }
  await rdb.set(key, json, "EX", ttlSec);
  return value;
}

/**
 * Phiên bản không chống stampede, chỉ dùng cho demo và test đối chứng:
 * mỗi caller miss đều tự gọi loader.
 */
export function cacheAsideUnprotected<T>(
  rdb: Redis,
  key: string,
  ttlSec: number,
  loader: () => Promise<T>,
): Promise<T> {
  return readThrough(rdb, key, ttlSec, loader);
}

/**
 * Cache-aside với một Map các promise đang chạy theo key: caller đầu tiên của một key bắt đầu
 * đọc cache và load, mọi caller trùng key trong lúc đó `await` cùng promise ấy.
 *
 * Đây là cách cài TỰ THIẾT KẾ tương đương `singleflight` của Go, không có nguồn chính thức (README nói rõ).
 * Giới hạn: chỉ dedup trong MỘT process. N instance vẫn là N lần load cho một key lạnh.
 */
export class RedisCache {
  private readonly rdb: Redis;
  private readonly inflight = new Map<string, Promise<unknown>>();

  constructor(rdb: Redis) {
    this.rdb = rdb;
  }

  /**
   * Cache-aside cho `key`. Cả đọc cache, load và ghi cache nằm trong promise dùng chung, và entry bị xóa
   * trong `finally` SAU khi đã ghi cache: caller đến muộn hoặc thấy promise đang chạy, hoặc thấy giá trị
   * trong Redis, nên không có khe hở nào cho lần load thứ hai. Loader lỗi thì mọi waiter nhận cùng lỗi,
   * không có gì được cache và lần gọi sau thử lại.
   * Hai lời gọi trùng key phải dùng cùng kiểu T (map không kiểm tra điều này).
   */
  cacheAside<T>(key: string, ttlSec: number, loader: () => Promise<T>): Promise<T> {
    const running = this.inflight.get(key);
    if (running) return running as Promise<T>;
    const promise = readThrough(this.rdb, key, ttlSec, loader).finally(() => {
      this.inflight.delete(key);
    });
    this.inflight.set(key, promise);
    return promise;
  }
}
