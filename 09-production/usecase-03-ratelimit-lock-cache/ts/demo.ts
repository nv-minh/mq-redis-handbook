// Demo ba mảnh hạ tầng trên Redis: sliding window rate limiter, distributed lock (kể cả lock hết hạn khi
// đang giữ và fencing token) và cache-aside có chống stampede. Mọi key nằm dưới một prefix riêng và bị xóa ở cuối.
import { setTimeout as sleep } from "node:timers/promises";
import { Redis } from "ioredis";
import { uniqueName } from "@handbook/testkit";
import {
  acquireLock,
  cacheAsideUnprotected,
  RedisCache,
  releaseLock,
  slidingWindowAllow,
  slidingWindowAllowNaive,
} from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const prefix = uniqueName("demo-uc03");
const POOL = 10;
const connections = Array.from({ length: POOL }, () => new Redis(url));
const rdb = connections[0]!;
const conn = (i: number): Redis => connections[i % POOL]!;
const count = (flags: boolean[]) => flags.filter(Boolean).length;

/** Escape ký tự glob của MATCH: \ * ? [ ]. */
function escapeGlob(text: string): string {
  return text.replace(/[\\*?[\]]/g, "\\$&");
}

async function deleteByPrefix(): Promise<void> {
  let cursor = "0";
  do {
    const [next, keys] = await rdb.scan(cursor, "MATCH", `${escapeGlob(prefix)}*`, "COUNT", 200);
    if (keys.length > 0) await rdb.del(...keys);
    cursor = next;
  } while (cursor !== "0");
}

async function rateLimiterDemo(): Promise<void> {
  console.log(
    "== 1. Rate limiter: tối đa 5 request trong cửa sổ 1000 ms, 20 request đến cùng lúc ==",
  );
  const burst = 20;
  const limit = 5;
  const windowMs = 1000;

  const lua = await Promise.all(
    Array.from({ length: burst }, (_, i) =>
      slidingWindowAllow(conn(i), `${prefix}:rl-lua`, limit, windowMs),
    ),
  );
  console.log(`Lua script (atomic): allowed ${count(lua)}, rejected ${burst - count(lua)}`);

  const naive = await Promise.all(
    Array.from({ length: burst }, (_, i) =>
      slidingWindowAllowNaive(conn(i), `${prefix}:rl-naive`, limit, windowMs),
    ),
  );
  console.log(
    `Đếm rồi ghi bằng lệnh riêng lẻ: allowed ${count(naive)}, rejected ${burst - count(naive)}`,
  );
  console.log(
    "Bản naive cho vượt limit vì nhiều caller cùng đọc 'còn chỗ' rồi cùng ghi (kết quả phụ thuộc may rủi của race).",
  );

  const start = performance.now();
  while (!(await slidingWindowAllow(rdb, `${prefix}:rl-lua`, limit, windowMs))) await sleep(50);
  console.log(
    `Cửa sổ trượt: request được allow lại sau ${Math.round(performance.now() - start)} ms, khi các request cũ nhất rời cửa sổ.`,
  );
  const ttl = await rdb.pttl(`${prefix}:rl-lua`);
  console.log(`Key có TTL ${ttl} ms nên key idle tự biến mất.\n`);
}

/** Resource mô phỏng: một giá trị cùng fencing token lớn nhất từng được chấp nhận. */
class Storage {
  value = "(trống)";
  lastFence = 0;

  /** Không kiểm tra gì: ai tới sau thắng. */
  writeUnsafe(writer: string): void {
    this.value = writer;
  }

  /** Từ chối token cũ hơn token đã thấy: đây là phần Redis lock không tự làm được. */
  writeFenced(fence: number, writer: string): boolean {
    if (fence < this.lastFence) return false;
    this.lastFence = fence;
    this.value = writer;
    return true;
  }
}

/** Chỉ cho demo: lấy lock và INCR bộ đếm fencing trong cùng MỘT script, nên token tăng đúng thứ tự lấy lock. */
const FENCED_ACQUIRE_LUA = `
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
  return redis.call('INCR', KEYS[2])
end
return 0
`;

async function lockDemo(): Promise<void> {
  console.log("== 2. Distributed lock: 5 worker tranh nhau một lock (SET NX PX + token) ==");
  const lockKey = `${prefix}:lock`;
  const workers = ["w1", "w2", "w3", "w4", "w5"];
  const tokens = await Promise.all(workers.map((_, i) => acquireLock(conn(i), lockKey, 5000)));
  workers.forEach((name, i) => {
    console.log(
      `  ${name}: ${tokens[i] ? `LẤY ĐƯỢC lock, token ${tokens[i]}` : "không lấy được (lock đang bị giữ)"}`,
    );
  });
  const winner = tokens.findIndex((token) => token !== null);
  console.log(`Đúng một worker thắng: ${count(tokens.map((t) => t !== null))}.`);
  console.log(
    `Release với token sai: ${await releaseLock(rdb, lockKey, "token-cua-nguoi-khac")} (lock vẫn còn, ${await rdb.exists(lockKey)} key).`,
  );
  console.log(
    `Release với token đúng của ${workers[winner]}: ${await releaseLock(rdb, lockKey, tokens[winner]!)}.\n`,
  );

  console.log("== 2b. Lock hết hạn khi holder còn đang làm việc (GC pause, network delay) ==");
  const fenceKey = `${prefix}:lock:fence`;
  const fencedAcquire = async (client: Redis, ttlMs: number) => {
    const token = `token-${Math.random().toString(36).slice(2, 10)}`;
    const fence = Number(await client.eval(FENCED_ACQUIRE_LUA, 2, lockKey, fenceKey, token, ttlMs));
    return fence > 0 ? { token, fence } : null;
  };
  const a = await fencedAcquire(rdb, 300);
  console.log(`A lấy lock (TTL 300 ms), fencing token ${a?.fence}. Rồi A bị pause quá TTL...`);
  let b: Awaited<ReturnType<typeof fencedAcquire>> = null;
  while (!b) {
    b = await fencedAcquire(conn(1), 5000);
    if (!b) await sleep(50);
  }
  console.log(
    `Lock hết hạn, B lấy được lock, fencing token ${b.fence}. Giờ A và B đều tin mình đang giữ lock.`,
  );

  const unsafe = new Storage();
  unsafe.writeUnsafe("B");
  unsafe.writeUnsafe("A (muộn)");
  console.log(
    `Resource KHÔNG kiểm tra token: B ghi rồi A tỉnh dậy ghi đè, giá trị cuối = "${unsafe.value}". Mutual exclusion đã vỡ.`,
  );

  const fenced = new Storage();
  const bOk = fenced.writeFenced(b.fence, "B");
  const aOk = fenced.writeFenced(a!.fence, "A (muộn)");
  console.log(
    `Resource kiểm tra fencing token: B (token ${b.fence}) ${bOk ? "được ghi" : "bị từ chối"}, A (token ${a!.fence}) ${aOk ? "được ghi" : "bị từ chối"}, giá trị cuối = "${fenced.value}".`,
  );

  console.log(
    `A release muộn: ${await releaseLock(rdb, lockKey, a!.token)} (token ngăn A xóa nhầm lock của B, còn ${await rdb.exists(lockKey)} key).`,
  );
  await releaseLock(conn(1), lockKey, b.token);
  console.log(
    "Kết luận: token chỉ chặn xóa nhầm. Muốn chặn ghi muộn thì resource phải kiểm tra fencing token. Lock này chỉ dùng cho efficiency.\n",
  );
}

async function cacheDemo(): Promise<void> {
  console.log("== 3. Cache-aside: 100 request đồng thời cùng miss một key lạnh ==");
  const callers = 100;
  const makeLoader = () => {
    const state = { calls: 0 };
    const loader = async () => {
      state.calls += 1;
      await sleep(100); // giả lập truy vấn DB chậm 100 ms
      return { id: 7, name: "Linh" };
    };
    return { state, loader };
  };

  const unprotected = makeLoader();
  await Promise.all(
    Array.from({ length: callers }, (_, i) =>
      cacheAsideUnprotected(conn(i), `${prefix}:cache-unprotected`, 60, unprotected.loader),
    ),
  );
  console.log(`Không chống stampede: loader (DB) bị gọi ${unprotected.state.calls} lần.`);

  const protectedLoader = makeLoader();
  const cache = new RedisCache(rdb);
  await Promise.all(
    Array.from({ length: callers }, () =>
      cache.cacheAside(`${prefix}:cache-protected`, 60, protectedLoader.loader),
    ),
  );
  console.log(`Có map in-flight: loader (DB) bị gọi ${protectedLoader.state.calls} lần.`);
  console.log(
    `Giá trị trong Redis: ${await rdb.get(`${prefix}:cache-protected`)}, TTL ${await rdb.pttl(`${prefix}:cache-protected`)} ms.`,
  );

  // Hai instance, mỗi instance một map in-flight riêng, cùng miss một key lạnh.
  const twoInstances = makeLoader();
  await Promise.all(
    [new RedisCache(conn(1)), new RedisCache(conn(2))].map((instance) =>
      instance.cacheAside(`${prefix}:cache-two-instances`, 60, twoInstances.loader),
    ),
  );
  console.log(
    `Hai instance cùng miss một key lạnh: loader bị gọi ${twoInstances.state.calls} lần (mỗi instance một lần).`,
  );
  console.log(
    "Chống stampede này chỉ có tác dụng trong MỘT process: N instance vẫn là N lần load (muốn hơn thì kết hợp với lock).\n",
  );
}

const errors: unknown[] = [];
try {
  await rateLimiterDemo();
  await lockDemo();
  await cacheDemo();
} catch (error) {
  errors.push(error);
}
// Dọn theo pha tuần tự: xóa key trước, đóng connection sau.
try {
  await deleteByPrefix();
} catch (error) {
  errors.push(error);
}
await Promise.allSettled(
  connections.map((connection) => connection.quit().catch(() => connection.disconnect())),
);
if (errors.length > 0) {
  console.error("Demo lỗi:", errors[0]);
  process.exitCode = 1;
} else {
  console.log("Đã dọn mọi key của demo.");
}
