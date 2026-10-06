// Demo: allkeys-lru evict các key lạnh và giữ key nóng, noeviction từ chối ghi với lỗi OOM.
// Demo THAY ĐỔI setting maxmemory của server và khôi phục lại trong `finally`.
// Chỉ chạy nó với Redis của compose stack trong handbook này (make up).
import { Redis } from "ioredis";
import { eventually, uniqueName } from "@handbook/testkit";
import {
  assertOwnRedis,
  deleteByPrefix,
  evictedKeys,
  fillUntilEviction,
  fillUntilRejected,
  limitMemory,
  readConfig,
  seedKeys,
  writeConfig,
} from "./lab.js";

const redis = new Redis(process.env.REDIS_URL ?? "redis://127.0.0.1:6379");
const prefix = uniqueName("demo:eviction");
await assertOwnRedis(redis); // từ chối khi gặp Redis lạ hoặc còn dữ liệu cũ, trước mọi lệnh CONFIG SET
const original = await readConfig(redis);
const HEADROOM = 256 * 1024;
const COLD_KEYS = 1500;

try {
  console.log("config đã lưu:", original);

  console.log("== allkeys-lru: ghi vượt giới hạn trong khi liên tục chạm vào một key nóng ==");
  await seedKeys(redis, `${prefix}:cold`, COLD_KEYS, 1024);
  await redis.set(`${prefix}:hot`, "hot");
  await eventually(async () => Number(await redis.object("IDLETIME", `${prefix}:cold:0`)) >= 2, {
    timeoutMs: 10_000,
  });
  await redis.get(`${prefix}:hot`);
  const maxmemory = await limitMemory(redis, "allkeys-lru", HEADROOM);
  console.log(`đã đặt maxmemory = ${maxmemory} byte (used_memory + ${HEADROOM})`);
  const evicted = await fillUntilEviction(redis, `${prefix}:fill`, 20_000, {
    touchKey: `${prefix}:hot`,
    minEvicted: 900,
  });
  const surviving = await redis.exists(
    ...Array.from({ length: COLD_KEYS }, (_, i) => `${prefix}:cold:${i}`),
  );
  console.log(`evicted_keys tăng thêm ${evicted}`);
  console.log(`key nóng còn sống: ${(await redis.exists(`${prefix}:hot`)) === 1}`);
  console.log(`key lạnh còn lại: ${surviving} trên ${COLD_KEYS}`);

  await writeConfig(redis, original);
  await deleteByPrefix(redis, prefix);

  console.log("== noeviction: cùng áp lực đó thì lệnh ghi bị từ chối ==");
  const before = await evictedKeys(redis);
  await limitMemory(redis, "noeviction", HEADROOM);
  console.log("lỗi khi ghi:", await fillUntilRejected(redis, `${prefix}:fill`, 20_000));
  console.log(`evicted_keys tăng thêm ${(await evictedKeys(redis)) - before}`);
} finally {
  await writeConfig(redis, original);
  await deleteByPrefix(redis, prefix);
  console.log("config đã khôi phục:", await readConfig(redis));
  redis.disconnect();
}
