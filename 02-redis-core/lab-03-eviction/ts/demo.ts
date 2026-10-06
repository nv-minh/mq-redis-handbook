// Demo: allkeys-lru evicts cold keys and keeps the hot one, noeviction rejects writes with OOM.
// It CHANGES maxmemory settings of the server and restores them in `finally`.
// Only run it against the Redis of this handbook's compose stack (make up).
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
await assertOwnRedis(redis); // refuse on a foreign or dirty Redis before any CONFIG SET
const original = await readConfig(redis);
const HEADROOM = 256 * 1024;
const COLD_KEYS = 1500;

try {
  console.log("saved config:", original);

  console.log("== allkeys-lru: write past the limit while touching one hot key ==");
  await seedKeys(redis, `${prefix}:cold`, COLD_KEYS, 1024);
  await redis.set(`${prefix}:hot`, "hot");
  await eventually(async () => Number(await redis.object("IDLETIME", `${prefix}:cold:0`)) >= 2, {
    timeoutMs: 10_000,
  });
  await redis.get(`${prefix}:hot`);
  const maxmemory = await limitMemory(redis, "allkeys-lru", HEADROOM);
  console.log(`maxmemory set to ${maxmemory} bytes (used_memory + ${HEADROOM})`);
  const evicted = await fillUntilEviction(redis, `${prefix}:fill`, 20_000, {
    touchKey: `${prefix}:hot`,
    minEvicted: 900,
  });
  const surviving = await redis.exists(
    ...Array.from({ length: COLD_KEYS }, (_, i) => `${prefix}:cold:${i}`),
  );
  console.log(`evicted_keys grew by ${evicted}`);
  console.log(`hot key survived: ${(await redis.exists(`${prefix}:hot`)) === 1}`);
  console.log(`cold keys left: ${surviving} of ${COLD_KEYS}`);

  await writeConfig(redis, original);
  await deleteByPrefix(redis, prefix);

  console.log("== noeviction: the same pressure rejects writes ==");
  const before = await evictedKeys(redis);
  await limitMemory(redis, "noeviction", HEADROOM);
  console.log("write error:", await fillUntilRejected(redis, `${prefix}:fill`, 20_000));
  console.log(`evicted_keys grew by ${(await evictedKeys(redis)) - before}`);
} finally {
  await writeConfig(redis, original);
  await deleteByPrefix(redis, prefix);
  console.log("restored config:", await readConfig(redis));
  redis.disconnect();
}
