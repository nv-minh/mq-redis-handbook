import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { eventually, uniqueName } from "@handbook/testkit";
import {
  deleteByPrefix,
  evictedKeys,
  fillUntilEviction,
  fillUntilRejected,
  limitMemory,
  readConfig,
  seedKeys,
  writeConfig,
  type EvictionConfig,
} from "./lab.js";

// This lab CHANGES the maxmemory settings of the server it connects to (and restores them).
// Only run it against the Redis of this handbook's compose stack (`make up`).
const redis = new Redis(process.env.REDIS_URL ?? "redis://127.0.0.1:6379");
const HEADROOM = 256 * 1024;
const COLD_KEYS = 1500;
const VALUE_SIZE = 1024;

let original: EvictionConfig;
const prefixes: string[] = [];

function newPrefix(label: string): string {
  const prefix = uniqueName(`lab02-${label}`);
  prefixes.push(prefix);
  return prefix;
}

/** Put the server back first (so deletes are never blocked by a full memory), then drop test keys. */
async function restore(): Promise<void> {
  await writeConfig(redis, original);
  for (const prefix of prefixes.splice(0)) await deleteByPrefix(redis, prefix);
}

beforeAll(async () => {
  // Save BEFORE the first CONFIG SET, so there is always something to restore.
  original = await readConfig(redis);
});

afterEach(restore);

afterAll(async () => {
  try {
    if (original) await restore();
  } finally {
    redis.disconnect();
  }
});

describe("lab-03 eviction", () => {
  it("allkeys_lru_evicts_cold_keys_and_keeps_hot_key", async () => {
    const prefix = newPrefix("lru");
    const hot = `${prefix}:hot`;
    const coldPrefix = `${prefix}:cold`;

    // 1. Load cold keys and the hot key while there is no limit.
    await seedKeys(redis, coldPrefix, COLD_KEYS, VALUE_SIZE);
    await redis.set(hot, "hot");

    // 2. LRU idle time has a resolution of one second: wait until the cold keys look old.
    await eventually(async () => Number(await redis.object("IDLETIME", `${coldPrefix}:0`)) >= 2, {
      timeoutMs: 10_000,
    });
    await redis.get(hot); // reading the hot key resets its idle time to 0

    // 3. Cap memory just above what is used now, then keep writing while touching the hot key.
    await limitMemory(redis, "allkeys-lru", HEADROOM);
    const evicted = await fillUntilEviction(redis, `${prefix}:fill`, 20_000, {
      touchKey: hot,
      minEvicted: 900,
    });

    // Never assert an exact number: eviction is approximate (sampling).
    expect(evicted).toBeGreaterThan(0);
    expect(await redis.exists(hot)).toBe(1);
    const survivingCold = await redis.exists(
      ...Array.from({ length: COLD_KEYS }, (_, i) => `${coldPrefix}:${i}`),
    );
    expect(survivingCold).toBeLessThan(COLD_KEYS);
  });

  it("noeviction_policy_rejects_writes_when_full", async () => {
    const prefix = newPrefix("noevict");
    await redis.set(`${prefix}:existing`, "still readable");
    const evictedBefore = await evictedKeys(redis);

    await limitMemory(redis, "noeviction", HEADROOM);
    const error = await fillUntilRejected(redis, `${prefix}:fill`, 20_000);

    expect(error).not.toBeNull();
    expect(error).toMatch(/^OOM/);
    // Reads keep working, nothing was evicted, and the data written before the limit survives.
    expect(await redis.get(`${prefix}:existing`)).toBe("still readable");
    expect(await evictedKeys(redis)).toBe(evictedBefore);
    expect(await redis.exists(`${prefix}:fill:0`)).toBe(1);
  });

  it("volatile_policy_rejects_writes_when_no_key_has_a_ttl", async () => {
    const prefix = newPrefix("volatile");
    const evictedBefore = await evictedKeys(redis);

    // volatile-lru may only evict keys that have a TTL. None of ours does, so it behaves like noeviction.
    await limitMemory(redis, "volatile-lru", HEADROOM);
    const error = await fillUntilRejected(redis, `${prefix}:fill`, 20_000);

    expect(error).toMatch(/^OOM/);
    expect(await evictedKeys(redis)).toBe(evictedBefore);
  });

  it("config_is_restored_to_the_saved_values", async () => {
    await limitMemory(redis, "allkeys-lru", HEADROOM);
    expect((await readConfig(redis)).policy).toBe("allkeys-lru");

    await writeConfig(redis, original);

    expect(await readConfig(redis)).toEqual(original);
  });
});
