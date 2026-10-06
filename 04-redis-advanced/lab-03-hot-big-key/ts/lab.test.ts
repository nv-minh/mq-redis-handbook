import { afterAll, afterEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { uniqueName } from "@handbook/testkit";
import { findBigKeys } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const redis = new Redis(url);
const createdKeys: string[] = [];

// UNLINK frees big values in a background thread, so the cleanup itself never blocks Redis.
afterEach(async () => {
  if (createdKeys.length > 0) await redis.unlink(...createdKeys.splice(0));
});
afterAll(() => {
  redis.disconnect();
});

const KIB = 1024;
const MIB = 1024 * KIB;

/** Create `count` tiny string keys under `prefix`. */
async function createSmallKeys(prefix: string, count: number): Promise<void> {
  const pipeline = redis.pipeline();
  for (let i = 0; i < count; i++) {
    const key = `${prefix}:small:${i}`;
    createdKeys.push(key);
    pipeline.set(key, "x");
  }
  await pipeline.exec();
}

describe("lab-03 big keys: SCAN + MEMORY USAGE", () => {
  it("find_big_keys_returns_key_over_threshold", async () => {
    const prefix = uniqueName("lab04-bigkey");
    await createSmallKeys(prefix, 300);

    // One 1 MiB string and one list of about 1 MiB made of many small elements.
    const bigString = `${prefix}:big-string`;
    const bigList = `${prefix}:big-list`;
    createdKeys.push(bigString, bigList);
    await redis.set(bigString, "x".repeat(MIB));
    const list = redis.pipeline();
    for (let i = 0; i < 100; i++)
      list.rpush(bigList, ...Array.from({ length: 100 }, () => "y".repeat(100)));
    await list.exec();

    // 512 KiB sits between the tiny keys and the two big ones.
    const found = await findBigKeys(redis, 512 * KIB, `${prefix}:*`);

    // Only the big keys are returned, the biggest first, and no small key.
    expect(found).toEqual([bigString, bigList]);
    expect(found.some((k) => k.includes(":small:"))).toBe(false);
  });

  it("find_big_keys_ignores_small_keys", async () => {
    const prefix = uniqueName("lab04-smallkeys");
    await createSmallKeys(prefix, 500);
    // A 100 KiB value is large for a cache entry but still under the 512 KiB threshold.
    const medium = `${prefix}:medium`;
    createdKeys.push(medium);
    await redis.set(medium, "m".repeat(100 * KIB));

    expect(await findBigKeys(redis, 512 * KIB, `${prefix}:*`)).toEqual([]);
    // The same data is found once the threshold drops below the medium key, which proves the
    // scan did visit it and only the threshold decided.
    expect(await findBigKeys(redis, 50 * KIB, `${prefix}:*`)).toEqual([medium]);
  });
});
