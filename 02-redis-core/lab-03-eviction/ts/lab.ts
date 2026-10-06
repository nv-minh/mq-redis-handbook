import type { Redis } from "ioredis";

/** The three settings this lab changes, as the strings CONFIG GET returns. */
export interface EvictionConfig {
  maxmemory: string;
  policy: string;
  samples: string;
}

const BATCH = 50;

async function configValue(redis: Redis, name: string): Promise<string> {
  // Measured with ioredis 6.0.0 (RESP3): CONFIG GET replies with a FLAT array ["name", "value"].
  const reply = (await redis.config("GET", name)) as string[];
  const value = reply[1];
  if (reply[0] !== name || value === undefined) {
    throw new Error(`unexpected CONFIG GET ${name} reply: ${JSON.stringify(reply)}`);
  }
  return value;
}

/** Read the current settings. Call this BEFORE the first CONFIG SET so you can put them back. */
export async function readConfig(redis: Redis): Promise<EvictionConfig> {
  return {
    maxmemory: await configValue(redis, "maxmemory"),
    policy: await configValue(redis, "maxmemory-policy"),
    samples: await configValue(redis, "maxmemory-samples"),
  };
}

/** Apply settings. The policy goes first, so a lower maxmemory is never enforced with the old policy. */
export async function writeConfig(redis: Redis, config: EvictionConfig): Promise<void> {
  await redis.config("SET", "maxmemory-policy", config.policy);
  await redis.config("SET", "maxmemory-samples", config.samples);
  await redis.config("SET", "maxmemory", config.maxmemory);
}

/** Parse one numeric field of INFO. */
async function infoNumber(redis: Redis, section: string, field: string): Promise<number> {
  const info = await redis.info(section);
  const match = new RegExp(`^${field}:(\\d+)`, "m").exec(info);
  if (!match) throw new Error(`INFO ${section} has no ${field}`);
  return Number(match[1]);
}

export const usedMemory = (redis: Redis) => infoNumber(redis, "memory", "used_memory");

/** Total number of keys the server has evicted since it started (INFO stats evicted_keys). */
export const evictedKeys = (redis: Redis) => infoNumber(redis, "stats", "evicted_keys");

/**
 * Cap memory relative to what is used right now, so the lab works on any baseline:
 * maxmemory = used_memory + headroomBytes. samples=10 makes approximate LRU closer to real LRU.
 * Returns the maxmemory it set, in bytes.
 */
export async function limitMemory(
  redis: Redis,
  policy: string,
  headroomBytes: number,
): Promise<number> {
  const maxmemory = (await usedMemory(redis)) + headroomBytes;
  await writeConfig(redis, { policy, samples: "10", maxmemory: String(maxmemory) });
  return maxmemory;
}

/** Write `count` keys `${prefix}:0 .. ${prefix}:count-1`, each holding `valueSize` bytes. */
export async function seedKeys(
  redis: Redis,
  prefix: string,
  count: number,
  valueSize: number,
): Promise<void> {
  const value = "x".repeat(valueSize);
  for (let start = 0; start < count; start += BATCH) {
    const pipeline = redis.pipeline();
    for (let i = start; i < Math.min(start + BATCH, count); i++)
      pipeline.set(`${prefix}:${i}`, value);
    const results = await pipeline.exec();
    const failed = results?.find(([error]) => error !== null);
    if (failed) throw failed[0];
  }
}

export interface FillOptions {
  /** Bytes per value. Default 1024. */
  valueSize?: number;
  /** A key to GET after every batch, so it stays "recently used". */
  touchKey?: string;
  /** Stop once at least this many keys were evicted. Default 1. */
  minEvicted?: number;
}

/**
 * Keep writing `${prefix}:0 ..` (at most `maxKeys` keys, in batches of 50) until the server has
 * evicted at least `minEvicted` keys. Returns how many keys were evicted during the call
 * (the delta of evicted_keys), 0 if the limit was never reached.
 */
export async function fillUntilEviction(
  redis: Redis,
  prefix: string,
  maxKeys: number,
  opts: FillOptions = {},
): Promise<number> {
  const { valueSize = 1024, touchKey, minEvicted = 1 } = opts;
  const value = "x".repeat(valueSize);
  const before = await evictedKeys(redis);
  for (let start = 0; start < maxKeys; start += BATCH) {
    const pipeline = redis.pipeline();
    for (let i = start; i < Math.min(start + BATCH, maxKeys); i++)
      pipeline.set(`${prefix}:${i}`, value);
    const results = await pipeline.exec();
    const failed = results?.find(([error]) => error !== null);
    if (failed) throw failed[0];
    if (touchKey) await redis.get(touchKey);
    const evicted = (await evictedKeys(redis)) - before;
    if (evicted >= minEvicted) return evicted;
  }
  return (await evictedKeys(redis)) - before;
}

/**
 * Keep writing `${prefix}:0 ..` until the server rejects a write. Returns the error message of the
 * first rejected write, or null if all `maxKeys` writes succeeded. Keys before the rejection stay.
 */
export async function fillUntilRejected(
  redis: Redis,
  prefix: string,
  maxKeys: number,
  valueSize = 1024,
): Promise<string | null> {
  const value = "x".repeat(valueSize);
  for (let start = 0; start < maxKeys; start += BATCH) {
    const pipeline = redis.pipeline();
    for (let i = start; i < Math.min(start + BATCH, maxKeys); i++)
      pipeline.set(`${prefix}:${i}`, value);
    const results = await pipeline.exec();
    const failed = results?.find(([error]) => error !== null);
    if (failed) return (failed[0] as Error).message;
  }
  return null;
}

/** Delete every key that starts with `${prefix}:`, using SCAN (never KEYS) and UNLINK. */
export async function deleteByPrefix(redis: Redis, prefix: string): Promise<void> {
  let cursor = "0";
  do {
    const [next, keys] = await redis.scan(cursor, "MATCH", `${prefix}:*`, "COUNT", 1000);
    cursor = next;
    if (keys.length > 0) await redis.unlink(...keys);
  } while (cursor !== "0");
}
