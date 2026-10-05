import type { Redis } from "ioredis";

/**
 * Check-and-decrement as ONE script, so no other client can run between the check and the DECR.
 * The script returns 1 when it decremented and 0 when the key is missing or not above zero.
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
 * Atomically decrement `key` only when it holds a number above zero.
 * Resolves true when this call took one unit, false otherwise. Never creates the key and never
 * lets it go below zero, however many callers race.
 *
 * ioredis `defineCommand` sends EVALSHA and falls back to loading the script on NOSCRIPT.
 */
export async function decrIfPositive(redis: Redis, key: string): Promise<boolean> {
  if (!defined.has(redis)) {
    redis.defineCommand("decrIfPositiveLua", { numberOfKeys: 1, lua: DECR_IF_POSITIVE_LUA });
    defined.add(redis);
  }
  return (await (redis as WithLua).decrIfPositiveLua(key)) === 1;
}

/**
 * The broken version, for the demo only: GET, decide in the client, then DECR.
 * Another client can DECR between the GET and the DECR, so the counter can go below zero.
 */
export async function decrIfPositiveNaive(redis: Redis, key: string): Promise<boolean> {
  const value = Number(await redis.get(key));
  if (value > 0) {
    await redis.decr(key);
    return true;
  }
  return false;
}

/** `n` SET commands, each awaited before the next one is sent: n round trips. */
export async function setSequential(redis: Redis, prefix: string, n: number): Promise<void> {
  for (let i = 0; i < n; i++) await redis.set(`${prefix}:${i}`, "1");
}

/** The same `n` SET commands queued in one pipeline and flushed together: 1 round trip. */
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
 * Count the writes ioredis makes on its socket. This is how the lab measures round trips without
 * a stopwatch: a command sent alone is one write, and a pipeline is flushed as a single write, so
 * "number of socket writes" equals "number of request/response round trips" here.
 * Resolves after connecting, because the socket only exists once the client is connected.
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
