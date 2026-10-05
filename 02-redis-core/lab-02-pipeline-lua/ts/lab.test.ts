import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { uniqueName } from "@handbook/testkit";
import { countSocketWrites, decrIfPositive, setPipelined, setSequential } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const CALLERS = 50;
// Several connections, so the callers really hit the server in parallel (one socket would
// serialize them on the client side and hide a race).
const clients = Array.from({ length: 10 }, () => new Redis(url));
const redis = clients[0] as Redis;
const createdKeys: string[] = [];

function newKey(label: string): string {
  const key = uniqueName(`lab02-${label}`);
  createdKeys.push(key);
  return key;
}

/** Run `callers` decrIfPositive calls at the same time and return how many reported success. */
async function hammer(key: string, callers: number): Promise<number> {
  const results = await Promise.all(
    Array.from({ length: callers }, (_, i) =>
      decrIfPositive(clients[i % clients.length] as Redis, key),
    ),
  );
  return results.filter(Boolean).length;
}

beforeAll(async () => {
  // Connect everything up front so the first calls of the test are not delayed by handshakes.
  await Promise.all(clients.map((c) => c.ping()));
});

afterEach(async () => {
  if (createdKeys.length > 0) await redis.del(...createdKeys.splice(0));
});

afterAll(() => {
  for (const c of clients) c.disconnect();
});

describe("lab-02 pipeline and lua", () => {
  it("pipeline_uses_fewer_round_trips_than_sequential", async () => {
    const prefix = newKey("pipe");
    const keys = Array.from({ length: 100 }, (_, i) => `${prefix}:${i}`);
    createdKeys.push(...keys);
    const counter = await countSocketWrites(redis);

    counter.reset();
    await setSequential(redis, prefix, 100);
    const sequentialTrips = counter.writes();

    counter.reset();
    await setPipelined(redis, prefix, 100);
    const pipelinedTrips = counter.writes();

    // Each awaited command is one write + one reply; the pipeline sends all 100 in one write.
    expect(sequentialTrips).toBe(100);
    expect(pipelinedTrips).toBe(1);
    // Fewer round trips, same effect: all 100 keys exist.
    expect(await redis.mget(...keys)).toEqual(Array.from({ length: 100 }, () => "1"));
  });

  it("decr_if_positive_never_goes_below_zero_under_50_concurrent_callers", async () => {
    const key = newKey("never-negative");
    await redis.set(key, "10");

    const successes = await hammer(key, CALLERS);

    // 50 callers raced for 10 units: the counter stops at 0 and never turns negative.
    expect(await redis.get(key)).toBe("0");
    expect(successes).toBeLessThanOrEqual(10);
  });

  it("exactly_n_callers_succeed_when_counter_is_n", async () => {
    for (const n of [1, 20, CALLERS]) {
      const key = newKey(`exact-${n}`);
      await redis.set(key, String(n));
      expect(await hammer(key, CALLERS)).toBe(n);
      expect(await redis.get(key)).toBe("0");
    }
  });

  it("decr_if_positive_returns_false_and_creates_nothing_for_a_missing_key", async () => {
    const key = newKey("missing");
    expect(await decrIfPositive(redis, key)).toBe(false);
    expect(await redis.exists(key)).toBe(0);
  });

  it("decr_if_positive_returns_false_at_zero", async () => {
    const key = newKey("zero");
    await redis.set(key, "0");
    expect(await decrIfPositive(redis, key)).toBe(false);
    expect(await redis.get(key)).toBe("0");
  });

  it("noscript_after_script_flush_is_recovered_by_the_client", async () => {
    const key = newKey("noscript");
    await redis.set(key, "2");
    expect(await decrIfPositive(redis, key)).toBe(true); // script is now cached on the server

    // The server forgets every cached script (this also happens on restart or failover).
    await redis.script("FLUSH");

    // EVALSHA fails with NOSCRIPT inside ioredis, which reloads the script and retries.
    expect(await decrIfPositive(redis, key)).toBe(true);
    expect(await redis.get(key)).toBe("0");
  });
});
