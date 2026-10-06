import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { uniqueName } from "@handbook/testkit";
import { countSocketWrites, decrIfPositive, setPipelined, setSequential } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const CALLERS = 50;
// Nhiều connection, để các caller thực sự đập vào server song song (một socket sẽ
// tuần tự hóa chúng ở phía client và che mất race).
const clients = Array.from({ length: 10 }, () => new Redis(url));
const redis = clients[0] as Redis;
const createdKeys: string[] = [];

function newKey(label: string): string {
  const key = uniqueName(`lab02-${label}`);
  createdKeys.push(key);
  return key;
}

/** Chạy `callers` lời gọi decrIfPositive cùng lúc và trả về số lời gọi báo thành công. */
async function hammer(key: string, callers: number): Promise<number> {
  const results = await Promise.all(
    Array.from({ length: callers }, (_, i) =>
      decrIfPositive(clients[i % clients.length] as Redis, key),
    ),
  );
  return results.filter(Boolean).length;
}

beforeAll(async () => {
  // Kết nối mọi client từ đầu để các lời gọi đầu tiên của test không bị chậm vì handshake.
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

    // Mỗi lệnh được await là một write + một reply; pipeline gửi cả 100 lệnh trong một write.
    expect(sequentialTrips).toBe(100);
    expect(pipelinedTrips).toBe(1);
    // Ít round trip hơn, cùng kết quả: cả 100 key đều tồn tại.
    expect(await redis.mget(...keys)).toEqual(Array.from({ length: 100 }, () => "1"));
  });

  it("decr_if_positive_never_goes_below_zero_under_50_concurrent_callers", async () => {
    const key = newKey("never-negative");
    await redis.set(key, "10");

    const successes = await hammer(key, CALLERS);

    // 50 caller tranh nhau 10 đơn vị: counter dừng ở 0 và không bao giờ âm.
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
    expect(await decrIfPositive(redis, key)).toBe(true); // script giờ đã được cache trên server

    // Server quên mọi script đã cache (điều này cũng xảy ra khi restart hoặc failover).
    await redis.script("FLUSH");

    // EVALSHA fail với NOSCRIPT bên trong ioredis, ioredis nạp lại script và thử lại.
    expect(await decrIfPositive(redis, key)).toBe(true);
    expect(await redis.get(key)).toBe("0");
  });
});
