import { afterAll, afterEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { eventually, uniqueName } from "@handbook/testkit";
import {
  acquireLock,
  cacheAsideUnprotected,
  RedisCache,
  releaseLock,
  slidingWindowAllow,
} from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
// Nhiều connection để các caller đồng thời thật sự đi trên các socket khác nhau.
const POOL = 10;
const connections = Array.from({ length: POOL }, () => new Redis(url));
const rdb = connections[0]!;
const conn = (i: number): Redis => connections[i % POOL]!;

// Đồng hồ của test (performance.now) và đồng hồ của server là hai đồng hồ khác nhau,
// nên cận dưới về thời gian chừa dung sai nhỏ cho độ phân giải và sai lệch giữa chúng.
const CLOCK_SLACK_MS = 50;

// Mọi key đều bắt đầu bằng một prefix duy nhất được đăng ký TRƯỚC khi dùng,
// để afterEach dọn bằng SCAN theo prefix dù test fail giữa chừng.
const prefixes: string[] = [];
const gates: Gate[] = [];

function newKey(label: string): string {
  const prefix = uniqueName(`uc03-${label}`);
  prefixes.push(prefix);
  return `${prefix}:key`;
}

interface Gate {
  promise: Promise<void>;
  open: () => void;
}

/** Promise mở bằng tay: loader đứng chờ ở đây để test điều khiển được lúc loader kết thúc. */
function makeGate(): Gate {
  let open!: () => void;
  const promise = new Promise<void>((resolve) => (open = resolve));
  const gate = { promise, open };
  gates.push(gate);
  return gate;
}

/** Escape ký tự glob của MATCH: \ * ? [ ]. */
function escapeGlob(text: string): string {
  return text.replace(/[\\*?[\]]/g, "\\$&");
}

async function scanKeys(prefix: string): Promise<string[]> {
  const found: string[] = [];
  let cursor = "0";
  do {
    const [next, keys] = await rdb.scan(cursor, "MATCH", `${escapeGlob(prefix)}*`, "COUNT", 200);
    found.push(...keys);
    cursor = next;
  } while (cursor !== "0");
  return found;
}

async function deleteByPrefix(prefix: string): Promise<void> {
  const keys = await scanKeys(prefix);
  if (keys.length > 0) await rdb.del(...keys);
}

/**
 * Chạy các pha TUẦN TỰ: pha sau chỉ bắt đầu khi pha trước đã xong hẳn. Một pha lỗi không làm bỏ qua
 * các pha sau (việc dọn vẫn phải chạy tiếp), và lỗi đầu tiên được ném lại ở cuối.
 */
async function runPhases(phases: (() => Promise<unknown>)[]): Promise<void> {
  const errors: unknown[] = [];
  for (const phase of phases) {
    try {
      await phase();
    } catch (error) {
      errors.push(error);
    }
  }
  if (errors.length > 0) throw errors[0];
}

afterEach(async () => {
  // Mở mọi gate để loader còn đang chờ không bị treo.
  for (const gate of gates.splice(0)) gate.open();
  const owned = prefixes.splice(0);
  await runPhases([
    // Pha 1: xóa key của test bằng SCAN theo prefix (không bao giờ KEYS trên Redis dùng chung).
    async () => {
      for (const prefix of owned) await deleteByPrefix(prefix);
    },
    // Pha 2: kiểm tra không còn key nào sót.
    async () => {
      for (const prefix of owned) {
        expect(await scanKeys(prefix), `key còn sót của prefix ${prefix}`).toEqual([]);
      }
    },
  ]);
});

afterAll(async () => {
  // Đóng mọi connection trên mọi đường, kể cả khi quit lỗi.
  await Promise.allSettled(
    connections.map((connection) => connection.quit().catch(() => connection.disconnect())),
  );
});

describe("rate limiter: sliding window log bằng sorted set và Lua", () => {
  it("allows_limit_requests_then_rejects_within_window", async () => {
    const sequential = newKey("rl-seq");
    for (let i = 0; i < 3; i++)
      expect(await slidingWindowAllow(rdb, sequential, 3, 60_000)).toBe(true);
    expect(await slidingWindowAllow(rdb, sequential, 3, 60_000)).toBe(false);

    // 50 caller thật sự đồng thời, trên 10 connection khác nhau, tranh limit = 10: đúng 10 lần được allow.
    const concurrent = newKey("rl-50");
    const results = await Promise.all(
      Array.from({ length: 50 }, (_, i) => slidingWindowAllow(conn(i), concurrent, 10, 60_000)),
    );
    expect(results.filter(Boolean)).toHaveLength(10);
    expect(await rdb.zcard(concurrent)).toBe(10);
  });

  it("window_slides_so_old_requests_stop_counting", async () => {
    const key = newKey("rl-slide");
    const windowMs = 1000;
    const start = performance.now();
    expect(await slidingWindowAllow(rdb, key, 2, windowMs)).toBe(true);
    expect(await slidingWindowAllow(rdb, key, 2, windowMs)).toBe(true);
    // Còn trong cửa sổ thì request thứ ba bị từ chối.
    expect(await slidingWindowAllow(rdb, key, 2, windowMs)).toBe(false);

    // Poll cho tới khi request cũ nhất rời cửa sổ và allow đổi thành true (không sleep cố định).
    await eventually(
      async () => ((await slidingWindowAllow(rdb, key, 2, windowMs)) ? true : undefined),
      {
        timeoutMs: 5000,
        intervalMs: 50,
      },
    );
    expect(performance.now() - start).toBeGreaterThanOrEqual(windowMs - CLOCK_SLACK_MS);
  });

  it("rejected_requests_are_not_recorded", async () => {
    const key = newKey("rl-reject");
    expect(await slidingWindowAllow(rdb, key, 1, 60_000)).toBe(true);
    const before = await rdb.zrange(key, "0", "-1", "WITHSCORES");
    for (let i = 0; i < 5; i++) expect(await slidingWindowAllow(rdb, key, 1, 60_000)).toBe(false);
    // Request bị từ chối không ghi gì: sorted set y nguyên, nên cửa sổ không bị đẩy lùi.
    expect(await rdb.zrange(key, "0", "-1", "WITHSCORES")).toEqual(before);
    expect(before).toHaveLength(2);
  });

  it("limit_zero_always_rejects_and_creates_no_key", async () => {
    const key = newKey("rl-zero");
    for (let i = 0; i < 3; i++) expect(await slidingWindowAllow(rdb, key, 0, 60_000)).toBe(false);
    expect(await rdb.exists(key)).toBe(0);
  });

  it("invalid_limit_or_window_is_rejected_with_a_clear_error", async () => {
    const key = newKey("rl-invalid");
    const bad: [number, number][] = [
      [-1, 1000],
      [1.5, 1000],
      [Number.NaN, 1000],
      [5, 0],
      [5, -100],
      [5, 1.5],
      [5, Number.POSITIVE_INFINITY],
    ];
    for (const [limit, windowMs] of bad) {
      await expect(slidingWindowAllow(rdb, key, limit, windowMs)).rejects.toThrow(RangeError);
    }
    await expect(slidingWindowAllow(rdb, key, -1, 1000)).rejects.toThrow(/limit/);
    await expect(slidingWindowAllow(rdb, key, 5, 0)).rejects.toThrow(/windowMs/);
    expect(await rdb.exists(key)).toBe(0);
  });

  it("key_has_a_ttl_so_idle_keys_do_not_leak", async () => {
    const key = newKey("rl-ttl");
    expect(await slidingWindowAllow(rdb, key, 5, 60_000)).toBe(true);
    const pttl = await rdb.pttl(key);
    expect(pttl).toBeGreaterThan(0);
    expect(pttl).toBeLessThanOrEqual(60_000);
  });

  it("different_keys_do_not_influence_each_other", async () => {
    const a = newKey("rl-a");
    const b = newKey("rl-b");
    expect(await slidingWindowAllow(rdb, a, 1, 60_000)).toBe(true);
    expect(await slidingWindowAllow(rdb, a, 1, 60_000)).toBe(false);
    expect(await slidingWindowAllow(rdb, b, 1, 60_000)).toBe(true);
  });
});

describe("distributed lock: một node Redis, SET NX PX và Lua release", () => {
  it("only_one_of_many_concurrent_callers_gets_the_lock", async () => {
    const key = newKey("lock-race");
    const tokens = await Promise.all(
      Array.from({ length: 30 }, (_, i) => acquireLock(conn(i), key, 30_000)),
    );
    const winners = tokens.filter((token): token is string => token !== null);
    expect(winners).toHaveLength(1);
    // Token 128 bit crypto-random, dạng hex: 32 ký tự.
    expect(winners[0]).toMatch(/^[0-9a-f]{32}$/);
    expect(await rdb.get(key)).toBe(winners[0]);
    expect(await rdb.pttl(key)).toBeGreaterThan(0);
  });

  it("release_with_wrong_token_does_not_release", async () => {
    const key = newKey("lock-wrong");
    const token = await acquireLock(rdb, key, 30_000);
    expect(token).not.toBeNull();

    expect(await releaseLock(rdb, key, "not-the-token")).toBe(false);
    // Lock vẫn còn, người khác vẫn không lấy được, và chủ thật vẫn release được.
    expect(await rdb.get(key)).toBe(token);
    expect(await acquireLock(conn(1), key, 30_000)).toBeNull();
    expect(await releaseLock(rdb, key, token!)).toBe(true);
    expect(await rdb.exists(key)).toBe(0);
    // Release lại hoặc release lock không tồn tại đều trả false, không ném lỗi.
    expect(await releaseLock(rdb, key, token!)).toBe(false);
  });

  it("lock_expires_after_ttl", async () => {
    const key = newKey("lock-ttl");
    const ttlMs = 500;
    const start = performance.now();
    const tokenA = await acquireLock(rdb, key, ttlMs);
    expect(tokenA).not.toBeNull();
    // Còn trong TTL thì người khác chưa lấy được.
    expect(await acquireLock(conn(1), key, 30_000)).toBeNull();

    // Poll cho tới khi lock hết hạn và caller khác lấy được (không sleep cố định).
    const tokenB = await eventually(
      async () => (await acquireLock(conn(1), key, 30_000)) ?? undefined,
      {
        timeoutMs: 5000,
        intervalMs: 50,
      },
    );
    expect(performance.now() - start).toBeGreaterThanOrEqual(ttlMs - CLOCK_SLACK_MS);
    expect(tokenB).not.toBe(tokenA);

    // Holder cũ release muộn: false, và KHÔNG xóa lock của holder mới (lỗi kinh điển mà token ngăn được).
    expect(await releaseLock(rdb, key, tokenA!)).toBe(false);
    expect(await rdb.get(key)).toBe(tokenB);
    expect(await releaseLock(conn(1), key, tokenB)).toBe(true);
  });

  it("expired_holder_still_believes_it_holds_the_lock", async () => {
    // Lỗi mà lock đơn node KHÔNG tránh được: holder A bị pause (GC, network) lâu hơn TTL.
    // Redis không báo gì cho A. Từ lúc B lấy lock, cả A và B đều tin mình đang giữ lock.
    const key = newKey("lock-two-holders");
    const tokenA = await acquireLock(rdb, key, 300);
    expect(tokenA).not.toBeNull();
    const tokenB = await eventually(
      async () => (await acquireLock(conn(1), key, 30_000)) ?? undefined,
      {
        timeoutMs: 5000,
        intervalMs: 50,
      },
    );

    // A vẫn cầm token của mình và không có cách nào biết nó đã mất lock, cho tới khi release.
    expect(tokenA).not.toBe(tokenB);
    expect(await rdb.get(key)).toBe(tokenB);
    // Token chỉ giúp A không xóa nhầm lock của B, không ngăn A ghi vào resource trong lúc đó.
    // Muốn chặn ghi muộn phải có fencing token ở chính resource (xem demo và README).
    expect(await releaseLock(rdb, key, tokenA!)).toBe(false);
  });

  it("delex_ifeq_releases_without_lua_on_redis_8_4_plus", async (context) => {
    // DELEX ... IFEQ có từ Redis 8.4: so sánh rồi xóa trong một lệnh, không cần Lua.
    const info = await rdb.call("COMMAND", "INFO", "DELEX");
    if (!Array.isArray(info) || info[0] === null) context.skip();

    const key = newKey("lock-delex");
    const token = await acquireLock(rdb, key, 30_000);
    expect(token).not.toBeNull();
    expect(await rdb.call("DELEX", key, "IFEQ", "not-the-token")).toBe(0);
    expect(await rdb.get(key)).toBe(token);
    expect(await rdb.call("DELEX", key, "IFEQ", token!)).toBe(1);
    expect(await rdb.exists(key)).toBe(0);
  });

  it("invalid_ttl_is_rejected_with_a_clear_error", async () => {
    const key = newKey("lock-invalid");
    for (const ttlMs of [0, -1, 1.5, Number.NaN]) {
      await expect(acquireLock(rdb, key, ttlMs)).rejects.toThrow(RangeError);
    }
    expect(await rdb.exists(key)).toBe(0);
  });
});

describe("cache-aside với chống stampede trong một process", () => {
  /** Loader đếm số lần được gọi; có thể bị giữ lại bởi một gate. */
  function countingLoader<T>(value: T, gate?: Gate) {
    const state = { calls: 0 };
    const loader = async (): Promise<T> => {
      state.calls += 1;
      if (gate) await gate.promise;
      return value;
    };
    return { state, loader };
  }

  it("hundred_concurrent_cache_misses_call_loader_once", async () => {
    const key = newKey("cache-100");
    const cache = new RedisCache(rdb);
    const gate = makeGate();
    const { state, loader } = countingLoader({ id: 7, name: "Linh" }, gate);

    // 100 caller đồng thời trên key lạnh. Loader bị giữ ở gate cho tới khi cả 100 đã vào cuộc.
    const callers = Array.from({ length: 100 }, () => cache.cacheAside(key, 60, loader));
    await eventually(async () => (state.calls >= 1 ? true : undefined), { timeoutMs: 5000 });
    gate.open();
    const values = await Promise.all(callers);

    expect(state.calls).toBe(1);
    expect(values).toHaveLength(100);
    for (const value of values) expect(value).toEqual({ id: 7, name: "Linh" });
    // Giá trị đã nằm trong Redis dạng JSON và có TTL.
    expect(JSON.parse((await rdb.get(key))!)).toEqual({ id: 7, name: "Linh" });
    const pttl = await rdb.pttl(key);
    expect(pttl).toBeGreaterThan(0);
    expect(pttl).toBeLessThanOrEqual(60_000);
  });

  it("without_protection_every_concurrent_miss_calls_loader", async () => {
    // Đối chứng: cùng kịch bản nhưng không có map in-flight, mỗi caller miss rồi tự gọi loader.
    const key = newKey("cache-unprotected");
    const gate = makeGate();
    const { state, loader } = countingLoader("v", gate);
    const callers = Array.from({ length: 100 }, () => cacheAsideUnprotected(rdb, key, 60, loader));
    // Loader bị giữ ở gate nên chưa có SET nào: cả 100 caller đều miss và gọi loader.
    await eventually(async () => (state.calls === 100 ? true : undefined), { timeoutMs: 10_000 });
    gate.open();
    expect(await Promise.all(callers)).toHaveLength(100);
    expect(state.calls).toBe(100);
  });

  it("cache_hit_does_not_call_loader", async () => {
    const key = newKey("cache-hit");
    const cache = new RedisCache(rdb);
    const { state, loader } = countingLoader("giá trị");
    expect(await cache.cacheAside(key, 60, loader)).toBe("giá trị");
    expect(await cache.cacheAside(key, 60, loader)).toBe("giá trị");
    // Cache của một instance khác (map in-flight riêng) cũng thấy giá trị trong Redis.
    expect(await new RedisCache(conn(1)).cacheAside(key, 60, loader)).toBe("giá trị");
    expect(state.calls).toBe(1);
  });

  it("ttl_expiry_makes_the_next_call_reload", async () => {
    const key = newKey("cache-ttl");
    const cache = new RedisCache(rdb);
    // EX tính bằng giây nguyên, nên TTL nhỏ nhất của API này là 1 giây.
    const ttlSec = 1;
    let version = 0;
    const loader = async () => ++version;
    const start = performance.now();
    expect(await cache.cacheAside(key, ttlSec, loader)).toBe(1);
    expect(await cache.cacheAside(key, ttlSec, loader)).toBe(1);

    // Poll bằng chính cacheAside cho tới khi TTL hết và loader được gọi lại.
    const reloaded = await eventually(
      async () => {
        const value = await cache.cacheAside(key, ttlSec, loader);
        return value === 2 ? value : undefined;
      },
      { timeoutMs: 5000, intervalMs: 50 },
    );
    expect(reloaded).toBe(2);
    expect(performance.now() - start).toBeGreaterThanOrEqual(ttlSec * 1000 - CLOCK_SLACK_MS);
  });

  it("loader_failure_is_not_cached_and_next_call_retries", async () => {
    const key = newKey("cache-fail");
    const cache = new RedisCache(rdb);
    const gate = makeGate();
    const boom = new Error("DB sập");
    let calls = 0;
    const failing = async (): Promise<string> => {
      calls += 1;
      await gate.promise;
      throw boom;
    };

    // 5 waiter chung một lần load: cả 5 nhận cùng một lỗi, loader chỉ chạy một lần.
    const waiters = Array.from({ length: 5 }, () => cache.cacheAside(key, 60, failing));
    const settled = Promise.allSettled(waiters);
    await eventually(async () => (calls >= 1 ? true : undefined), { timeoutMs: 5000 });
    gate.open();
    for (const result of await settled) {
      expect(result.status).toBe("rejected");
      expect((result as PromiseRejectedResult).reason).toBe(boom);
    }
    expect(calls).toBe(1);
    // Lỗi không bị cache: Redis không có key, và entry in-flight đã được xóa.
    expect(await rdb.exists(key)).toBe(0);

    // Lần gọi sau thử lại và thành công.
    expect(await cache.cacheAside(key, 60, async () => "đã ổn")).toBe("đã ổn");
    expect(await rdb.exists(key)).toBe(1);
  });

  it("concurrent_callers_on_two_keys_call_loader_once_per_key", async () => {
    const keyA = newKey("cache-a");
    const keyB = newKey("cache-b");
    const cache = new RedisCache(rdb);
    const gate = makeGate();
    const a = countingLoader("A", gate);
    const b = countingLoader("B", gate);
    const callers = [
      ...Array.from({ length: 20 }, () => cache.cacheAside(keyA, 60, a.loader)),
      ...Array.from({ length: 20 }, () => cache.cacheAside(keyB, 60, b.loader)),
    ];
    await eventually(async () => (a.state.calls >= 1 && b.state.calls >= 1 ? true : undefined), {
      timeoutMs: 5000,
    });
    gate.open();
    const values = await Promise.all(callers);
    expect(values.slice(0, 20).every((v) => v === "A")).toBe(true);
    expect(values.slice(20).every((v) => v === "B")).toBe(true);
    expect([a.state.calls, b.state.calls]).toEqual([1, 1]);
  });

  it("null_value_is_cached_and_distinguished_from_a_miss", async () => {
    const key = newKey("cache-null");
    const cache = new RedisCache(rdb);
    let calls = 0;
    const loader = async (): Promise<string | null> => {
      calls += 1;
      return null;
    };
    expect(await cache.cacheAside(key, 60, loader)).toBeNull();
    // Miss là key không tồn tại (GET trả nil), còn null đã cache là chuỗi JSON "null".
    expect(await rdb.get(key)).toBe("null");
    expect(await cache.cacheAside(key, 60, loader)).toBeNull();
    expect(calls).toBe(1);
  });

  it("undefined_value_is_rejected_and_not_cached", async () => {
    const key = newKey("cache-undefined");
    const cache = new RedisCache(rdb);
    await expect(
      cache.cacheAside(key, 60, async () => undefined as unknown as string),
    ).rejects.toThrow(TypeError);
    expect(await rdb.exists(key)).toBe(0);
  });

  it("invalid_ttl_is_rejected_with_a_clear_error", async () => {
    const key = newKey("cache-invalid");
    const cache = new RedisCache(rdb);
    for (const ttlSec of [0, -1, 1.5, Number.NaN]) {
      await expect(cache.cacheAside(key, ttlSec, async () => "x")).rejects.toThrow(RangeError);
    }
    expect(await rdb.exists(key)).toBe(0);
  });
});
