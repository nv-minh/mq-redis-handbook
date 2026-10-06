import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { eventually, uniqueName } from "@handbook/testkit";
import {
  assertOwnRedis,
  deleteByPrefix,
  evictedKeys,
  fillUntilEviction,
  fillUntilRejected,
  guardProblem,
  limitMemory,
  readConfig,
  seedKeys,
  writeConfig,
  type EvictionConfig,
} from "./lab.js";

// Lab này THAY ĐỔI setting maxmemory của server mà nó kết nối tới (và khôi phục lại).
// Chỉ chạy nó với Redis của compose stack trong handbook này (`make up`).
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

/** Trả server về như cũ trước (để lệnh xóa không bao giờ bị chặn vì đầy bộ nhớ), rồi mới xóa key của test. */
async function restore(): Promise<void> {
  await writeConfig(redis, original);
  for (const prefix of prefixes.splice(0)) await deleteByPrefix(redis, prefix);
}

beforeAll(async () => {
  // Từ chối trước mọi lệnh CONFIG SET: sai server hoặc còn config cũ. `original` không được đặt khi
  // bị từ chối, nên afterAll không bao giờ ghi config ngược lại.
  await assertOwnRedis(redis);
  // Lưu TRƯỚC lần CONFIG SET đầu tiên, để luôn có cái mà khôi phục.
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

    // 1. Nạp các key lạnh và key nóng khi chưa có giới hạn.
    await seedKeys(redis, coldPrefix, COLD_KEYS, VALUE_SIZE);
    await redis.set(hot, "hot");

    // 2. Idle time của LRU có độ phân giải một giây: chờ tới khi các key lạnh trông đã cũ.
    await eventually(async () => Number(await redis.object("IDLETIME", `${coldPrefix}:0`)) >= 2, {
      timeoutMs: 10_000,
    });
    await redis.get(hot); // đọc key nóng đặt lại idle time của nó về 0

    // 3. Giới hạn bộ nhớ ngay trên mức đang dùng, rồi tiếp tục ghi trong khi chạm vào key nóng.
    await limitMemory(redis, "allkeys-lru", HEADROOM);
    const evicted = await fillUntilEviction(redis, `${prefix}:fill`, 20_000, {
      touchKey: hot,
      minEvicted: 900,
    });

    // Không bao giờ assert một con số chính xác: eviction chỉ là xấp xỉ (dựa trên sampling).
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
    // Lệnh đọc vẫn chạy, không có gì bị evict, và dữ liệu ghi trước giới hạn vẫn còn.
    expect(await redis.get(`${prefix}:existing`)).toBe("still readable");
    expect(await evictedKeys(redis)).toBe(evictedBefore);
    expect(await redis.exists(`${prefix}:fill:0`)).toBe(1);
  });

  it("volatile_policy_rejects_writes_when_no_key_has_a_ttl", async () => {
    const prefix = newPrefix("volatile");
    const evictedBefore = await evictedKeys(redis);

    // volatile-lru chỉ được evict các key có TTL. Không key nào của ta có, nên nó hành xử như noeviction.
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

describe("lab-03 safety guard", () => {
  it("guard_refuses_a_redis_without_the_handbook_marker_and_changes_nothing", async () => {
    const before = await readConfig(redis);
    await expect(assertOwnRedis(redis, "some-other-marker.rdb")).rejects.toThrow(
      /refusing to run.*not the handbook marker/,
    );
    expect(await readConfig(redis)).toEqual(before); // chỉ đọc: không có CONFIG SET nào xảy ra
  });

  it("guard_refuses_leftover_config_with_a_hint_to_recreate_the_stack", () => {
    const state = { dbfilename: "mq-handbook.rdb", maxmemory: "4000000", policy: "allkeys-lru" };
    expect(guardProblem(state, "mq-handbook.rdb")).toMatch(/make down && make up/);
    expect(
      guardProblem({ ...state, maxmemory: "0", policy: "noeviction" }, "mq-handbook.rdb"),
    ).toBeNull();
  });
});
