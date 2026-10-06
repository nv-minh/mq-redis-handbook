import { afterAll, afterEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { uniqueName } from "@handbook/testkit";
import { findBigKeys } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const redis = new Redis(url);
const createdKeys: string[] = [];

// UNLINK giải phóng value lớn ở thread nền, nên việc dọn dẹp không chặn Redis (DEL thì có thể chặn).
afterEach(async () => {
  if (createdKeys.length > 0) await redis.unlink(...createdKeys.splice(0));
});
afterAll(() => {
  redis.disconnect();
});

const KIB = 1024;
const MIB = 1024 * KIB;

/** Tạo `count` key string rất nhỏ dưới `prefix`. */
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

    // Một chuỗi 1 MiB và một list khoảng 1 MiB gồm nhiều phần tử nhỏ (big key do nhiều phần tử, không do một value lớn).
    const bigString = `${prefix}:big-string`;
    const bigList = `${prefix}:big-list`;
    createdKeys.push(bigString, bigList);
    await redis.set(bigString, "x".repeat(MIB));
    const list = redis.pipeline();
    for (let i = 0; i < 100; i++)
      list.rpush(bigList, ...Array.from({ length: 100 }, () => "y".repeat(100)));
    await list.exec();

    // Ngưỡng 512 KiB nằm giữa các key nhỏ (vài chục byte) và hai key lớn (khoảng 1 MiB).
    const found = await findBigKeys(redis, 512 * KIB, `${prefix}:*`);

    // Chỉ trả về các key lớn, key lớn nhất đứng đầu, và không có key nhỏ nào lọt vào.
    expect(found).toEqual([bigString, bigList]);
    expect(found.some((k) => k.includes(":small:"))).toBe(false);
  });

  it("find_big_keys_ignores_small_keys", async () => {
    const prefix = uniqueName("lab04-smallkeys");
    await createSmallKeys(prefix, 500);
    // Value 100 KiB là lớn với một entry cache nhưng vẫn dưới ngưỡng 512 KiB.
    const medium = `${prefix}:medium`;
    createdKeys.push(medium);
    await redis.set(medium, "m".repeat(100 * KIB));

    expect(await findBigKeys(redis, 512 * KIB, `${prefix}:*`)).toEqual([]);
    // Hạ ngưỡng xuống dưới key cỡ vừa thì cùng dữ liệu đó được tìm thấy,
    // chứng tỏ lần quét đã đi qua nó và chỉ có ngưỡng quyết định kết quả.
    expect(await findBigKeys(redis, 50 * KIB, `${prefix}:*`)).toEqual([medium]);
  });
});
