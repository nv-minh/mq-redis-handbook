import { afterAll, afterEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { eventually, uniqueName } from "@handbook/testkit";
import { dedupKey, processOnce } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
// Nhiều connection để các lời gọi đồng thời thật sự đi trên các socket khác nhau.
const connections = Array.from({ length: 5 }, () => new Redis(url));
const rdb = connections[0]!;
const messageIds: string[] = [];

/** Đăng ký id trước khi dùng để afterEach luôn xóa key, kể cả khi test fail giữa chừng. */
function newMessageId(): string {
  const id = uniqueName("lab01-msg");
  messageIds.push(id);
  return id;
}

/** Promise mở bằng tay: effect đứng chờ ở đây để test điều khiển được thời điểm effect kết thúc. */
function gate(): { promise: Promise<void>; open: () => void; fail: (error: Error) => void } {
  let open!: () => void;
  let fail!: (error: Error) => void;
  const promise = new Promise<void>((resolve, reject) => {
    open = resolve;
    fail = reject;
  });
  return { promise, open, fail };
}

afterEach(async () => {
  const keys = messageIds.splice(0).map(dedupKey);
  if (keys.length > 0) await rdb.del(...keys);
});

afterAll(() => {
  for (const connection of connections) connection.disconnect();
});

describe("lab-01 idempotency: SET NX EX dedup key", () => {
  it("duplicate_delivery_applies_effect_once", async () => {
    const id = newMessageId();
    let applied = 0;
    const effect = async () => {
      applied += 1;
    };

    expect(await processOnce(rdb, id, effect, 60)).toBe("applied");
    // Broker giao lại cùng message (at-least-once): effect không chạy lần hai.
    expect(await processOnce(rdb, id, effect, 60)).toBe("duplicate");
    expect(await processOnce(rdb, id, effect, 60)).toBe("duplicate");
    expect(applied).toBe(1);

    // Sau khi thành công key vẫn còn và có TTL: đó chính là bộ nhớ dedup.
    expect(await rdb.exists(dedupKey(id))).toBe(1);
    const ttl = await rdb.ttl(dedupKey(id));
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(60);
  });

  it("different_message_ids_are_independent", async () => {
    const a = newMessageId();
    const b = newMessageId();
    const applied: string[] = [];
    expect(await processOnce(rdb, a, async () => void applied.push("a"), 60)).toBe("applied");
    expect(await processOnce(rdb, b, async () => void applied.push("b"), 60)).toBe("applied");
    expect(applied).toEqual(["a", "b"]);
  });

  it("failed_effect_releases_key_so_retry_can_apply", async () => {
    const id = newMessageId();
    let successes = 0;
    const boom = new Error("downstream timeout");

    // Effect lỗi: lỗi gốc được ném lại nguyên vẹn và key bị nhả để lần giao lại còn áp dụng được.
    const failure = await processOnce(
      rdb,
      id,
      async () => {
        throw boom;
      },
      60,
    ).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(failure).toBe(boom);
    expect(await rdb.exists(dedupKey(id))).toBe(0);

    // Redelivery: lần này effect thành công và được áp dụng.
    const retry = await processOnce(
      rdb,
      id,
      async () => {
        successes += 1;
      },
      60,
    );
    expect(retry).toBe("applied");
    expect(successes).toBe(1);
    expect(await rdb.exists(dedupKey(id))).toBe(1);
  });

  it("concurrent_duplicates_apply_effect_once", async () => {
    const id = newMessageId();
    const callers = 20;
    let effectCalls = 0;
    const effectGate = gate();
    const effect = async () => {
      effectCalls += 1;
      // Người thắng giữ effect đang chạy cho tới khi 19 người còn lại đã có kết quả,
      // nên 20 lời gọi chắc chắn chồng lên nhau.
      await effectGate.promise;
    };

    let settled = 0;
    const calls = Array.from({ length: callers }, (_, i) =>
      processOnce(connections[i % connections.length]!, id, effect, 60).finally(() => {
        settled += 1;
      }),
    );
    // Hiện tại mới 19 lời gọi xong, người thắng vẫn còn chờ gate.
    await eventually(async () => settled === callers - 1, { timeoutMs: 10_000 });
    effectGate.open();
    const results = await Promise.all(calls);

    expect(effectCalls).toBe(1);
    expect(results.filter((r) => r === "applied")).toHaveLength(1);
    expect(results.filter((r) => r === "duplicate")).toHaveLength(callers - 1);
  });

  it("in_flight_duplicate_is_reported_duplicate_and_lost_when_first_effect_fails", async () => {
    // Cửa sổ đã biết của pattern: trong lúc effect đầu còn chạy, bản trùng nhận 'duplicate'
    // (consumer thật sẽ ack nó). Nếu effect đầu rồi lỗi và nhả key thì bản trùng đã ack đó mất luôn.
    const id = newMessageId();
    let successes = 0;
    const firstGate = gate();
    const boom = new Error("effect đầu lỗi");

    const first = processOnce(rdb, id, () => firstGate.promise, 60).then(
      () => undefined,
      (error: unknown) => error,
    );
    // Chờ tới khi lời gọi đầu đã chiếm key (effect đang bay).
    await eventually(async () => (await rdb.exists(dedupKey(id))) === 1, { timeoutMs: 10_000 });

    const second = await processOnce(
      connections[1]!,
      id,
      async () => {
        successes += 1;
      },
      60,
    );
    expect(second).toBe("duplicate");

    firstGate.fail(boom);
    expect(await first).toBe(boom);

    // Kết quả: key đã nhả, effect chưa từng được áp dụng, mà bản trùng đã nhận "duplicate" (đã ack).
    expect(await rdb.exists(dedupKey(id))).toBe(0);
    expect(successes).toBe(0);
  });

  it("key_expires_after_ttl_so_later_duplicate_is_applied_again", async () => {
    // Tính chất của pattern: dedup chỉ nhớ trong ttlSeconds. TTL phải lớn hơn cửa sổ redeliver.
    const id = newMessageId();
    let applied = 0;
    const effect = async () => {
      applied += 1;
    };

    expect(await processOnce(rdb, id, effect, 1)).toBe("applied");
    // Ngay sau khi thành công key còn đó (mới vài ms trôi qua, còn rất xa mốc hết hạn 1 giây).
    expect(await rdb.exists(dedupKey(id))).toBe(1);

    await eventually(async () => (await rdb.exists(dedupKey(id))) === 0, { timeoutMs: 10_000 });
    expect(await processOnce(rdb, id, effect, 1)).toBe("applied");
    expect(applied).toBe(2);
  });

  it("invalid_ttl_is_rejected_before_touching_redis", async () => {
    const id = newMessageId();
    for (const ttl of [0, -5, 1.5, Number.NaN]) {
      await expect(processOnce(rdb, id, async () => {}, ttl)).rejects.toThrow(/ttlSeconds/);
    }
    expect(await rdb.exists(dedupKey(id))).toBe(0);
  });
});
