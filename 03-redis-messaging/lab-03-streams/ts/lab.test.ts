import { afterAll, afterEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { eventually, uniqueName } from "@handbook/testkit";
import { StreamQueue, type StreamMessage } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
// Một connection cho các lệnh thường. Mỗi consumer có connection riêng cho XREADGROUP BLOCK.
const redis = new Redis(url);
const blockingConnections: Redis[] = [];
const blockingOf = new WeakMap<StreamQueue, Redis>();
const streamKeys: string[] = [];
const BLOCK_MS = 200;

function newStreamKey(): string {
  const key = uniqueName("lab03-stream");
  streamKeys.push(key);
  return key;
}

/** Một đối tượng phía consumer: connection lệnh dùng chung, connection blocking riêng. */
function newQueue(stream: string): StreamQueue {
  const blocking = new Redis(url);
  blockingConnections.push(blocking);
  const queue = new StreamQueue({ redis, blocking, stream, blockMs: BLOCK_MS });
  blockingOf.set(queue, blocking);
  return queue;
}

/** Mô phỏng crash: consumer biến mất mà không ack và connection của nó bị cắt. */
function crash(queue: StreamQueue): void {
  blockingOf.get(queue)?.disconnect();
}

afterEach(async () => {
  for (const connection of blockingConnections.splice(0)) connection.disconnect();
  // DEL xóa stream cùng với các consumer group và pending list của chúng.
  if (streamKeys.length > 0) await redis.del(...streamKeys.splice(0));
});

afterAll(() => {
  redis.disconnect();
});

describe("lab-03 streams: consumer groups", () => {
  it("each_message_goes_to_one_consumer_in_group", async () => {
    const stream = newStreamKey();
    const group = uniqueName("workers");
    const c1 = newQueue(stream);
    const c2 = newQueue(stream);
    await c1.createGroup(group);
    const total = 20;
    const published: string[] = [];
    for (let i = 0; i < total; i++) published.push(await c1.publish({ n: String(i) }));

    // Hai consumer của cùng một group đọc đồng thời cho tới khi mọi thứ đã được giao.
    const received: Record<string, string[]> = { c1: [], c2: [] };
    const readAll = async (queue: StreamQueue, name: string) => {
      while (received.c1!.length + received.c2!.length < total) {
        const batch = await queue.consume(group, name, 2);
        received[name]!.push(...batch.map((m) => m.id));
      }
    };
    await Promise.all([readAll(c1, "c1"), readAll(c2, "c2")]);

    // Mỗi id được giao đúng một lần, và gộp lại các consumer nhận đủ mọi message.
    const all = [...received.c1!, ...received.c2!];
    expect(all).toHaveLength(total);
    expect(new Set(all).size).toBe(total);
    expect([...all].sort(compareIds)).toEqual(published);

    // Server cũng đồng ý: pending list giữ mỗi id một lần, thuộc về consumer đã nhận nó.
    const pending = await c1.pendingEntries(group);
    expect(pending).toHaveLength(total);
    for (const entry of pending) {
      expect(received[entry.consumer]).toContain(entry.id);
      expect(entry.deliveryCount).toBe(1);
    }
  });

  it("xack_removes_entry_from_pending_list", async () => {
    const stream = newStreamKey();
    const group = uniqueName("workers");
    const queue = newQueue(stream);
    await queue.createGroup(group);
    for (let i = 0; i < 3; i++) await queue.publish({ n: String(i) });

    const messages = await queue.consume(group, "c1", 10);
    expect(messages).toHaveLength(3);
    // Đã giao nhưng chưa ack: cả ba nằm trong pending list (PEL).
    expect(await queue.pendingCount(group)).toBe(3);

    expect(await queue.ack(group, messages[0]!.id)).toBe(1);
    expect(await queue.pendingCount(group)).toBe(2);
    // Ack lại cùng một id không xóa thêm gì.
    expect(await queue.ack(group, messages[0]!.id)).toBe(0);
    expect(await queue.pendingCount(group)).toBe(2);

    await queue.ack(group, messages[1]!.id);
    await queue.ack(group, messages[2]!.id);
    expect(await queue.pendingCount(group)).toBe(0);
  });

  it("xautoclaim_recovers_pending_from_dead_consumer", async () => {
    const stream = newStreamKey();
    const group = uniqueName("workers");
    const minIdleMs = 200;
    const dead = newQueue(stream);
    const rescuer = newQueue(stream);
    await dead.createGroup(group);
    const id = await dead.publish({ job: "send-email" });

    // Consumer A đọc entry rồi "crash": nó không bao giờ ack và connection của nó bị cắt.
    const delivered = await dead.consume(group, "consumer-a", 1);
    expect(delivered.map((m) => m.id)).toEqual([id]);
    crash(dead);

    // Chờ (không sleep) tới khi entry đã idle ít nhất gấp đôi minIdleMs, để lần claim
    // bên dưới cách xa ranh giới "idle time lớn hơn min-idle-time".
    const idleBeforeClaim = await eventually(
      async () => {
        const [entry] = await rescuer.pendingEntries(group);
        return entry !== undefined && entry.idleMs >= 2 * minIdleMs && entry.idleMs;
      },
      { timeoutMs: 10_000 },
    );

    const claimed = await rescuer.claimStale(group, "consumer-b", minIdleMs);
    expect(claimed.messages).toEqual([{ id, fields: { job: "send-email" } }]);
    expect(claimed.deletedIds).toEqual([]);

    // Entry đã chuyển sang pending list của consumer B, và lần claim được tính là lần giao thứ hai.
    const [entry] = await rescuer.pendingEntries(group);
    expect(entry).toMatchObject({ id, consumer: "consumer-b", deliveryCount: 2 });
    expect(entry!.idleMs).toBeLessThan(idleBeforeClaim); // lần claim đã đặt lại idle time

    // B hoàn thành công việc.
    expect(await rescuer.ack(group, id)).toBe(1);
    expect(await rescuer.pendingCount(group)).toBe(0);
  });

  it("claim_stale_with_nothing_pending_returns_empty_result", async () => {
    const stream = newStreamKey();
    const group = uniqueName("workers");
    const queue = newQueue(stream);
    await queue.createGroup(group);

    // Group đã tồn tại nhưng chưa từng giao gì.
    expect(await queue.claimStale(group, "consumer-b", 100)).toEqual({
      messages: [],
      deletedIds: [],
    });

    // Đã giao và đã ack: pending list lại rỗng.
    const id = await queue.publish({ n: "1" });
    await queue.consume(group, "consumer-a", 1);
    await queue.ack(group, id);
    expect(await queue.claimStale(group, "consumer-b", 100)).toEqual({
      messages: [],
      deletedIds: [],
    });
  });

  it("xautoclaim_skips_entries_that_are_not_idle_long_enough", async () => {
    const stream = newStreamKey();
    const group = uniqueName("workers");
    const queue = newQueue(stream);
    await queue.createGroup(group);
    const id = await queue.publish({ n: "1" });
    await queue.consume(group, "consumer-a", 1);

    // Min idle một phút áp lên một entry vừa được giao cách đây vài mili giây.
    const claimed = await queue.claimStale(group, "consumer-b", 60_000);
    expect(claimed.messages).toEqual([]);
    const [entry] = await queue.pendingEntries(group);
    expect(entry).toMatchObject({ id, consumer: "consumer-a", deliveryCount: 1 });
  });

  it("xautoclaim_reports_ids_deleted_from_the_stream", async () => {
    const stream = newStreamKey();
    const group = uniqueName("workers");
    const minIdleMs = 100;
    const queue = newQueue(stream);
    await queue.createGroup(group);
    const id = await queue.publish({ n: "1" });
    await queue.consume(group, "consumer-a", 1);
    // Entry rời khỏi stream (XDEL, hoặc trimming) trong khi vẫn còn trong pending list.
    expect(await redis.xdel(stream, id)).toBe(1);
    await eventually(
      async () => {
        const [entry] = await queue.pendingEntries(group);
        return entry !== undefined && entry.idleMs >= 2 * minIdleMs;
      },
      { timeoutMs: 10_000 },
    );

    // Redis 7.0+: XAUTOCLAIM không claim nó, loại nó khỏi PEL và trả về id của nó ở
    // phần tử thứ ba của reply.
    const claimed = await queue.claimStale(group, "consumer-b", minIdleMs);
    expect(claimed.messages).toEqual([]);
    expect(claimed.deletedIds).toEqual([id]);
    expect(await queue.pendingCount(group)).toBe(0);
  });

  it("consume_returns_empty_list_when_no_new_message_arrives_within_the_block_time", async () => {
    const stream = newStreamKey();
    const group = uniqueName("workers");
    const queue = newQueue(stream);
    await queue.createGroup(group);

    const started = Date.now();
    const messages: StreamMessage[] = await queue.consume(group, "c1", 5);

    expect(messages).toEqual([]);
    expect(Date.now() - started).toBeGreaterThanOrEqual(BLOCK_MS - 50);
  });

  it("create_group_is_idempotent", async () => {
    const stream = newStreamKey();
    const group = uniqueName("workers");
    const queue = newQueue(stream);
    await queue.createGroup(group);
    // XGROUP CREATE lần thứ hai bị server báo lỗi BUSYGROUP, và lab chỉ nuốt đúng lỗi đó.
    await expect(queue.createGroup(group)).resolves.toBeUndefined();
    // Các lỗi khác không bị nuốt: tạo group trên key sai kiểu là lỗi WRONGTYPE.
    const notAStream = newStreamKey();
    await redis.set(notAStream, "x");
    await expect(newQueue(notAStream).createGroup(group)).rejects.toThrow(/WRONGTYPE/);
  });
});

/** Id của stream được sắp theo phần mili giây, rồi theo phần sequence. */
function compareIds(a: string, b: string): number {
  const [aMs, aSeq] = a.split("-").map(Number) as [number, number];
  const [bMs, bSeq] = b.split("-").map(Number) as [number, number];
  return aMs - bMs || aSeq - bSeq;
}
