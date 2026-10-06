import { afterAll, afterEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { eventually, uniqueName } from "@handbook/testkit";
import { StreamQueue, type StreamMessage } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
// One connection for normal commands. Every consumer gets its own connection for XREADGROUP BLOCK.
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

/** One consumer-side object: shared commands connection, its own blocking connection. */
function newQueue(stream: string): StreamQueue {
  const blocking = new Redis(url);
  blockingConnections.push(blocking);
  const queue = new StreamQueue({ redis, blocking, stream, blockMs: BLOCK_MS });
  blockingOf.set(queue, blocking);
  return queue;
}

/** Simulate a crash: the consumer vanishes without acking and its connection is dropped. */
function crash(queue: StreamQueue): void {
  blockingOf.get(queue)?.disconnect();
}

afterEach(async () => {
  for (const connection of blockingConnections.splice(0)) connection.disconnect();
  // DEL removes the stream together with its consumer groups and their pending lists.
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

    // Two consumers of the same group read at the same time until everything has been delivered.
    const received: Record<string, string[]> = { c1: [], c2: [] };
    const readAll = async (queue: StreamQueue, name: string) => {
      while (received.c1!.length + received.c2!.length < total) {
        const batch = await queue.consume(group, name, 2);
        received[name]!.push(...batch.map((m) => m.id));
      }
    };
    await Promise.all([readAll(c1, "c1"), readAll(c2, "c2")]);

    // Each id was delivered exactly once, and together the consumers got every message.
    const all = [...received.c1!, ...received.c2!];
    expect(all).toHaveLength(total);
    expect(new Set(all).size).toBe(total);
    expect([...all].sort(compareIds)).toEqual(published);

    // The server agrees: the pending list holds each id once, owned by the consumer that received it.
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
    // Delivered but not acked: all three sit in the pending list (PEL).
    expect(await queue.pendingCount(group)).toBe(3);

    expect(await queue.ack(group, messages[0]!.id)).toBe(1);
    expect(await queue.pendingCount(group)).toBe(2);
    // Acking the same id again removes nothing.
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

    // Consumer A reads the entry and "crashes": it never acks and its connection is dropped.
    const delivered = await dead.consume(group, "consumer-a", 1);
    expect(delivered.map((m) => m.id)).toEqual([id]);
    crash(dead);

    // Wait (no sleep) until the entry has been idle for at least twice minIdleMs, so the claim
    // below is far from the boundary of "idle time greater than min-idle-time".
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

    // The entry moved to consumer B's pending list, and the claim counted as a second delivery.
    const [entry] = await rescuer.pendingEntries(group);
    expect(entry).toMatchObject({ id, consumer: "consumer-b", deliveryCount: 2 });
    expect(entry!.idleMs).toBeLessThan(idleBeforeClaim); // the claim reset the idle time

    // B finishes the job.
    expect(await rescuer.ack(group, id)).toBe(1);
    expect(await rescuer.pendingCount(group)).toBe(0);
  });

  it("claim_stale_with_nothing_pending_returns_empty_result", async () => {
    const stream = newStreamKey();
    const group = uniqueName("workers");
    const queue = newQueue(stream);
    await queue.createGroup(group);

    // Group exists but nothing was ever delivered.
    expect(await queue.claimStale(group, "consumer-b", 100)).toEqual({
      messages: [],
      deletedIds: [],
    });

    // Delivered and acked: the pending list is empty again.
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

    // One minute of min idle against an entry that was delivered milliseconds ago.
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
    // The entry leaves the stream (XDEL, or trimming) while it is still in the pending list.
    expect(await redis.xdel(stream, id)).toBe(1);
    await eventually(
      async () => {
        const [entry] = await queue.pendingEntries(group);
        return entry !== undefined && entry.idleMs >= 2 * minIdleMs;
      },
      { timeoutMs: 10_000 },
    );

    // Redis 7.0+: XAUTOCLAIM does not claim it, drops it from the PEL and returns its id as the
    // third element of the reply.
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
    // The second XGROUP CREATE fails with BUSYGROUP on the server, and the lab swallows exactly that.
    await expect(queue.createGroup(group)).resolves.toBeUndefined();
    // Other errors are not swallowed: a group on a key of the wrong type is a WRONGTYPE error.
    const notAStream = newStreamKey();
    await redis.set(notAStream, "x");
    await expect(newQueue(notAStream).createGroup(group)).rejects.toThrow(/WRONGTYPE/);
  });
});

/** Stream ids order by millisecond part, then by sequence part. */
function compareIds(a: string, b: string): number {
  const [aMs, aSeq] = a.split("-").map(Number) as [number, number];
  const [bMs, bSeq] = b.split("-").map(Number) as [number, number];
  return aMs - bMs || aSeq - bSeq;
}
