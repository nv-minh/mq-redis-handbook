import { afterAll, afterEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { eventually, uniqueName } from "@handbook/testkit";
import { ReliableQueue } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
// One connection for normal commands. Every consumer gets its own connection for BLMOVE.
const redis = new Redis(url);
const blockingConnections: Redis[] = [];
const blockingOf = new WeakMap<ReliableQueue, Redis>();
const createdKeys: string[] = [];

function newQueueKey(): string {
  const key = uniqueName("lab03-list");
  createdKeys.push(key);
  return key;
}

/** One consumer-side queue object: shared commands connection, its own blocking connection. */
function newQueue(queueKey: string, blockTimeoutSeconds = 1): ReliableQueue {
  const blocking = new Redis(url);
  blockingConnections.push(blocking);
  const queue = new ReliableQueue({ redis, blocking, queue: queueKey, blockTimeoutSeconds });
  blockingOf.set(queue, blocking);
  return queue;
}

/** Simulate a crash: the consumer vanishes without acking and its connection is dropped. */
function crash(queue: ReliableQueue): void {
  blockingOf.get(queue)?.disconnect();
}

/** The processing lists of the consumers a test used are keyed under the queue key. */
function registerProcessingKeys(queue: ReliableQueue, ...consumerIds: string[]): void {
  for (const id of consumerIds) createdKeys.push(queue.processingKey(id));
}

afterEach(async () => {
  for (const connection of blockingConnections.splice(0)) connection.disconnect();
  if (createdKeys.length > 0) await redis.del(...createdKeys.splice(0));
});

afterAll(() => {
  redis.disconnect();
});

describe("lab-02 list queue: reliable queue with BLMOVE", () => {
  it("message_stays_in_processing_list_until_ack", async () => {
    const queueKey = newQueueKey();
    const queue = newQueue(queueKey);
    registerProcessingKeys(queue, "worker-1");
    await queue.enqueue("job-1");
    expect(await redis.lrange(queueKey, 0, -1)).toEqual(["job-1"]);

    expect(await queue.dequeue("worker-1")).toBe("job-1");

    // BLMOVE moved the message atomically: gone from the queue, parked in the processing list.
    expect(await redis.lrange(queueKey, 0, -1)).toEqual([]);
    expect(await redis.lrange(queue.processingKey("worker-1"), 0, -1)).toEqual(["job-1"]);

    // Only the ack removes it from the processing list.
    expect(await queue.ack("worker-1", "job-1")).toBe(true);
    expect(await redis.lrange(queue.processingKey("worker-1"), 0, -1)).toEqual([]);
  });

  it("message_is_recovered_after_consumer_crash", async () => {
    const queueKey = newQueueKey();
    const crashing = newQueue(queueKey);
    const survivor = newQueue(queueKey);
    registerProcessingKeys(survivor, "worker-a", "worker-b");
    await survivor.enqueue("job-1");

    // worker-a takes the message and "crashes": it never acks, and its connection is dropped.
    expect(await crashing.dequeue("worker-a")).toBe("job-1");
    crash(crashing);
    expect(await redis.lrange(survivor.processingKey("worker-a"), 0, -1)).toEqual(["job-1"]);
    expect(await redis.lrange(queueKey, 0, -1)).toEqual([]);

    // A recovery pass puts the dead consumer's message back on the queue.
    expect(await survivor.recoverStale("worker-a")).toBe(1);
    expect(await redis.lrange(survivor.processingKey("worker-a"), 0, -1)).toEqual([]);

    // Another consumer now receives the very same message: at-least-once delivery.
    expect(await survivor.dequeue("worker-b")).toBe("job-1");
    expect(await survivor.ack("worker-b", "job-1")).toBe(true);
    expect(await redis.lrange(queueKey, 0, -1)).toEqual([]);
    expect(await redis.lrange(survivor.processingKey("worker-b"), 0, -1)).toEqual([]);
  });

  it("dequeue_on_empty_queue_returns_null_after_the_block_timeout", async () => {
    const queue = newQueue(newQueueKey(), 1);
    registerProcessingKeys(queue, "worker-1");

    const started = Date.now();
    expect(await queue.dequeue("worker-1")).toBeNull();
    const waitedMs = Date.now() - started;

    // BLMOVE really blocked for about the timeout (it cannot return earlier on an empty list),
    // and an empty result leaves nothing in the processing list.
    expect(waitedMs).toBeGreaterThanOrEqual(900);
    expect(await redis.lrange(queue.processingKey("worker-1"), 0, -1)).toEqual([]);
  });

  it("blocked_dequeue_wakes_up_when_a_message_arrives", async () => {
    const queueKey = newQueueKey();
    const queue = newQueue(queueKey, 10);
    registerProcessingKeys(queue, "worker-1");
    const blockedBefore = await blockedClients();

    const pending = queue.dequeue("worker-1");
    // Wait until the server reports one more blocked client: BLMOVE is parked on the empty list.
    await eventually(async () => (await blockedClients()) > blockedBefore, { timeoutMs: 5000 });

    await queue.enqueue("late-job");
    // The push wakes the blocked client right away, long before the 10 second timeout.
    expect(await pending).toBe("late-job");
  });

  it("messages_are_delivered_in_fifo_order", async () => {
    const queueKey = newQueueKey();
    const queue = newQueue(queueKey);
    registerProcessingKeys(queue, "worker-1");
    for (const message of ["a", "b", "c"]) await queue.enqueue(message);

    expect(await queue.dequeue("worker-1")).toBe("a");
    expect(await queue.dequeue("worker-1")).toBe("b");
    expect(await queue.dequeue("worker-1")).toBe("c");
  });

  it("recover_stale_on_empty_processing_list_returns_zero", async () => {
    const queue = newQueue(newQueueKey());
    expect(await queue.recoverStale("nobody")).toBe(0);
  });

  it("recovered_messages_are_redelivered_in_their_original_order", async () => {
    const queueKey = newQueueKey();
    const crashing = newQueue(queueKey);
    const survivor = newQueue(queueKey);
    registerProcessingKeys(survivor, "worker-a", "worker-b");
    for (const message of ["a", "b", "c"]) await survivor.enqueue(message);
    for (let i = 0; i < 3; i++) await crashing.dequeue("worker-a");
    crash(crashing);

    expect(await survivor.recoverStale("worker-a")).toBe(3);

    expect(await survivor.dequeue("worker-b")).toBe("a");
    expect(await survivor.dequeue("worker-b")).toBe("b");
    expect(await survivor.dequeue("worker-b")).toBe("c");
  });

  it("ack_of_a_message_not_in_the_processing_list_returns_false", async () => {
    const queue = newQueue(newQueueKey());
    registerProcessingKeys(queue, "worker-1");
    expect(await queue.ack("worker-1", "never-dequeued")).toBe(false);
  });
});

/** `blocked_clients` from INFO: clients currently waiting in a blocking command. */
async function blockedClients(): Promise<number> {
  const info = await redis.info("clients");
  const match = /blocked_clients:(\d+)/.exec(info);
  return match ? Number(match[1]) : 0;
}
