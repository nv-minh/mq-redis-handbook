import { afterAll, afterEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { eventually, uniqueName } from "@handbook/testkit";
import { ReliableQueue } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
// Một connection cho các lệnh thường. Mỗi consumer có connection riêng cho BLMOVE.
const redis = new Redis(url);
const blockingConnections: Redis[] = [];
const blockingOf = new WeakMap<ReliableQueue, Redis>();
const connectionNameOf = new WeakMap<ReliableQueue, string>();
const createdKeys: string[] = [];

function newQueueKey(): string {
  const key = uniqueName("lab03-list");
  createdKeys.push(key);
  return key;
}

/** Một đối tượng queue phía consumer: connection lệnh dùng chung, connection blocking riêng. */
function newQueue(queueKey: string, blockTimeoutSeconds = 1): ReliableQueue {
  // Tên connection duy nhất giúp test tìm đúng connection này trong CLIENT LIST.
  const connectionName = uniqueName("lab03-blocking");
  const blocking = new Redis(url, { connectionName });
  blockingConnections.push(blocking);
  const queue = new ReliableQueue({ redis, blocking, queue: queueKey, blockTimeoutSeconds });
  blockingOf.set(queue, blocking);
  connectionNameOf.set(queue, connectionName);
  return queue;
}

/** Mô phỏng crash: consumer biến mất mà không ack và connection của nó bị cắt. */
function crash(queue: ReliableQueue): void {
  blockingOf.get(queue)?.disconnect();
}

/** Các processing list của consumer mà test dùng có key nằm dưới key của queue. */
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

    // BLMOVE chuyển message một cách atomic: biến khỏi queue, được cất trong processing list.
    expect(await redis.lrange(queueKey, 0, -1)).toEqual([]);
    expect(await redis.lrange(queue.processingKey("worker-1"), 0, -1)).toEqual(["job-1"]);

    // Chỉ có ack mới xóa nó khỏi processing list.
    expect(await queue.ack("worker-1", "job-1")).toBe(true);
    expect(await redis.lrange(queue.processingKey("worker-1"), 0, -1)).toEqual([]);
  });

  it("message_is_recovered_after_consumer_crash", async () => {
    const queueKey = newQueueKey();
    const crashing = newQueue(queueKey);
    const survivor = newQueue(queueKey);
    registerProcessingKeys(survivor, "worker-a", "worker-b");
    await survivor.enqueue("job-1");

    // worker-a lấy message rồi "crash": nó không bao giờ ack, và connection của nó bị cắt.
    expect(await crashing.dequeue("worker-a")).toBe("job-1");
    crash(crashing);
    expect(await redis.lrange(survivor.processingKey("worker-a"), 0, -1)).toEqual(["job-1"]);
    expect(await redis.lrange(queueKey, 0, -1)).toEqual([]);

    // Một lượt recovery đưa message của consumer đã chết về lại queue.
    expect(await survivor.recoverStale("worker-a")).toBe(1);
    expect(await redis.lrange(survivor.processingKey("worker-a"), 0, -1)).toEqual([]);

    // Một consumer khác giờ nhận đúng message đó: at-least-once delivery.
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

    // BLMOVE thực sự đã chặn khoảng bằng timeout (nó không thể return sớm hơn trên list rỗng),
    // và kết quả rỗng không để lại gì trong processing list.
    expect(waitedMs).toBeGreaterThanOrEqual(900);
    expect(await redis.lrange(queue.processingKey("worker-1"), 0, -1)).toEqual([]);
  });

  it("blocked_dequeue_wakes_up_when_a_message_arrives", async () => {
    const queueKey = newQueueKey();
    const queue = newQueue(queueKey, 10);
    registerProcessingKeys(queue, "worker-1");

    const pending = queue.dequeue("worker-1");
    // Chờ tới khi server báo CHÍNH connection này đang bị chặn trong BLMOVE (flags=b trong CLIENT LIST).
    await eventually(async () => isBlockedInBlmove(connectionNameOf.get(queue) as string), {
      timeoutMs: 5000,
    });

    await queue.enqueue("late-job");
    // Lệnh push đánh thức client đang bị chặn ngay lập tức, rất lâu trước timeout 10 giây.
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

/** True khi client tên `connectionName` đang bị chặn trong BLMOVE, theo CLIENT LIST. */
async function isBlockedInBlmove(connectionName: string): Promise<boolean> {
  const list = (await redis.client("LIST")) as string;
  return list
    .split("\n")
    .some(
      (line) =>
        line.includes(` name=${connectionName} `) &&
        / flags=\S*b\S* /.test(line) &&
        line.includes(" cmd=blmove"),
    );
}
