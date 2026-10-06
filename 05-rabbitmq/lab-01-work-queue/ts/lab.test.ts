import { afterEach, describe, expect, it } from "vitest";
import type { ChannelModel, ConfirmChannel, ConsumeMessage } from "amqplib";
import { eventually, uniqueName } from "@handbook/testkit";
import { declareWorkQueue, openConnection, publishTasks, startWorker } from "./lab.js";

const connections: ChannelModel[] = [];
const queues: string[] = [];

/** Mở một connection và đăng ký để afterEach luôn đóng nó, kể cả khi test fail. */
async function open(): Promise<ChannelModel> {
  const connection = await openConnection();
  connections.push(connection);
  return connection;
}

function newQueue(): string {
  const queue = uniqueName("lab01-work");
  queues.push(queue);
  return queue;
}

/** Channel publish ở confirm mode: publishTasks chỉ return sau khi broker đã nhận hết. */
async function publisherFor(connection: ChannelModel, queue: string): Promise<ConfirmChannel> {
  const channel = await connection.createConfirmChannel();
  await declareWorkQueue(channel, queue);
  return channel;
}

function deferred<T = void>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

/** Số message đang ready (chưa giao cho consumer nào) trong queue. */
async function readyCount(connection: ChannelModel, queue: string): Promise<number> {
  const channel = await connection.createChannel();
  try {
    return (await channel.checkQueue(queue)).messageCount;
  } finally {
    await channel.close().catch(() => undefined);
  }
}

afterEach(async () => {
  // Đóng hết connection của test trước, rồi xóa queue bằng một connection mới (channel của test có thể đã chết vì lỗi).
  await Promise.all(connections.splice(0).map((c) => c.close().catch(() => undefined)));
  if (queues.length === 0) return;
  const cleaner = await openConnection();
  try {
    const channel = await cleaner.createChannel();
    for (const queue of queues.splice(0)) await channel.deleteQueue(queue);
  } finally {
    await cleaner.close().catch(() => undefined);
  }
});

describe("lab-01 work queue: prefetch, ack và redelivery", () => {
  it("prefetch_1_gives_slow_worker_fewer_messages", async () => {
    const total = 10;
    const queue = newQueue();
    const connection = await open();
    const publisher = await publisherFor(connection, queue);
    const slowChannel = await connection.createChannel();
    const fastChannel = await connection.createChannel();

    const slowReceived = deferred();
    const release = deferred();
    const slowBodies: string[] = [];
    const fastBodies: string[] = [];

    // Worker chậm giữ message đầu tiên của nó cho tới khi test cho phép, nên nó không bao giờ ack trong lúc test quan sát.
    await startWorker(
      slowChannel,
      queue,
      async (msg) => {
        slowBodies.push(msg.content.toString());
        slowReceived.resolve();
        await release.promise;
      },
      { prefetch: 1, consumerTag: uniqueName("slow") },
    );
    // Worker nhanh ack ngay, nhưng chỉ bắt đầu sau khi worker chậm đã nhận message của nó.
    // Nhờ vậy kết quả không phụ thuộc việc broker giao message đầu tiên cho worker nào.
    await startWorker(
      fastChannel,
      queue,
      async (msg) => {
        await slowReceived.promise;
        fastBodies.push(msg.content.toString());
      },
      { prefetch: 1, consumerTag: uniqueName("fast") },
    );

    await publishTasks(
      publisher,
      queue,
      Array.from({ length: total }, (_, i) => `task-${i}`),
    );

    // Prefetch 1: worker chậm chỉ giữ đúng 1 message chưa ack, nên 9 message còn lại đều về worker nhanh.
    await eventually(async () => fastBodies.length === total - 1 || undefined, {
      timeoutMs: 10_000,
    });
    expect(slowBodies).toHaveLength(1);
    expect(fastBodies).toHaveLength(total - 1);
    expect(await readyCount(connection, queue)).toBe(0);
    // Mọi message đều có chủ: không message nào bị trùng giữa hai worker.
    expect(new Set([...slowBodies, ...fastBodies]).size).toBe(total);

    release.resolve();
  });

  it("unacked_message_is_redelivered_after_worker_disconnects", async () => {
    const queue = newQueue();
    const connectionA = await open();
    const connectionB = await open();
    const publisher = await publisherFor(connectionB, queue);

    // Worker A nhận message nhưng không bao giờ ack (handler treo mãi), rồi connection của nó bị đóng: mô phỏng crash.
    const aReceived = deferred<ConsumeMessage>();
    await startWorker(
      await connectionA.createChannel(),
      queue,
      async (msg) => {
        aReceived.resolve(msg);
        await new Promise<void>(() => undefined);
      },
      { prefetch: 1, consumerTag: uniqueName("worker-a") },
    );
    await publishTasks(publisher, queue, ["job-1"]);
    const first = await aReceived.promise;
    expect(first.content.toString()).toBe("job-1");
    expect(first.fields.redelivered).toBe(false);

    await connectionA.close();

    // Broker requeue message chưa ack khi connection đóng, worker B nhận lại đúng message đó với cờ redelivered.
    const bReceived = deferred<ConsumeMessage>();
    await startWorker(
      await connectionB.createChannel(),
      queue,
      async (msg) => bReceived.resolve(msg),
      { prefetch: 1, consumerTag: uniqueName("worker-b") },
    );
    const second = await bReceived.promise;
    expect(second.content.toString()).toBe("job-1");
    expect(second.fields.redelivered).toBe(true);
  });

  it("get_on_empty_queue_returns_nothing", async () => {
    const queue = newQueue();
    const connection = await open();
    const channel = await publisherFor(connection, queue);

    // basic.get trên queue rỗng trả get-empty, amqplib hiển thị là false (không có message, không có lỗi).
    expect(await channel.get(queue, { noAck: true })).toBe(false);
  });

  it("reject_requeue_counts_toward_delivery_count_but_nack_does_not", async () => {
    const connection = await open();

    // Giao lần đầu chưa có header nào. Mỗi lần reject(requeue=true) rồi giao lại, x-delivery-count tăng 1.
    const rejectQueue = newQueue();
    const rejectChannel = await publisherFor(connection, rejectQueue);
    const rejected: ConsumeMessage[] = [];
    await rejectChannel.consume(rejectQueue, (msg) => {
      if (msg === null) return;
      rejected.push(msg);
      if (rejected.length < 3) rejectChannel.reject(msg, true);
      else rejectChannel.ack(msg);
    });
    await publishTasks(rejectChannel, rejectQueue, ["m"]);
    await eventually(async () => rejected.length === 3 || undefined, { timeoutMs: 10_000 });
    expect(rejected[0]?.properties.headers?.["x-delivery-count"]).toBeUndefined();
    // Đo trên RabbitMQ 4.3.6: lần giao lại đầu tiên có x-delivery-count = 1 (không phải 0).
    expect(rejected[1]?.properties.headers?.["x-delivery-count"]).toBe(1);
    expect(rejected[1]?.properties.headers?.["x-acquired-count"]).toBe(1);
    expect(rejected[2]?.properties.headers?.["x-delivery-count"]).toBe(2);

    // Từ 4.3, nack(requeue=true) chỉ tăng x-acquired-count, không tăng x-delivery-count.
    const nackQueue = newQueue();
    const nackChannel = await publisherFor(connection, nackQueue);
    const nacked: ConsumeMessage[] = [];
    await nackChannel.consume(nackQueue, (msg) => {
      if (msg === null) return;
      nacked.push(msg);
      if (nacked.length < 3) nackChannel.nack(msg, false, true);
      else nackChannel.ack(msg);
    });
    await publishTasks(nackChannel, nackQueue, ["m"]);
    await eventually(async () => nacked.length === 3 || undefined, { timeoutMs: 10_000 });
    expect(nacked[1]?.properties.headers?.["x-delivery-count"]).toBeUndefined();
    expect(nacked[1]?.properties.headers?.["x-acquired-count"]).toBe(1);
    expect(nacked[2]?.properties.headers?.["x-acquired-count"]).toBe(2);
  });

  it("without_prefetch_quorum_queue_delivers_at_most_2000_unacked", async () => {
    const queue = newQueue();
    const connection = await open();
    const publisher = await publisherFor(connection, queue);
    const consumer = await connection.createChannel();

    // Consumer không gọi basic.qos và không bao giờ ack: prefetch không giới hạn.
    let received = 0;
    await consumer.consume(queue, (msg) => {
      if (msg !== null) received++;
    });
    const published = 2100;
    await publishTasks(
      publisher,
      queue,
      Array.from({ length: published }, (_, i) => `m-${i}`),
    );

    // Quorum queue tự chặn ở 2000 unacked: 100 message còn lại nằm ready trong queue.
    await eventually(async () => (await readyCount(connection, queue)) === 100 || undefined, {
      timeoutMs: 15_000,
    });
    await eventually(async () => received === 2000 || undefined, { timeoutMs: 15_000 });
  });
});
