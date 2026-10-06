import { afterEach, describe, expect, it } from "vitest";
import type { ChannelModel, ConfirmChannel, ConsumeMessage, GetMessage } from "amqplib";
import { eventually, uniqueName } from "@handbook/testkit";
import {
  attemptsOf,
  openConnection,
  publishWork,
  setupRetryTopology,
  startWorker,
  type RetryQueues,
} from "./lab.js";

const connections: ChannelModel[] = [];
const names: string[] = [];

async function open(): Promise<ChannelModel> {
  const connection = await openConnection();
  connections.push(connection);
  return connection;
}

interface Setup {
  connection: ChannelModel;
  channel: ConfirmChannel;
  queues: RetryQueues;
}

/** Dựng topology retry với tên duy nhất. Tên được đăng ký trước để afterEach xóa dù test fail. */
async function setup(opts: { maxRetries: number; retryDelayMs: number }): Promise<Setup> {
  const name = uniqueName("lab03-retry");
  names.push(name);
  const connection = await open();
  const channel = await connection.createConfirmChannel();
  const queues = await setupRetryTopology(channel, name, opts);
  return { connection, channel, queues };
}

/** Số message đang ready trong queue. */
async function readyCount(connection: ChannelModel, queue: string): Promise<number> {
  const channel = await connection.createChannel();
  try {
    return (await channel.checkQueue(queue)).messageCount;
  } finally {
    await channel.close().catch(() => undefined);
  }
}

/** Chờ tới khi DLQ có đủ `count` message. */
async function waitForDlq(s: Setup, count: number): Promise<void> {
  await eventually(
    async () => (await readyCount(s.connection, s.queues.dlq)) === count || undefined,
    {
      timeoutMs: 15_000,
    },
  );
}

/** Mỗi lần gọi handler là một lần xử lý. Handler luôn ném lỗi để ép message đi hết vòng retry. */
function alwaysFailing(calls: number[] = []): (msg: Buffer) => Promise<void> {
  return async () => {
    calls.push(performance.now());
    throw new Error("lỗi giả lập: downstream không phản hồi");
  };
}

afterEach(async () => {
  await Promise.all(connections.splice(0).map((c) => c.close().catch(() => undefined)));
  if (names.length === 0) return;
  const cleaner = await openConnection();
  try {
    const channel = await cleaner.createChannel();
    // Tên queue và exchange của topology đều là `<name>.work`, `<name>.retry`, `<name>.dlq`.
    const all = names.splice(0).flatMap((n) => [`${n}.work`, `${n}.retry`, `${n}.dlq`]);
    for (const queue of all) await channel.deleteQueue(queue);
    for (const exchange of all) await channel.deleteExchange(exchange);
  } finally {
    await cleaner.close().catch(() => undefined);
  }
});

describe("lab-03 retry với DLX và TTL", () => {
  it("failing_message_is_retried_max_retries_times_then_lands_in_dlq", async () => {
    const maxRetries = 3;
    const s = await setup({ maxRetries, retryDelayMs: 100 });
    expect(new Set([s.queues.work, s.queues.retry, s.queues.dlq]).size).toBe(3);
    const calls: number[] = [];
    await startWorker(s.channel, s.queues, alwaysFailing(calls), {
      consumerTag: uniqueName("worker"),
    });

    await publishWork(s.channel, s.queues, "poison");
    await waitForDlq(s, 1);

    // Lần xử lý đầu tiên cộng với maxRetries lần retry.
    expect(calls).toHaveLength(maxRetries + 1);
    // Message không bị mất và không kẹt ở đâu khác: đúng một bản ở DLQ, work và retry đều rỗng.
    expect(await readyCount(s.connection, s.queues.work)).toBe(0);
    expect(await readyCount(s.connection, s.queues.retry)).toBe(0);
    const dead = await s.channel.get(s.queues.dlq, { noAck: true });
    expect(dead).not.toBe(false);
    expect((dead as GetMessage).content.toString()).toBe("poison");
  });

  it("dlq_message_carries_x_death_count_and_reason", async () => {
    const s = await setup({ maxRetries: 2, retryDelayMs: 100 });
    await startWorker(s.channel, s.queues, alwaysFailing(), { consumerTag: uniqueName("worker") });

    await publishWork(s.channel, s.queues, "poison");
    await waitForDlq(s, 1);

    const dead = (await s.channel.get(s.queues.dlq, { noAck: true })) as GetMessage;
    const headers = dead.properties.headers ?? {};
    // x-death là array các table, một phần tử cho mỗi cặp {queue, reason}, lần dead-letter gần nhất đứng đầu.
    const xDeath = headers["x-death"] ?? [];
    expect(xDeath).toHaveLength(2);
    expect(xDeath[0]).toMatchObject({
      queue: s.queues.retry,
      reason: "expired",
      count: 2,
      exchange: s.queues.retry,
      "routing-keys": ["retry"],
    });
    expect(xDeath[1]).toMatchObject({
      queue: s.queues.work,
      reason: "rejected",
      count: 2,
      exchange: s.queues.work,
      "routing-keys": ["task"],
    });
    // Hình dạng đo được với amqplib 2.2.0: count là number, time là object timestamp.
    expect(typeof xDeath[1]?.count).toBe("number");
    expect(xDeath[1]?.time).toMatchObject({ "!": "timestamp", value: expect.any(Number) });
    expect(headers["x-first-death-reason"]).toBe("rejected");
    expect(headers["x-first-death-queue"]).toBe(s.queues.work);
    // Hai header do worker thêm khi chuyển message sang DLQ: lý do lỗi cuối cùng và tổng số lần đã xử lý.
    expect(headers["x-failure-reason"]).toBe("lỗi giả lập: downstream không phản hồi");
    expect(headers["x-attempts"]).toBe(3);
  });

  it("retry_respects_retry_delay", async () => {
    const retryDelayMs = 300;
    const s = await setup({ maxRetries: 1, retryDelayMs });
    // Thời điểm handler thất bại ngay trước khi worker reject, và thời điểm message được giao lại (cùng đồng hồ performance.now).
    const calls: number[] = [];
    await startWorker(s.channel, s.queues, alwaysFailing(calls), {
      consumerTag: uniqueName("worker"),
    });

    await publishWork(s.channel, s.queues, "poison");
    await waitForDlq(s, 1);

    expect(calls).toHaveLength(2);
    const elapsed = (calls[1] ?? 0) - (calls[0] ?? 0);
    // Chỉ khẳng định cận dưới (sai số 20 ms), không bao giờ khẳng định cận trên chặt: tải của máy làm thời gian dài ra.
    expect(elapsed).toBeGreaterThanOrEqual(retryDelayMs - 20);
  });

  it("max_retries_zero_sends_first_failure_straight_to_dlq", async () => {
    const s = await setup({ maxRetries: 0, retryDelayMs: 100 });
    const calls: number[] = [];
    await startWorker(s.channel, s.queues, alwaysFailing(calls), {
      consumerTag: uniqueName("worker"),
    });

    await publishWork(s.channel, s.queues, "poison");
    await waitForDlq(s, 1);

    expect(calls).toHaveLength(1);
    expect(await readyCount(s.connection, s.queues.retry)).toBe(0);
    const dead = (await s.channel.get(s.queues.dlq, { noAck: true })) as GetMessage;
    // Message chưa từng đi qua queue retry nên chưa có x-death, chỉ có lý do lỗi do worker ghi.
    expect(dead.properties.headers?.["x-death"]).toBeUndefined();
    expect(dead.properties.headers?.["x-failure-reason"]).toBe(
      "lỗi giả lập: downstream không phản hồi",
    );
    expect(dead.properties.headers?.["x-attempts"]).toBe(1);
  });
  it("handler_throwing_undefined_still_goes_through_the_retry_path", async () => {
    const s = await setup({ maxRetries: 1, retryDelayMs: 100 });
    const calls: number[] = [];
    // Handler ném `undefined` (không phải Error): worker vẫn phải coi đây là thất bại, không được ack như thành công.
    await startWorker(
      s.channel,
      s.queues,
      async () => {
        calls.push(performance.now());
        throw undefined;
      },
      { consumerTag: uniqueName("worker") },
    );

    await publishWork(s.channel, s.queues, "poison");
    await waitForDlq(s, 1);

    expect(calls).toHaveLength(2);
    const dead = (await s.channel.get(s.queues.dlq, { noAck: true })) as GetMessage;
    expect(dead.properties.headers?.["x-failure-reason"]).toBe("undefined");
    expect(dead.properties.headers?.["x-attempts"]).toBe(2);
  });

  it("nack_without_requeue_dead_letters_and_raises_x_death_count", async () => {
    const s = await setup({ maxRetries: 5, retryDelayMs: 100 });
    // Không dùng worker của lab: tự consume để nack lần đầu bằng basic.nack (requeue=false) rồi quan sát lần giao lại.
    const deliveries: ConsumeMessage[] = [];
    const secondDelivery = new Promise<ConsumeMessage>((resolve) => {
      void s.channel.consume(s.queues.work, (msg) => {
        if (msg === null) return;
        if (deliveries.length === 0) {
          deliveries.push(msg);
          s.channel.nack(msg, false, false);
        } else {
          s.channel.ack(msg);
          resolve(msg);
        }
      });
    });

    await publishWork(s.channel, s.queues, "poison");
    const second = await secondDelivery;

    // nack(requeue=false) dead-letter giống hệt reject(requeue=false): message đi qua queue retry và x-death của queue work tăng.
    expect(attemptsOf(second.properties.headers, s.queues.work)).toBe(1);
    expect(second.properties.headers?.["x-first-death-reason"]).toBe("rejected");
  });
});
