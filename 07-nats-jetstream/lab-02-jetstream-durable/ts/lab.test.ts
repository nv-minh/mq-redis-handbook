import { afterEach, describe, expect, it } from "vitest";
import { eventually, uniqueName } from "@handbook/testkit";
import {
  bindConsumer,
  consumerInfo,
  createStreamAndConsumer,
  disconnect,
  fetchMessages,
  publishMessages,
  teardown,
  watchMaxDeliveries,
} from "./lab.js";

const names: string[] = [];

/** Đăng ký tên trước khi tạo để afterEach luôn dọn, kể cả khi bước tạo bị lỗi giữa chừng. */
function newName(): string {
  const name = uniqueName("lab02");
  names.push(name);
  return name;
}

afterEach(async () => {
  // allSettled: một teardown lỗi không được làm các stream còn lại bị bỏ sót. Lỗi đầu tiên vẫn được ném ra.
  const results = await Promise.allSettled(names.splice(0).map((name) => teardown(name)));
  const failed = results.find((r) => r.status === "rejected");
  if (failed) throw (failed as PromiseRejectedResult).reason;
});

const bodies = (count: number, prefix = "job") =>
  Array.from({ length: count }, (_, i) => `${prefix}-${i}`);

describe("lab-02 JetStream durable consumer: ack, AckWait, MaxDeliver", () => {
  it("durable_consumer_resumes_after_reconnect", async () => {
    const total = 10;
    const consumed = 4;
    const name = newName();
    const consumer = await createStreamAndConsumer(name, { ackWaitMs: 5_000, maxDeliver: 5 });
    await publishMessages(name, bodies(total));

    // Nhận 4 message đầu và ack. ackAck chờ server xác nhận đã ghi ack, nên không còn ack nào đang bay khi ngắt kết nối.
    const first = await fetchMessages(consumer, consumed, 5_000);
    expect(first.map((m) => m.string())).toEqual(bodies(consumed));
    for (const m of first) expect(await m.ackAck()).toBe(true);

    // Ngắt kết nối thật: vị trí đọc nằm ở server (durable consumer), không nằm ở client.
    await disconnect(name);

    // Connection mới bind vào cùng durable: chỉ nhận phần còn lại, không nhận lại message đã ack.
    const resumed = await bindConsumer(name);
    const rest = await fetchMessages(resumed, total - consumed, 5_000);
    expect(rest.map((m) => m.string())).toEqual(bodies(total, "job").slice(consumed));
    for (const m of rest) expect(m.info.deliveryCount).toBe(1);
    for (const m of rest) await m.ackAck();

    const info = await consumerInfo(name);
    expect(info.num_ack_pending).toBe(0);
    expect(info.num_pending).toBe(0);
    expect(info.ack_floor.stream_seq).toBe(total);
  });

  it("unacked_message_is_redelivered_after_ack_wait", async () => {
    const ackWaitMs = 1_000;
    const name = newName();
    const consumer = await createStreamAndConsumer(name, { ackWaitMs, maxDeliver: 5 });
    await publishMessages(name, ["job-1"]);

    const [first] = await fetchMessages(consumer, 1, 5_000);
    expect(first?.string()).toBe("job-1");
    expect(first?.info.deliveryCount).toBe(1);
    expect(first?.redelivered).toBe(false);
    // Không ack: server coi worker đã chết khi hết AckWait. Đồng hồ monotonic của client chỉ dùng để chặn dưới.
    const receivedAt = performance.now();

    // Poll bằng fetch có hạn (expires 1 s): hết AckWait thì server giao lại cho pull request đang chờ.
    const second = await eventually(
      async () => (await fetchMessages(consumer, 1, 1_000))[0] ?? undefined,
      { timeoutMs: 20_000, intervalMs: 50 },
    );
    const elapsed = performance.now() - receivedAt;

    expect(second.string()).toBe("job-1");
    expect(second.info.deliveryCount).toBe(2);
    expect(second.redelivered).toBe(true);
    // Chỉ khẳng định cận dưới. Timer của server chạy từ lúc giao nên client đo hụt vài ms, vì vậy chừa 20% dung sai.
    expect(elapsed).toBeGreaterThanOrEqual(ackWaitMs * 0.8);

    expect(await second.ackAck()).toBe(true);
    expect((await consumerInfo(name)).num_ack_pending).toBe(0);
  });

  it("message_stops_after_max_deliver", async () => {
    const maxDeliver = 3;
    const name = newName();
    const consumer = await createStreamAndConsumer(name, { ackWaitMs: 500, maxDeliver });
    // Subscribe advisory trước khi publish: watchMaxDeliveries đã flush nên server chắc chắn thấy subscription.
    const watch = await watchMaxDeliveries(name);
    await publishMessages(name, ["poison"]);

    // Handler không bao giờ ack. Vòng lặp chạy tới khi server báo MAX_DELIVERIES, nên không cần đoán thời gian.
    const deliveries: number[] = [];
    await eventually(
      async () => {
        for (const m of await fetchMessages(consumer, 1, 1_000))
          deliveries.push(m.info.deliveryCount);
        return watch.advisories.length > 0 || undefined;
      },
      { timeoutMs: 30_000, intervalMs: 20 },
    );

    // Phía client: đúng maxDeliver lần giao với deliveryCount 1, 2, 3.
    expect(deliveries).toEqual([1, 2, 3]);
    // Phía server: advisory đúng một lần, nói rõ message nào và đã giao bao nhiêu lần.
    expect(watch.advisories).toHaveLength(1);
    expect(watch.advisories[0]).toMatchObject({
      stream: name,
      consumer: name,
      stream_seq: 1,
      deliveries: 3,
    });

    // Sau advisory: không còn pending ack, không còn gì để giao, và một fetch có hạn không nhận được gì.
    expect(await fetchMessages(consumer, 1, 1_000)).toEqual([]);
    const info = await consumerInfo(name);
    expect(info.num_ack_pending).toBe(0);
    expect(info.num_pending).toBe(0);
    expect(info.delivered.consumer_seq).toBe(maxDeliver);
    expect(info.delivered.stream_seq).toBe(1);
    // Round trip trên connection của watch: nếu còn advisory thứ hai thì nó đã tới trước PONG.
    await watch.flush();
    expect(watch.advisories).toHaveLength(1);
  });

  it("max_deliver_larger_than_attempts_does_not_stop_early", async () => {
    const name = newName();
    const consumer = await createStreamAndConsumer(name, { ackWaitMs: 5_000, maxDeliver: 5 });
    const watch = await watchMaxDeliveries(name);
    await publishMessages(name, ["flaky"]);

    // Hai lần đầu nak (giao lại ngay, không chờ AckWait), lần thứ ba ack: 3 lần giao chưa chạm maxDeliver = 5.
    const deliveries: number[] = [];
    await eventually(
      async () => {
        for (const m of await fetchMessages(consumer, 1, 1_000)) {
          deliveries.push(m.info.deliveryCount);
          if (m.info.deliveryCount < 3) m.nak();
          else return await m.ackAck();
        }
        return undefined;
      },
      { timeoutMs: 20_000, intervalMs: 20 },
    );

    expect(deliveries).toEqual([1, 2, 3]);
    await watch.flush();
    expect(watch.advisories).toEqual([]);
    const info = await consumerInfo(name);
    expect(info.num_ack_pending).toBe(0);
    expect(info.ack_floor.stream_seq).toBe(1);
  });

  it("fetch_on_empty_stream_returns_nothing_within_bound", async () => {
    const name = newName();
    const consumer = await createStreamAndConsumer(name, { ackWaitMs: 5_000, maxDeliver: 5 });

    // Stream rỗng: fetch có expires trả về mảng rỗng khi hết hạn (server trả 408), không treo và không ném lỗi.
    const started = performance.now();
    expect(await fetchMessages(consumer, 5, 1_000)).toEqual([]);
    expect(performance.now() - started).toBeLessThan(10_000);
  });

  it("invalid_ack_wait_and_max_deliver_are_rejected", async () => {
    // Server nhận ack_wait = 0 và im lặng đổi thành 30 s, max_deliver = 0 hoặc -2 thành -1 (đã đo).
    // Lab chặn từ phía client để giá trị sai không biến thành giá trị mặc định khác hẳn ý định.
    const name = newName();
    await expect(createStreamAndConsumer(name, { ackWaitMs: 0, maxDeliver: 3 })).rejects.toThrow(
      /ackWaitMs/,
    );
    await expect(createStreamAndConsumer(name, { ackWaitMs: -5, maxDeliver: 3 })).rejects.toThrow(
      /ackWaitMs/,
    );
    await expect(
      createStreamAndConsumer(name, { ackWaitMs: Number.NaN, maxDeliver: 3 }),
    ).rejects.toThrow(/ackWaitMs/);
    await expect(
      createStreamAndConsumer(name, { ackWaitMs: 1_000, maxDeliver: 0 }),
    ).rejects.toThrow(/maxDeliver/);
    await expect(
      createStreamAndConsumer(name, { ackWaitMs: 1_000, maxDeliver: -2 }),
    ).rejects.toThrow(/maxDeliver/);
  });
});
