import { afterEach, describe, expect, it } from "vitest";
import type { NatsConnection } from "@nats-io/transport-node";
import { eventually, uniqueName } from "@handbook/testkit";
import { openConnection, publishAll, subscribeCollect } from "./lab.js";
import type { Receiver } from "./lab.js";

const connections: NatsConnection[] = [];

/** Mở một connection và đăng ký để afterEach luôn đóng nó, kể cả khi test fail. */
async function open(): Promise<NatsConnection> {
  const nc = await openConnection();
  connections.push(nc);
  return nc;
}

afterEach(async () => {
  // allSettled: một connection đóng lỗi không được làm các connection còn lại bị bỏ sót.
  const results = await Promise.allSettled(connections.splice(0).map((nc) => nc.close()));
  const failed = results.find((r) => r.status === "rejected");
  if (failed) throw (failed as PromiseRejectedResult).reason;
});

/** Chờ mọi message đã tới client rồi dừng subscription: drain xử lý hết message còn nằm trong buffer. */
async function settle(receivers: Receiver[]): Promise<void> {
  await Promise.all(receivers.map((r) => r.subscription.drain()));
  await Promise.all(receivers.map((r) => r.done));
}

describe("lab-01 core NATS: queue group và at-most-once", () => {
  it("queue_group_delivers_each_message_to_one_member", async () => {
    const total = 60;
    const subject = uniqueName("lab01.orders");
    const queue = uniqueName("packers");
    const bodies = Array.from({ length: total }, (_, i) => `order-${i}`);

    // Mỗi member là một connection riêng, giống ba worker chạy ở ba process khác nhau.
    // subscribeCollect đã flush, nên server chắc chắn biết cả ba member trước khi publish.
    const members: Receiver[] = [];
    for (let i = 0; i < 3; i++) members.push(await subscribeCollect(await open(), subject, queue));

    await publishAll(await open(), subject, bodies);

    await eventually(async () => members.reduce((n, m) => n + m.received.length, 0) >= total, {
      timeoutMs: 10_000,
    });
    await settle(members);

    // Mỗi message tới đúng một member: tổng bằng N, không trùng, hợp của ba member là toàn bộ.
    // Server chọn member ngẫu nhiên nên KHÔNG assert việc chia đều.
    const all = members.flatMap((m) => m.received);
    expect(all).toHaveLength(total);
    expect(new Set(all).size).toBe(total);
    expect([...all].sort()).toEqual([...bodies].sort());
  });

  it("core_nats_drops_messages_when_no_subscriber", async () => {
    const subject = uniqueName("lab01.events");
    const publisher = await open();

    // Chưa có subscriber nào: publish vẫn thành công nhưng server bỏ message, không lưu gì cả.
    // publishAll flush, nên server đã xử lý xong ba message này trước khi subscriber xuất hiện.
    await publishAll(publisher, subject, ["old-1", "old-2", "old-3"]);

    const late = await subscribeCollect(await open(), subject);
    await publishAll(publisher, subject, ["new-1"]);

    // Cùng một connection nhận theo thứ tự: nếu server có giữ message cũ thì chúng đã đến trước new-1.
    await eventually(async () => late.received.length >= 1, { timeoutMs: 10_000 });
    await settle([late]);
    expect(late.received).toEqual(["new-1"]);
  });
});
