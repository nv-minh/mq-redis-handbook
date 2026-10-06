// Demo: ba worker cùng một queue group chia nhau message, rồi cho thấy core NATS bỏ message khi chưa có subscriber.
import type { NatsConnection } from "@nats-io/transport-node";
import { eventually, uniqueName } from "@handbook/testkit";
import { openConnection, publishAll, subscribeCollect } from "./lab.js";
import type { Receiver } from "./lab.js";

const connections: NatsConnection[] = [];

async function open(): Promise<NatsConnection> {
  const nc = await openConnection();
  connections.push(nc);
  return nc;
}

async function settle(receivers: Receiver[]): Promise<void> {
  await Promise.all(receivers.map((r) => r.subscription.drain()));
  await Promise.all(receivers.map((r) => r.done));
}

try {
  const total = 30;
  const subject = uniqueName("demo.orders");
  const queue = uniqueName("packers");

  console.log(`--- queue group: 3 worker cùng group, publish ${total} message ---`);
  const workers: Receiver[] = [];
  for (let i = 0; i < 3; i++) workers.push(await subscribeCollect(await open(), subject, queue));
  const bodies = Array.from({ length: total }, (_, i) => `order-${i}`);
  await publishAll(await open(), subject, bodies);
  await eventually(async () => workers.reduce((n, w) => n + w.received.length, 0) >= total, {
    timeoutMs: 10_000,
  });
  await settle(workers);
  workers.forEach((w, i) => console.log(`worker ${i + 1} nhận ${w.received.length} message`));
  const all = workers.flatMap((w) => w.received);
  console.log(
    `Tổng ${all.length} message, ${new Set(all).size} message khác nhau: mỗi message tới đúng một worker.`,
  );
  console.log(
    "Kết luận: server chọn member ngẫu nhiên nên số message mỗi worker lệch nhau, đừng kỳ vọng chia đều.",
  );

  console.log("--- core NATS không lưu message ---");
  const eventSubject = uniqueName("demo.events");
  const publisher = await open();
  await publishAll(publisher, eventSubject, ["old-1", "old-2", "old-3"]);
  console.log(
    "Đã publish old-1, old-2, old-3 khi chưa có subscriber nào (publish vẫn thành công).",
  );
  const late = await subscribeCollect(await open(), eventSubject);
  await publishAll(publisher, eventSubject, ["new-1"]);
  await eventually(async () => late.received.length >= 1, { timeoutMs: 10_000 });
  await settle([late]);
  console.log(`Subscriber đến sau chỉ nhận: ${JSON.stringify(late.received)}`);
  console.log(
    "Kết luận: core NATS là at-most-once và không có persistence, muốn giữ message phải dùng JetStream (lab 02).",
  );
} finally {
  await Promise.allSettled(connections.map((nc) => nc.close()));
}
