// Demo DLQ trên Redis Streams: một poison message bị thử lại bằng XAUTOCLAIM tới maxAttempts rồi vào DLQ
// kèm metadata, sau khi "sửa bug" thì replay về stream chính. Demo cũng mô phỏng một consumer chết giữa chừng.
// Mọi stream bị xóa ở cuối.
import { Redis } from "ioredis";
import { eventually, uniqueName } from "@handbook/testkit";
import { DlqQueue } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const redis = new Redis(url);
const blocking = new Redis(url);
const stream = uniqueName("demo:orders");
const dlqStream = `${stream}:dlq`;
const group = "workers";
const minIdleMs = 200;
const queue = new DlqQueue({
  redis,
  blocking,
  stream,
  group,
  dlqStream,
  consumer: "consumer-1",
  minIdleMs,
  blockMs: 100,
});

try {
  await queue.ensureGroup();

  console.log("== 1. Poison message: retry bằng XAUTOCLAIM rồi vào DLQ ==");
  await queue.publish({ order: "1", note: "ok" });
  const poisonId = await queue.publish({ order: "2", note: "không parse được" });
  const stats = await queue.consumeWithDlq({ maxAttempts: 3 }, async (delivery) => {
    console.log(`  handler nhận ${delivery.id} (lần giao thứ ${delivery.deliveryCount})`);
    if (delivery.fields.order === "2") throw new Error("cannot parse order 2");
  });
  console.log("thống kê:", JSON.stringify(stats));
  console.log(
    `message poison ${poisonId} đã được XACK khỏi main stream:`,
    await queue.pendingEntries(),
  );
  for (const entry of await queue.dlqEntries()) {
    console.log("DLQ entry:", JSON.stringify(entry.fields, null, 2));
  }

  console.log("\n== 2. Sửa bug rồi replay DLQ về main stream ==");
  console.log(
    "replayDlq(10) đã chuyển:",
    await queue.replayDlq(10),
    "entry (limit lớn hơn kích thước DLQ)",
  );
  console.log("replayDlq(10) lần nữa trên DLQ rỗng:", await queue.replayDlq(10));
  const replayed = await queue.consumeWithDlq({ maxAttempts: 3 }, async (delivery) => {
    console.log(`  handler đã sửa xử lý thành công`, JSON.stringify(delivery.fields));
  });
  console.log("thống kê sau replay:", JSON.stringify(replayed));

  console.log("\n== 3. Consumer chết giữa chừng: delivery count vượt maxAttempts thì vào DLQ ==");
  await queue.publish({ order: "3" });
  // Mô phỏng consumer chết: đọc bằng XREADGROUP rồi bỏ đó, không xử lý, không XACK.
  await redis.xreadgroup("GROUP", group, "consumer-dead", "STREAMS", stream, ">");
  await eventually(
    async () => {
      const [entry] = await queue.pendingEntries();
      return entry !== undefined && entry.idleMs >= 2 * minIdleMs;
    },
    { timeoutMs: 10_000 },
  );
  const rescued = await queue.consumeWithDlq({ maxAttempts: 1 }, async () => {
    console.log("  handler không được gọi trong kịch bản này");
  });
  console.log("thống kê:", JSON.stringify(rescued));
  for (const entry of await queue.dlqEntries()) console.log("DLQ entry:", entry.fields.error);
  console.log(
    "Cửa sổ crash giữa XADD vào DLQ và XACK: entry có thể vào DLQ hai lần (at-least-once), xem README.",
  );
} finally {
  await redis.del(stream, dlqStream);
  blocking.disconnect();
  redis.disconnect();
}
