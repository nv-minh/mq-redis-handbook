// Demo competing consumers: 4 worker cùng một consumer group chia nhau 20 message (mỗi message đúng một worker),
// rồi một consumer "chết" giữa chừng bỏ lại một message trong PEL và một worker còn sống claim nó.
// Cách chia việc giữa các worker thay đổi mỗi lần chạy, phần khác thì cố định. Stream bị xóa ở cuối.
import { Redis } from "ioredis";
import { eventually, uniqueName } from "@handbook/testkit";
import { WorkerGroup, type Delivery } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const redis = new Redis(url);
const stream = uniqueName("demo:jobs");
const groupName = "workers";
const minIdleMs = 300;
const group = new WorkerGroup({ redis, stream, group: groupName, minIdleMs, blockMs: 100 });
const handles: { stop(): Promise<void> }[] = [];
let stopFailure: unknown;

try {
  await group.ensureGroup();

  console.log("== 1. Bốn worker chia nhau 20 message ==");
  const total = 20;
  for (let i = 0; i < total; i++) await group.publish({ job: `job-${i}` });
  const perWorker = new Map<string, number>();
  let processed = 0;
  const handle = group.startWorkers(4, async (delivery: Delivery) => {
    perWorker.set(delivery.consumer, (perWorker.get(delivery.consumer) ?? 0) + 1);
    processed += 1;
  });
  handles.push(handle);
  await eventually(async () => processed >= total && (await group.pendingEntries()).length === 0, {
    timeoutMs: 20_000,
  });
  await handle.stop();
  console.log(`đã xử lý ${processed}/${total} message, PEL rỗng`);
  for (const [consumer, count] of [...perWorker].sort())
    console.log(`  ${consumer}: ${count} message`);

  console.log("\n== 2. Một consumer chết giữa chừng, worker còn sống tiếp quản ==");
  const lostId = await group.publish({ job: "job-mồ-côi" });
  // Mô phỏng consumer chết: đọc bằng XREADGROUP rồi bỏ đó, không xử lý, không XACK.
  await redis.xreadgroup("GROUP", groupName, "consumer-dead", "STREAMS", stream, ">");
  console.log("PEL sau khi consumer chết:", JSON.stringify(await group.pendingEntries()));
  await eventually(
    async () => {
      const [entry] = await group.pendingEntries();
      return entry !== undefined && entry.idleMs >= 2 * minIdleMs;
    },
    { timeoutMs: 10_000 },
  );
  const rescued: Delivery[] = [];
  const rescuers = group.startWorkers(2, async (delivery) => {
    rescued.push(delivery);
  });
  handles.push(rescuers);
  await eventually(async () => rescued.length >= 1 && (await group.pendingEntries()).length === 0, {
    timeoutMs: 20_000,
  });
  await rescuers.stop();
  const [first] = rescued;
  console.log(
    `${first?.consumer} đã claim ${lostId} (deliveryCount ${first?.deliveryCount}), xử lý và ack. PEL rỗng.`,
  );
  console.log(
    "Giao hàng là at-least-once: message có thể được xử lý lại, nên handler phải idempotent.",
  );
} finally {
  const results = await Promise.allSettled(handles.map((handle) => handle.stop()));
  await redis.del(stream);
  redis.disconnect();
  stopFailure = (results.find((r) => r.status === "rejected") as PromiseRejectedResult | undefined)
    ?.reason;
}
// Lỗi của stop() chỉ được báo khi phần chính đã chạy xong (không che lỗi của phần chính).
if (stopFailure) throw stopFailure;
