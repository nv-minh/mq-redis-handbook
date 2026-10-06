// Demo: consumer A đọc hai entry rồi crash mà không XACK, các entry nằm lại trong pending
// list (PEL) của nó, consumer B tiếp quản bằng XAUTOCLAIM rồi ack. Demo cũng in dạng reply
// thô mà ioredis 6.0.0 nhận được trên RESP3. Key của stream bị xóa ở cuối.
import { Redis } from "ioredis";
import { eventually, uniqueName } from "@handbook/testkit";
import { StreamQueue } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const redis = new Redis(url);
const blockingA = new Redis(url);
const blockingB = new Redis(url);
const stream = uniqueName("demo:stream");
const group = "workers";
const minIdleMs = 300;
const consumerA = new StreamQueue({ redis, blocking: blockingA, stream, blockMs: 500 });
const consumerB = new StreamQueue({ redis, blocking: blockingB, stream, blockMs: 500 });

async function showPending(label: string): Promise<void> {
  const entries = await consumerB.pendingEntries(group);
  console.log(`${label}: PEL = ${JSON.stringify(entries)}`);
}

try {
  await consumerA.createGroup(group);
  for (const job of ["job-1", "job-2", "job-3"]) await consumerA.publish({ job });

  const read = await consumerA.consume(group, "consumer-a", 2);
  console.log("consumer-a đã đọc:", JSON.stringify(read));
  await showPending("sau khi đọc, chưa ack gì");

  blockingA.disconnect();
  console.log("consumer-a crash mà không XACK");
  await eventually(
    async () => {
      const [entry] = await consumerB.pendingEntries(group);
      return entry !== undefined && entry.idleMs >= 2 * minIdleMs;
    },
    { timeoutMs: 10_000 },
  );

  const claimed = await consumerB.claimStale(group, "consumer-b", minIdleMs);
  console.log("consumer-b đã claim:", JSON.stringify(claimed));
  await showPending("sau XAUTOCLAIM (chủ sở hữu là consumer-b, deliveryCount 2)");

  for (const message of claimed.messages) await consumerB.ack(group, message.id);
  console.log("số pending sau khi ack:", await consumerB.pendingCount(group));

  const rest = await consumerB.consume(group, "consumer-b", 10);
  console.log("consumer-b còn đọc entry chưa từng được giao:", JSON.stringify(rest));

  console.log("== dạng reply thô (ioredis 6.0.0, RESP3) ==");
  console.log("XPENDING dạng summary:", JSON.stringify(await redis.xpending(stream, group)));
  console.log(
    "XPENDING dạng extended:",
    JSON.stringify(await redis.xpending(stream, group, "-", "+", 10)),
  );
  console.log(
    "XAUTOCLAIM (3 phần tử):",
    JSON.stringify(await redis.xautoclaim(stream, group, "consumer-c", 0, "0-0")),
  );
} finally {
  await redis.del(stream);
  redis.disconnect();
  blockingA.disconnect();
  blockingB.disconnect();
}
