// Demo: consumer A reads two entries and crashes without XACK, the entries stay in its pending
// list (PEL), consumer B takes them over with XAUTOCLAIM and acks. Also prints the raw reply
// shapes ioredis 6.0.0 gets on RESP3. The stream key is removed at the end.
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
  console.log("consumer-a read:", JSON.stringify(read));
  await showPending("after read, nothing acked");

  blockingA.disconnect();
  console.log("consumer-a crashes without XACK");
  await eventually(
    async () => {
      const [entry] = await consumerB.pendingEntries(group);
      return entry !== undefined && entry.idleMs >= 2 * minIdleMs;
    },
    { timeoutMs: 10_000 },
  );

  const claimed = await consumerB.claimStale(group, "consumer-b", minIdleMs);
  console.log("consumer-b claimed:", JSON.stringify(claimed));
  await showPending("after XAUTOCLAIM (owner is consumer-b, deliveryCount 2)");

  for (const message of claimed.messages) await consumerB.ack(group, message.id);
  console.log("pending count after acks:", await consumerB.pendingCount(group));

  const rest = await consumerB.consume(group, "consumer-b", 10);
  console.log("consumer-b also reads the never delivered entry:", JSON.stringify(rest));

  console.log("== raw reply shapes (ioredis 6.0.0, RESP3) ==");
  console.log("XPENDING summary:", JSON.stringify(await redis.xpending(stream, group)));
  console.log(
    "XPENDING extended:",
    JSON.stringify(await redis.xpending(stream, group, "-", "+", 10)),
  );
  console.log(
    "XAUTOCLAIM (3 elements):",
    JSON.stringify(await redis.xautoclaim(stream, group, "consumer-c", 0, "0-0")),
  );
} finally {
  await redis.del(stream);
  redis.disconnect();
  blockingA.disconnect();
  blockingB.disconnect();
}
