// Demo: a consumer takes a message and crashes without ack, the message sits in its processing
// list, a recovery pass moves it back and another consumer finishes it. Keys are removed at the end.
import { Redis } from "ioredis";
import { uniqueName } from "@handbook/testkit";
import { ReliableQueue } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const redis = new Redis(url);
const crashedBlocking = new Redis(url);
const survivorBlocking = new Redis(url);
const queueKey = uniqueName("demo:list-queue");
const options = { redis, queue: queueKey, blockTimeoutSeconds: 1 };
const crashing = new ReliableQueue({ ...options, blocking: crashedBlocking });
const survivor = new ReliableQueue({ ...options, blocking: survivorBlocking });

async function show(label: string): Promise<void> {
  const [queue, a, b] = await Promise.all([
    redis.lrange(queueKey, 0, -1),
    redis.lrange(survivor.processingKey("worker-a"), 0, -1),
    redis.lrange(survivor.processingKey("worker-b"), 0, -1),
  ]);
  console.log(
    `${label}: queue=${JSON.stringify(queue)} worker-a=${JSON.stringify(a)} worker-b=${JSON.stringify(b)}`,
  );
}

try {
  await survivor.enqueue("job-1");
  await survivor.enqueue("job-2");
  await show("after enqueue");

  console.log("worker-a dequeues:", await crashing.dequeue("worker-a"));
  await show("worker-a holds job-1 (no ack yet)");

  crashedBlocking.disconnect();
  console.log("worker-a crashes without ack");
  console.log("recoverStale(worker-a) moved:", await survivor.recoverStale("worker-a"));
  await show("after recovery");

  const started = Date.now();
  for (;;) {
    const msg = await survivor.dequeue("worker-b");
    if (msg === null) {
      console.log(
        `worker-b dequeue on an empty queue: null after blocking ${Date.now() - started} ms in total`,
      );
      break;
    }
    console.log(`worker-b dequeues ${msg}, acks:`, await survivor.ack("worker-b", msg));
  }
  await show("end");
} finally {
  await redis.del(queueKey, survivor.processingKey("worker-a"), survivor.processingKey("worker-b"));
  redis.disconnect();
  crashedBlocking.disconnect();
  survivorBlocking.disconnect();
}
