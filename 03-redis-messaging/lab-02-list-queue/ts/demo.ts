// Demo: một consumer lấy message rồi crash mà không ack, message nằm lại trong processing list của nó,
// một lượt recovery chuyển nó về queue và consumer khác xử lý xong. Các key bị xóa ở cuối.
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
  await show("sau enqueue");

  console.log("worker-a dequeue được:", await crashing.dequeue("worker-a"));
  await show("worker-a đang giữ job-1 (chưa ack)");

  crashedBlocking.disconnect();
  console.log("worker-a crash mà không ack");
  console.log("recoverStale(worker-a) đã chuyển:", await survivor.recoverStale("worker-a"));
  await show("sau recovery");

  const started = Date.now();
  for (;;) {
    const msg = await survivor.dequeue("worker-b");
    if (msg === null) {
      console.log(
        `worker-b dequeue trên queue rỗng: null sau khi chặn tổng cộng ${Date.now() - started} ms`,
      );
      break;
    }
    console.log(`worker-b dequeue ${msg}, ack:`, await survivor.ack("worker-b", msg));
  }
  await show("kết thúc");
} finally {
  await redis.del(queueKey, survivor.processingKey("worker-a"), survivor.processingKey("worker-b"));
  redis.disconnect();
  crashedBlocking.disconnect();
  survivorBlocking.disconnect();
}
