// Demo: hai worker (một nhanh, một chậm) cùng đọc một work queue, so sánh prefetch không giới hạn với prefetch 1.
// Sau đó một worker nhận message rồi crash mà không ack, và worker còn lại nhận lại message với cờ redelivered.
import { setTimeout as sleep } from "node:timers/promises";
import { uniqueName } from "@handbook/testkit";
import { declareWorkQueue, openConnection, publishTasks, startWorker } from "./lab.js";

const connection = await openConnection();
const queues: string[] = [];
const total = 12;

async function round(prefetch: number): Promise<void> {
  const queue = uniqueName("demo-work");
  queues.push(queue);
  const publisher = await connection.createConfirmChannel();
  await declareWorkQueue(publisher, queue);
  const counts = { nhanh: 0, chậm: 0 };
  let finished = 0;
  const allDone = new Promise<void>((resolve) => {
    // Mỗi worker xử lý tuần tự từng message (như một worker thật chỉ có một luồng xử lý), nên các message chờ nối đuôi nhau.
    const work = (who: "nhanh" | "chậm", ms: number) => {
      let tail: Promise<void> = Promise.resolve();
      return async () => {
        tail = tail.then(() => sleep(ms));
        await tail;
        counts[who]++;
        if (++finished === total) resolve();
      };
    };
    void (async () => {
      const slow = await connection.createChannel();
      const fast = await connection.createChannel();
      await startWorker(slow, queue, work("chậm", 300), { prefetch });
      await startWorker(fast, queue, work("nhanh", 20), { prefetch });
      await publishTasks(
        publisher,
        queue,
        Array.from({ length: total }, (_, i) => `task-${i}`),
      );
    })();
  });
  const started = Date.now();
  await allDone;
  const label = prefetch === 0 ? "không giới hạn" : String(prefetch);
  console.log(
    `prefetch ${label.padEnd(14)}: worker nhanh xử lý ${String(counts.nhanh).padStart(2)}, worker chậm xử lý ${String(counts.chậm).padStart(2)}, xong sau ${Date.now() - started} ms`,
  );
}

async function crashDemo(): Promise<void> {
  const queue = uniqueName("demo-crash");
  queues.push(queue);
  const crashing = await openConnection();
  const publisher = await connection.createConfirmChannel();
  await declareWorkQueue(publisher, queue);

  const received = new Promise<void>((resolve) => {
    void (async () => {
      await startWorker(
        await crashing.createChannel(),
        queue,
        async (msg) => {
          console.log(
            `worker A nhận ${msg.content.toString()} (redelivered=${msg.fields.redelivered}), chưa ack`,
          );
          resolve();
          await new Promise<void>(() => undefined);
        },
        { prefetch: 1 },
      );
    })();
  });
  await publishTasks(publisher, queue, ["job-1"]);
  await received;
  await crashing.close();
  console.log("worker A crash (connection bị đóng) mà không ack");

  const redelivered = new Promise<void>((resolve) => {
    void (async () => {
      await startWorker(
        await connection.createChannel(),
        queue,
        async (msg) => {
          console.log(
            `worker B nhận ${msg.content.toString()} (redelivered=${msg.fields.redelivered}) và ack`,
          );
          resolve();
        },
        { prefetch: 1 },
      );
    })();
  });
  await redelivered;
}

try {
  console.log(`--- ${total} task, worker nhanh mất 20 ms, worker chậm mất 300 ms ---`);
  await round(0);
  await round(1);
  console.log(
    "Kết luận: prefetch không giới hạn chia đều theo kiểu round-robin mù, prefetch 1 để việc chảy về worker rảnh.",
  );
  console.log("--- worker crash giữa chừng ---");
  await crashDemo();
  console.log(
    "Kết luận: message chưa ack được broker requeue khi connection đóng, nên consumer cần idempotent.",
  );
} finally {
  const channel = await connection.createChannel();
  for (const queue of queues) await channel.deleteQueue(queue);
  await connection.close();
}
