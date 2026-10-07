// Demo job queue gửi email với BullMQ: retry với exponential backoff, job hết attempts nằm trong failed set
// rồi được operator replay, và graceful shutdown chờ job đang chạy. Queue bị xóa ở cuối.
import { setTimeout as sleep } from "node:timers/promises";
import { eventually, uniqueName } from "@handbook/testkit";
import { createJobSystem, destroyQueue, type JobSystem } from "./lab.js";

const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const queueName = uniqueName("demo-email");
const startedAt = Date.now();
const at = () => `+${String(Date.now() - startedAt).padStart(5)} ms`;

async function metrics(system: JobSystem): Promise<string> {
  const text = await (await fetch(system.metricsUrl)).text();
  return text
    .split("\n")
    .filter((line) => line.startsWith("jobs_processed_total"))
    .join("  |  ");
}

let smtpHealthy = false;
let failuresLeft = 2;
let releaseSlow: (() => void) | undefined;
let slowStarted = false;
const systems: JobSystem[] = [];
let cleanupFailure: unknown;

try {
  const system = await createJobSystem({
    redisUrl,
    queueName,
    attempts: 3,
    backoffDelayMs: 300,
    sender: async (email, context) => {
      console.log(
        `${at()}  [sender] lần ${context.attempt} gửi "${email.subject}" tới ${email.to}`,
      );
      if (email.subject === "Email chậm") {
        slowStarted = true;
        await new Promise<void>((resolve) => (releaseSlow = resolve));
        console.log(`${at()}  [sender] email chậm đã gửi xong`);
        return;
      }
      if (email.subject === "Hóa đơn tháng 10" && !smtpHealthy)
        throw new Error("SMTP từ chối: 421 try again");
      if (email.subject === "Chào mừng" && failuresLeft > 0) {
        failuresLeft -= 1;
        throw new Error("SMTP timeout");
      }
    },
  });
  systems.push(system);
  console.log(`Metrics endpoint: ${system.metricsUrl}\n`);

  console.log(
    "== 1. Sender lỗi hai lần rồi thành công: retry với exponential backoff (300, 600 ms) ==",
  );
  const welcomeId = await system.enqueueEmail("an@example.com", "Chào mừng");
  await eventually(
    async () =>
      (await (await system.queue.getJob(welcomeId))?.getState()) === "completed" ? true : undefined,
    { timeoutMs: 15_000 },
  );
  const welcome = await system.queue.getJob(welcomeId);
  console.log(`job ${welcomeId}: completed sau ${welcome?.attemptsMade} lần chạy`);
  console.log(`metrics: ${await metrics(system)}`);
  console.log("failed đếm từng lần thử lỗi (2), completed đếm lần thành công (1).\n");

  console.log(
    "== 2. Sender luôn lỗi: hết 3 attempts, job nằm trong failed set, operator replay sau khi sửa SMTP ==",
  );
  const invoiceId = await system.enqueueEmail("binh@example.com", "Hóa đơn tháng 10");
  await eventually(async () => ((await system.queue.getFailedCount()) > 0 ? true : undefined), {
    timeoutMs: 15_000,
  });
  const [dead] = await system.queue.getFailed();
  console.log(
    `job ${invoiceId}: failed, attemptsMade=${dead?.attemptsMade}, lý do: "${dead?.failedReason}"`,
  );
  console.log(
    `failed set: ${await system.queue.getFailedCount()} job. Job vẫn còn trong Redis để điều tra.`,
  );
  smtpHealthy = true;
  console.log("-- operator sửa SMTP rồi replay job (job.retry, đặt lại bộ đếm attempts) --");
  await dead?.retry("failed", { resetAttemptsMade: true, resetAttemptsStarted: true });
  await eventually(
    async () =>
      (await (await system.queue.getJob(invoiceId))?.getState()) === "completed" ? true : undefined,
    { timeoutMs: 15_000 },
  );
  console.log(
    `job ${invoiceId}: completed sau khi replay. failed set còn ${await system.queue.getFailedCount()} job.`,
  );
  console.log(`metrics: ${await metrics(system)}\n`);

  console.log("== 3. Graceful shutdown: shutdown(5000) chờ job đang chạy xong ==");
  const slowId = await system.enqueueEmail("chi@example.com", "Email chậm");
  await eventually(async () => (slowStarted ? true : undefined), { timeoutMs: 10_000 });
  console.log(`${at()}  gọi shutdown(5000) trong lúc email chậm đang chạy`);
  const shutdown = system.shutdown(5000).then(() => console.log(`${at()}  shutdown đã resolve`));
  await sleep(700);
  console.log(`${at()}  sau 700 ms: metrics vẫn truy cập được -> ${await metrics(system)}`);
  releaseSlow?.();
  await shutdown;
  const closed = await fetch(system.metricsUrl).then(
    () => "vẫn mở (lỗi)",
    () => "đã đóng",
  );
  console.log(`metrics endpoint sau shutdown: ${closed}`);
  const reader = await createJobSystem({ redisUrl, queueName, sender: async () => {} });
  systems.push(reader);
  const slowJob = await reader.queue.getJob(slowId);
  console.log(
    `job ${slowId}: ${await slowJob?.getState()}, attemptsMade=${slowJob?.attemptsMade}: không retry, không mất.`,
  );
  console.log(
    "\nGiao hàng là at-least-once: nếu worker chết giữa chừng, job chạy lại, nên sender phải idempotent.",
  );
} finally {
  releaseSlow?.();
  const results = await Promise.allSettled(systems.map((system) => system.shutdown(2000)));
  // destroyQueue chạy sau khi mọi worker đã đóng.
  const cleanup = await destroyQueue(redisUrl, queueName).then(
    () => undefined,
    (error: unknown) => ({ error }),
  );
  const failed = results.find((result) => result.status === "rejected") as
    PromiseRejectedResult | undefined;
  cleanupFailure = failed ? failed.reason : cleanup?.error;
}
// Lỗi dọn dẹp chỉ được báo khi phần chính đã chạy xong (không che lỗi của phần chính).
if (cleanupFailure) throw cleanupFailure;
