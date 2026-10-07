import http from "node:http";
import type { AddressInfo } from "node:net";
import { Queue, Worker, type Job } from "bullmq";
import { Redis } from "ioredis";
import { Counter, Registry } from "@prometheus-io/client";

/** Nội dung một email cần gửi. Lab không gửi email thật: việc gửi nằm trong `EmailSender`. */
export interface EmailJob {
  to: string;
  subject: string;
}

export interface SendContext {
  /** Id của job trong queue. */
  jobId: string;
  /** Lần chạy thứ mấy của job này (bắt đầu từ 1). Chạy lại sau khi worker chết cũng tăng số này. */
  attempt: number;
}

/** Hàm gửi email có thể thay thế: test cho nó thành công, lỗi N lần hoặc bị giữ lại bởi một gate. */
export type EmailSender = (email: EmailJob, context: SendContext) => Promise<void>;

export interface JobSystemOptions {
  /** Địa chỉ Redis (REDIS_URL). */
  redisUrl: string;
  /** Tên queue. Mọi key của BullMQ nằm dưới `bull:<queueName>:`. */
  queueName: string;
  sender: EmailSender;
  /** Số job một worker chạy song song. Mặc định 1. */
  concurrency?: number;
  /** Tổng số lần chạy tối đa của một job (lần đầu cộng các lần retry). Mặc định 3. */
  attempts?: number;
  /** Base của exponential backoff, mili giây. Chờ `base * 2^(attemptsMade - 1)` sau lần fail thứ attemptsMade. Mặc định 1000. */
  backoffDelayMs?: number;
  /** Thời gian lock job của worker (mili giây). Worker chết thì job thành "stalled" sau khi lock hết hạn. Mặc định của BullMQ là 30000. */
  lockDurationMs?: number;
  /** Chu kỳ stalled checker (mili giây). Mặc định của BullMQ là 30000. */
  stalledIntervalMs?: number;
  /** Nhận lỗi của worker (mất kết nối, mất lock...). Mặc định in ra stderr. */
  onError?: (error: Error) => void;
}

export interface EnqueueOptions {
  /** Hoãn job: nó nằm trong delayed set tới khi hết `delayMs`. */
  delayMs?: number;
  /** Ghi đè số lần chạy tối đa của riêng job này. */
  attempts?: number;
}

export interface JobSystem {
  /** Queue của BullMQ, để kiểm tra trạng thái job (getJob, getFailed, ...). */
  readonly queue: Queue;
  /** URL của endpoint `GET /metrics` (cổng do hệ điều hành chọn). */
  readonly metricsUrl: string;
  /** Đưa một email vào queue, trả về id của job. */
  enqueueEmail(to: string, subject: string, options?: EnqueueOptions): Promise<string>;
  /**
   * Tắt worker êm: ngừng nhận job mới, chờ job đang chạy xong tối đa `timeoutMs`, rồi đóng
   * worker, metrics endpoint, queue và các connection. Hết `timeoutMs` mà job chưa xong thì đóng
   * worker kiểu force: job đó vẫn ở trạng thái active cho tới khi lock hết hạn, rồi một worker khác
   * nhặt lại (stalled) và chạy lại từ đầu. Không có job nào đang chạy thì resolve ngay. Gọi nhiều lần an toàn.
   */
  shutdown(timeoutMs: number): Promise<void>;
}

/** Retention được đặt có chủ đích: mặc định BullMQ giữ completed và failed job vô thời hạn trong Redis. */
const KEEP_COMPLETED = 100;
const KEEP_FAILED = 1000;

/**
 * Nối queue, worker, metrics và shutdown thành một hệ thống job nhỏ.
 *
 * Metric `jobs_processed_total{status}` đếm kết quả của TỪNG LẦN chạy sender: `completed` là số lần
 * sender thành công, `failed` là số lần sender lỗi (kể cả lần lỗi sẽ được retry). Job lỗi hai lần rồi
 * thành công cho failed = 2 và completed = 1. Đếm ở mức sender chứ không ở mức broker, nên nếu worker
 * chết hoặc bị force close trước khi BullMQ ghi nhận kết quả thì job chạy lại và được đếm lại.
 */
export async function createJobSystem(options: JobSystemOptions): Promise<JobSystem> {
  const {
    redisUrl,
    queueName,
    sender,
    concurrency = 1,
    attempts = 3,
    backoffDelayMs = 1000,
    lockDurationMs,
    stalledIntervalMs,
    onError = (error) => console.error("[job-queue] lỗi worker:", error),
  } = options;

  const registry = new Registry();
  const processed = new Counter({
    name: "jobs_processed_total",
    help: "Số lần chạy sender, theo kết quả (mỗi lần thử được đếm một lần).",
    labelNames: ["status"],
    registers: [registry],
  });
  // Khởi tạo cả hai nhãn để endpoint luôn in ra cả completed và failed, kể cả khi còn bằng 0.
  processed.inc({ status: "completed" }, 0);
  processed.inc({ status: "failed" }, 0);

  // Connection cho lệnh thường. BullMQ tự nhân bản connection chuyên cho lệnh blocking của Worker.
  const queueConnection = new Redis(redisUrl);
  // Worker bắt buộc maxRetriesPerRequest: null: BullMQ ném lỗi nếu giá trị mặc định (20) được truyền vào,
  // vì lệnh blocking không được tự fail sau vài lần thử lại.
  const workerConnection = new Redis(redisUrl, { maxRetriesPerRequest: null });
  const queue = new Queue<EmailJob>(queueName, {
    connection: queueConnection,
    defaultJobOptions: {
      attempts,
      backoff: { type: "exponential", delay: backoffDelayMs },
      removeOnComplete: { count: KEEP_COMPLETED },
      removeOnFail: { count: KEEP_FAILED },
    },
  });
  queue.on("error", onError);

  let inFlight = 0;
  let drained: (() => void) | undefined;
  const processor = async (job: Job<EmailJob>): Promise<void> => {
    inFlight += 1;
    try {
      await sender(job.data, { jobId: String(job.id), attempt: job.attemptsStarted });
      processed.inc({ status: "completed" });
    } catch (error) {
      processed.inc({ status: "failed" });
      // Sender có thể ném giá trị không phải Error (chuỗi, số...): chuẩn hóa để BullMQ luôn có failedReason.
      throw error instanceof Error ? error : new Error(String(error));
    } finally {
      inFlight -= 1;
      if (inFlight === 0) drained?.();
    }
  };
  const worker = new Worker<EmailJob>(queueName, processor, {
    connection: workerConnection,
    concurrency,
    ...(lockDurationMs === undefined ? {} : { lockDuration: lockDurationMs }),
    ...(stalledIntervalMs === undefined ? {} : { stalledInterval: stalledIntervalMs }),
  });
  worker.on("error", onError);

  const server = http.createServer((request, response) => {
    if (request.method === "GET" && request.url === "/metrics") {
      registry.metrics().then(
        (body) => {
          response.writeHead(200, { "Content-Type": registry.contentType });
          response.end(body);
        },
        (error: unknown) => {
          response.writeHead(500).end(String(error));
        },
      );
      return;
    }
    response.writeHead(404).end();
  });
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    await worker.waitUntilReady();
  } catch (error) {
    await Promise.allSettled([worker.close(true), queue.close()]);
    server.close();
    queueConnection.disconnect();
    workerConnection.disconnect();
    throw error;
  }
  const metricsUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}/metrics`;

  /** Chờ tới khi không còn sender nào đang chạy, hoặc hết `timeoutMs`. Trả true nếu đã hết job. */
  const waitForDrain = (timeoutMs: number): Promise<boolean> => {
    if (inFlight === 0) return Promise.resolve(true);
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        drained = undefined;
        resolve(false);
      }, timeoutMs);
      drained = () => {
        clearTimeout(timer);
        drained = undefined;
        resolve(true);
      };
    });
  };

  const doShutdown = async (timeoutMs: number): Promise<void> => {
    // Mỗi bước chạy dù bước trước lỗi; lỗi đầu tiên được ném lại ở cuối.
    const errors: unknown[] = [];
    const step = async (fn: () => Promise<unknown>): Promise<void> => {
      try {
        await fn();
      } catch (error) {
        errors.push(error);
      }
    };
    let drainedInTime = true;
    // worker.close() không có timeout và không thể nâng lên force sau khi đã gọi (BullMQ lưu lại
    // promise đang đóng). Vì vậy ngừng nhận job trước, tự chờ tới hạn, rồi mới chọn close thường hay force.
    await step(() => worker.pause(true));
    await step(async () => {
      drainedInTime = await waitForDrain(timeoutMs);
    });
    await step(() => worker.close(!drainedInTime));
    await step(
      () =>
        new Promise<void>((resolve, reject) => {
          server.close((error) => (error ? reject(error) : resolve()));
          server.closeAllConnections();
        }),
    );
    await step(() => queue.close());
    queueConnection.disconnect();
    workerConnection.disconnect();
    if (errors.length > 0) throw errors[0];
  };
  let closing: Promise<void> | undefined;

  return {
    queue,
    metricsUrl,
    async enqueueEmail(to, subject, enqueueOptions = {}) {
      const job = await queue.add(
        "email",
        { to, subject },
        {
          ...(enqueueOptions.delayMs === undefined ? {} : { delay: enqueueOptions.delayMs }),
          ...(enqueueOptions.attempts === undefined ? {} : { attempts: enqueueOptions.attempts }),
        },
      );
      if (job.id === undefined) throw new Error("BullMQ không trả về id cho job");
      return job.id;
    },
    shutdown(timeoutMs) {
      closing ??= doShutdown(timeoutMs);
      return closing;
    },
  };
}

/**
 * Xóa mọi key của một queue (`queue.obliterate({ force: true })`), dùng khi dọn test và demo.
 * Gọi sau khi mọi worker của queue đã đóng.
 */
export async function destroyQueue(redisUrl: string, queueName: string): Promise<void> {
  const connection = new Redis(redisUrl);
  const queue = new Queue(queueName, { connection });
  try {
    await queue.obliterate({ force: true });
  } finally {
    await queue.close();
    connection.disconnect();
  }
}
