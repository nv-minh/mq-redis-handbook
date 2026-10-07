import { afterEach, afterAll, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { eventually, uniqueName } from "@handbook/testkit";
import {
  createJobSystem,
  destroyQueue,
  type EmailSender,
  type JobSystem,
  type JobSystemOptions,
} from "./lab.js";

const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const redis = new Redis(redisUrl);

// Mọi resource được đăng ký TRƯỚC khi dùng, để afterEach dọn dù test fail giữa chừng.
const systems: JobSystem[] = [];
const queueNames = new Set<string>();
const gates: Gate[] = [];

interface Gate {
  /** Sender gọi wait() để bị giữ lại cho tới khi test gọi release(). */
  wait(): Promise<void>;
  release(): void;
}

function makeGate(): Gate {
  let release!: () => void;
  const opened = new Promise<void>((resolve) => (release = resolve));
  const gate: Gate = { wait: () => opened, release };
  gates.push(gate);
  return gate;
}

async function newSystem(
  sender: EmailSender,
  options: Partial<Omit<JobSystemOptions, "sender">> = {},
): Promise<JobSystem> {
  const queueName = options.queueName ?? uniqueName("job-queue");
  queueNames.add(queueName);
  const system = await createJobSystem({
    redisUrl,
    queueName,
    sender,
    onError: () => {},
    ...options,
  });
  systems.push(system);
  return system;
}

async function scanKeys(pattern: string): Promise<string[]> {
  const found: string[] = [];
  let cursor = "0";
  do {
    const [next, keys] = await redis.scan(cursor, "MATCH", pattern, "COUNT", 200);
    found.push(...keys);
    cursor = next;
  } while (cursor !== "0");
  return found;
}

/** Chạy mọi bước dọn dẹp dù một bước ném lỗi, rồi ném lỗi đầu tiên. */
async function runAll(steps: (() => Promise<unknown>)[]): Promise<void> {
  const results = await Promise.allSettled(
    steps.map(async (step) => {
      await step();
    }),
  );
  const failed = results.find((r) => r.status === "rejected");
  if (failed) throw (failed as PromiseRejectedResult).reason;
}

afterEach(async () => {
  // Mở mọi gate để shutdown không bị treo bởi một sender còn bị giữ.
  for (const gate of gates.splice(0)) gate.release();
  const toClose = systems.splice(0);
  const names = [...queueNames];
  queueNames.clear();
  await runAll([
    () => Promise.all(toClose.map((system) => system.shutdown(2000))),
    // Sau khi worker đã đóng: obliterate rồi kiểm tra không còn key nào của queue.
    async () => {
      await runAll(names.map((name) => () => destroyQueue(redisUrl, name)));
      for (const name of names) {
        expect(await scanKeys(`bull:${name}:*`), `key còn sót của queue ${name}`).toEqual([]);
      }
    },
  ]);
});

afterAll(() => {
  redis.disconnect();
});

async function fetchMetrics(system: JobSystem): Promise<string> {
  const response = await fetch(system.metricsUrl);
  expect(response.status).toBe(200);
  return response.text();
}

function metricValue(text: string, status: "completed" | "failed"): number | undefined {
  const match = new RegExp(`^jobs_processed_total\\{status="${status}"\\} (\\d+)$`, "m").exec(text);
  return match ? Number(match[1]) : undefined;
}

describe("usecase-01 job queue (BullMQ): retry, failed set, delay, graceful shutdown, metrics", () => {
  it("failed_job_is_retried_with_exponential_backoff_up_to_attempts", async () => {
    const base = 200;
    const calls: number[] = [];
    const system = await newSystem(
      async (_email, context) => {
        calls.push(Date.now());
        if (context.attempt <= 3) throw new Error(`smtp down (lần ${context.attempt})`);
      },
      { attempts: 4, backoffDelayMs: base },
    );

    const id = await system.enqueueEmail("an@example.com", "Chào mừng");
    await eventually(
      async () => {
        const job = await system.queue.getJob(id);
        return (await job?.getState()) === "completed" ? job : undefined;
      },
      { timeoutMs: 8000 },
    );

    // Sender fail 3 lần đầu rồi thành công: đúng 4 lần chạy.
    expect(calls).toHaveLength(4);
    const job = await system.queue.getJob(id);
    expect(job?.attemptsMade).toBe(4);
    // BullMQ exponential: chờ base * 2^(attemptsMade - 1) sau lần fail thứ attemptsMade (200, 400, 800 ms).
    // Chỉ khẳng định cận dưới: thời điểm nhận job còn phụ thuộc độ bận của worker.
    for (let failure = 1; failure <= 3; failure += 1) {
      const gap = calls[failure]! - calls[failure - 1]!;
      expect(gap, `khoảng cách giữa lần ${failure} và lần ${failure + 1}`).toBeGreaterThanOrEqual(
        base * 2 ** (failure - 1),
      );
    }
    // Metric đếm từng lần thử: 3 lần fail và 1 lần completed.
    const text = await fetchMetrics(system);
    expect(metricValue(text, "failed")).toBe(3);
    expect(metricValue(text, "completed")).toBe(1);
  });

  it("job_exceeding_attempts_is_kept_in_failed_set_or_archive", async () => {
    let calls = 0;
    const system = await newSystem(
      async () => {
        calls += 1;
        throw new Error("mailbox full");
      },
      { attempts: 3, backoffDelayMs: 50 },
    );

    const id = await system.enqueueEmail("an@example.com", "Hóa đơn");
    const failed = await eventually(
      async () => {
        const jobs = await system.queue.getFailed();
        return jobs.length > 0 ? jobs : undefined;
      },
      { timeoutMs: 8000 },
    );

    expect(failed).toHaveLength(1);
    expect(failed[0]!.id).toBe(id);
    expect(failed[0]!.failedReason).toBe("mailbox full");
    expect(failed[0]!.attemptsMade).toBe(3);
    expect(calls).toBe(3);
    expect(await failed[0]!.getState()).toBe("failed");
    // Job nằm lại trong failed set (removeOnFail giữ tối đa 1000 job), không bị xóa.
    expect(await system.queue.getFailedCount()).toBe(1);
  });

  it("scheduled_job_runs_not_before_its_delay", async () => {
    const delayMs = 800;
    const calledAt: number[] = [];
    const system = await newSystem(async () => {
      calledAt.push(Date.now());
    });

    const before = Date.now();
    const id = await system.enqueueEmail("an@example.com", "Nhắc lịch", { delayMs });
    // Chưa tới hạn thì job nằm trong delayed set, sender chưa được gọi.
    const job = await system.queue.getJob(id);
    expect(await job?.getState()).toBe("delayed");
    expect(calledAt).toEqual([]);

    await eventually(async () => (calledAt.length > 0 ? true : undefined), { timeoutMs: 8000 });
    expect(calledAt[0]! - before).toBeGreaterThanOrEqual(delayMs);
  });

  it("graceful_shutdown_waits_for_in_flight_job_to_finish", async () => {
    const gate = makeGate();
    const events: string[] = [];
    const system = await newSystem(async () => {
      events.push("sender-start");
      await gate.wait();
      events.push("sender-end");
    });

    const id = await system.enqueueEmail("an@example.com", "Đang gửi");
    await eventually(async () => (events.includes("sender-start") ? true : undefined), {
      timeoutMs: 8000,
    });

    const shutdown = system.shutdown(10_000).then(() => events.push("shutdown-resolved"));
    // Một round trip tới Redis cho shutdown (nếu sai) cơ hội resolve sớm.
    const job = await system.queue.getJob(id);
    expect(await job?.getState()).toBe("active");
    expect(events).toEqual(["sender-start"]);
    // Metrics endpoint vẫn truy cập được trong lúc đang shutdown.
    expect(metricValue(await fetchMetrics(system), "completed")).toBe(0);

    gate.release();
    await shutdown;

    // Sender xong rồi shutdown mới resolve.
    expect(events).toEqual(["sender-start", "sender-end", "shutdown-resolved"]);
    // Job completed, không retry, không fail. Queue đã đóng nên đọc lại bằng connection riêng.
    const reader = await newSystem(async () => {}, { queueName: system.queue.name });
    const done = await reader.queue.getJob(id);
    expect(await done?.getState()).toBe("completed");
    expect(done?.attemptsMade).toBe(1);
    expect(done?.attemptsStarted).toBe(1);
    expect(await reader.queue.getFailedCount()).toBe(0);
    // Endpoint đã đóng sau shutdown.
    await expect(fetch(system.metricsUrl)).rejects.toThrow();
  });

  it("in_flight_job_outlasting_shutdown_timeout_is_recovered_by_another_worker", async () => {
    const gate = makeGate();
    const settled: Promise<void>[] = [];
    const first = await newSystem(
      () => {
        const run = gate.wait();
        settled.push(run);
        return run;
      },
      { lockDurationMs: 1000, stalledIntervalMs: 500 },
    );
    const queueName = first.queue.name;

    const id = await first.enqueueEmail("an@example.com", "Gửi chậm");
    await eventually(async () => (settled.length > 0 ? true : undefined), { timeoutMs: 8000 });

    // Job chạy lâu hơn timeout: shutdown không đợi mãi, đóng worker kiểu force sau 300 ms.
    const started = Date.now();
    await first.shutdown(300);
    expect(Date.now() - started).toBeGreaterThanOrEqual(295);

    // BullMQ không đưa job về waiting: job vẫn "active" cho tới khi lock hết hạn.
    const calls: { jobId: string; attempt: number }[] = [];
    const second = await newSystem(
      async (_email, context) => {
        calls.push({ jobId: context.jobId, attempt: context.attempt });
      },
      { queueName, lockDurationMs: 1000, stalledIntervalMs: 500 },
    );
    expect(await (await second.queue.getJob(id))?.getState()).toBe("active");

    // Stalled checker của worker thứ hai nhặt job lại: không mất job, nhưng sender chạy lần thứ hai.
    await eventually(async () => (calls.length > 0 ? true : undefined), { timeoutMs: 15_000 });
    expect(calls).toEqual([{ jobId: id, attempt: 2 }]);
    const job = await eventually(
      async () => {
        const current = await second.queue.getJob(id);
        return (await current?.getState()) === "completed" ? current : undefined;
      },
      { timeoutMs: 8000 },
    );
    expect(job.stalledCounter).toBe(1);
    expect(job.attemptsStarted).toBe(2);
    expect(await second.queue.getFailedCount()).toBe(0);

    // Sender cũ cuối cùng cũng xong: không làm job đổi trạng thái và không làm test treo.
    gate.release();
    await Promise.all(settled);
    expect(await (await second.queue.getJob(id))?.getState()).toBe("completed");
  });

  it("metrics_endpoint_reports_jobs_processed_total", async () => {
    const system = await newSystem(async (email) => {
      if (email.to.startsWith("bad")) throw new Error("địa chỉ không tồn tại");
    });

    for (const to of ["a@example.com", "b@example.com", "c@example.com"]) {
      await system.enqueueEmail(to, "ok");
    }
    // attempts = 1: mỗi job lỗi chỉ chạy một lần, nên số lần fail bằng số job fail.
    for (const to of ["bad-1@example.com", "bad-2@example.com"]) {
      await system.enqueueEmail(to, "lỗi", { attempts: 1 });
    }

    const text = await eventually(
      async () => {
        const body = await fetchMetrics(system);
        return metricValue(body, "completed") === 3 && metricValue(body, "failed") === 2
          ? body
          : undefined;
      },
      { timeoutMs: 8000 },
    );
    expect(text).toContain("# HELP jobs_processed_total");
    expect(text).toContain("# TYPE jobs_processed_total counter");
    expect(text).toContain('jobs_processed_total{status="completed"} 3');
    expect(text).toContain('jobs_processed_total{status="failed"} 2');
  });

  it("two_workers_do_not_process_the_same_job_twice", async () => {
    const queueName = uniqueName("job-queue");
    const seen: string[] = [];
    const sender: EmailSender = async (_email, context) => {
      seen.push(context.jobId);
    };
    const one = await newSystem(sender, { queueName, concurrency: 3 });
    await newSystem(sender, { queueName, concurrency: 3 });

    const total = 30;
    const ids = new Set<string>();
    for (let i = 0; i < total; i += 1) ids.add(await one.enqueueEmail(`u${i}@example.com`, "bulk"));
    await eventually(async () => (seen.length >= total ? true : undefined), { timeoutMs: 15_000 });

    expect(ids.size).toBe(total);
    expect(new Set(seen).size).toBe(seen.length);
    expect([...seen].sort()).toEqual([...ids].sort());
  });

  it("job_with_single_attempt_fails_straight_to_failed_set", async () => {
    let calls = 0;
    const system = await newSystem(async () => {
      calls += 1;
      throw new Error("từ chối ngay");
    });

    const id = await system.enqueueEmail("an@example.com", "một lần", { attempts: 1 });
    const failed = await eventually(
      async () => {
        const jobs = await system.queue.getFailed();
        return jobs.length > 0 ? jobs : undefined;
      },
      { timeoutMs: 8000 },
    );
    expect(failed[0]!.id).toBe(id);
    expect(failed[0]!.attemptsMade).toBe(1);
    expect(failed[0]!.failedReason).toBe("từ chối ngay");
    expect(calls).toBe(1);
  });

  it("sender_throwing_a_non_error_value_counts_as_failed", async () => {
    const system = await newSystem(async () => {
      throw "không phải Error";
    });

    const id = await system.enqueueEmail("an@example.com", "ném chuỗi", { attempts: 1 });
    const failed = await eventually(
      async () => {
        const jobs = await system.queue.getFailed();
        return jobs.length > 0 ? jobs : undefined;
      },
      { timeoutMs: 8000 },
    );
    expect(failed[0]!.id).toBe(id);
    expect(failed[0]!.failedReason).toBe("không phải Error");
    expect(metricValue(await fetchMetrics(system), "failed")).toBe(1);
  });

  it("shutdown_with_no_job_in_flight_resolves_immediately_and_closes_metrics_endpoint", async () => {
    const system = await newSystem(async () => {});
    expect(metricValue(await fetchMetrics(system), "completed")).toBe(0);

    const started = Date.now();
    await system.shutdown(30_000);
    expect(Date.now() - started).toBeLessThan(3000);
    await expect(fetch(system.metricsUrl)).rejects.toThrow();
    // Gọi lần hai vẫn an toàn.
    await system.shutdown(30_000);
  });
});
