import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { afterAll, afterEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { eventually, uniqueName } from "@handbook/testkit";
import { DlqQueue, type Delivery } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
// Một connection cho lệnh thường. Mỗi consumer có connection riêng cho XREADGROUP BLOCK.
const redis = new Redis(url);
const blockingConnections: Redis[] = [];
const keys: string[] = [];
const children: ChildProcess[] = [];
const BLOCK_MS = 100;
const MIN_IDLE_MS = 200;

interface Names {
  stream: string;
  group: string;
  dlqStream: string;
  marker: string;
}

/** Đăng ký mọi key trước khi dùng để afterEach luôn xóa, kể cả khi test fail giữa chừng. */
function newNames(): Names {
  const stream = uniqueName("lab03-stream");
  const names = {
    stream,
    group: uniqueName("lab03-group"),
    dlqStream: `${stream}:dlq`,
    marker: `${stream}:started`,
  };
  keys.push(names.stream, names.dlqStream, names.marker);
  return names;
}

function newQueue(names: Names, consumer: string): DlqQueue {
  const blocking = new Redis(url);
  blockingConnections.push(blocking);
  return new DlqQueue({
    redis,
    blocking,
    stream: names.stream,
    group: names.group,
    dlqStream: names.dlqStream,
    consumer,
    minIdleMs: MIN_IDLE_MS,
    blockMs: BLOCK_MS,
  });
}

/** Kill mọi child còn sống và chờ chúng thoát hẳn: không để lại process mồ côi dù test fail. */
async function reapChildren(): Promise<void> {
  await Promise.all(
    children.splice(0).map(
      (child) =>
        new Promise<void>((resolve) => {
          if (child.exitCode !== null || child.signalCode !== null) return resolve();
          child.once("exit", () => resolve());
          child.kill("SIGKILL");
        }),
    ),
  );
}

afterEach(async () => {
  try {
    await reapChildren();
  } finally {
    for (const connection of blockingConnections.splice(0)) connection.disconnect();
    // DEL xóa stream cùng với các consumer group và pending list của chúng.
    if (keys.length > 0) await redis.del(...keys.splice(0));
  }
});

afterAll(() => {
  redis.disconnect();
});

describe("lab-03 DLQ: consumer group, XAUTOCLAIM retry, dead-letter stream", () => {
  it("poison_message_moves_to_dlq_after_max_attempts_with_error_metadata", async () => {
    const names = newNames();
    const queue = newQueue(names, "consumer-1");
    await queue.ensureGroup();
    const id = await queue.publish({ order: "42", note: "poison" });

    const attempts: number[] = [];
    const before = Date.now();
    const stats = await queue.consumeWithDlq({ maxAttempts: 3 }, async (delivery) => {
      attempts.push(delivery.deliveryCount);
      throw new Error("cannot parse order 42");
    });
    const after = Date.now();

    // Handler chạy đúng maxAttempts lần, delivery count (từ XPENDING) tăng 1, 2, 3.
    expect(attempts).toEqual([1, 2, 3]);
    expect(stats).toEqual({ succeeded: 0, failed: 3, deadLettered: 1 });

    const dlq = await queue.dlqEntries();
    expect(dlq).toHaveLength(1);
    const fields = dlq[0]!.fields;
    expect(Object.keys(fields).sort()).toEqual([
      "attempts",
      "error",
      "failed_at",
      "original_id",
      "payload",
    ]);
    expect(fields.original_id).toBe(id);
    expect(fields.error).toBe("cannot parse order 42");
    expect(fields.attempts).toBe("3");
    expect(fields.failed_at).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
    const failedAt = Date.parse(fields.failed_at!);
    expect(failedAt).toBeGreaterThanOrEqual(before - 1000);
    expect(failedAt).toBeLessThanOrEqual(after + 1000);
    expect(JSON.parse(fields.payload!)).toEqual({ order: "42", note: "poison" });

    // Entry gốc đã được XACK: pending list sạch.
    expect(await queue.pendingEntries()).toEqual([]);
  });

  it("handler_that_recovers_before_max_attempts_is_not_dead_lettered", async () => {
    const names = newNames();
    const queue = newQueue(names, "consumer-1");
    await queue.ensureGroup();
    await queue.publish({ order: "7" });

    const attempts: number[] = [];
    const stats = await queue.consumeWithDlq({ maxAttempts: 5 }, async (delivery) => {
      attempts.push(delivery.deliveryCount);
      if (delivery.deliveryCount < 3) throw new Error("lỗi tạm thời");
    });

    expect(attempts).toEqual([1, 2, 3]);
    expect(stats).toEqual({ succeeded: 1, failed: 2, deadLettered: 0 });
    expect(await queue.dlqEntries()).toEqual([]);
    expect(await queue.pendingEntries()).toEqual([]);
  });

  it("max_attempts_one_sends_first_failure_straight_to_dlq", async () => {
    const names = newNames();
    const queue = newQueue(names, "consumer-1");
    await queue.ensureGroup();
    const id = await queue.publish({ order: "1" });

    let calls = 0;
    const stats = await queue.consumeWithDlq({ maxAttempts: 1 }, async () => {
      calls += 1;
      throw new Error("lỗi ngay lần đầu");
    });

    expect(calls).toBe(1);
    expect(stats).toEqual({ succeeded: 0, failed: 1, deadLettered: 1 });
    const [entry] = await queue.dlqEntries();
    expect(entry?.fields).toMatchObject({ original_id: id, attempts: "1" });
    expect(await queue.pendingEntries()).toEqual([]);
  });

  it("entry_delivered_more_than_max_attempts_before_handler_runs_goes_to_dlq", async () => {
    // Consumer chết ở lần giao cuối cùng (không kịp báo lỗi): delivery count vượt maxAttempts,
    // và message phải vào DLQ thay vì làm consumer kế tiếp chết lặp lại mãi.
    const names = newNames();
    const queue = newQueue(names, "consumer-1");
    await queue.ensureGroup();
    const id = await queue.publish({ order: "9" });
    // Lần giao thứ nhất: đọc thẳng bằng XREADGROUP rồi bỏ đó, như một consumer đã chết.
    await redis.xreadgroup("GROUP", names.group, "dead-consumer", "STREAMS", names.stream, ">");

    let calls = 0;
    const stats = await queue.consumeWithDlq({ maxAttempts: 1 }, async () => {
      calls += 1;
    });

    expect(calls).toBe(0);
    expect(stats).toEqual({ succeeded: 0, failed: 0, deadLettered: 1 });
    const [entry] = await queue.dlqEntries();
    expect(entry?.fields).toMatchObject({ original_id: id, attempts: "2" });
    expect(entry?.fields.error).toContain("consumer");
    expect(await queue.pendingEntries()).toEqual([]);
  });

  it("empty_stream_consume_returns_without_error", async () => {
    const names = newNames();
    const queue = newQueue(names, "consumer-1");
    await queue.ensureGroup();

    let calls = 0;
    const stats = await queue.consumeWithDlq({ maxAttempts: 3 }, async () => {
      calls += 1;
    });

    expect(calls).toBe(0);
    expect(stats).toEqual({ succeeded: 0, failed: 0, deadLettered: 0 });
    expect(await queue.dlqEntries()).toEqual([]);
  });

  it("max_attempts_must_be_a_positive_integer", async () => {
    const names = newNames();
    const queue = newQueue(names, "consumer-1");
    for (const maxAttempts of [0, -1, 1.5, Number.NaN]) {
      await expect(queue.consumeWithDlq({ maxAttempts }, async () => {})).rejects.toThrow(
        /maxAttempts/,
      );
    }
  });

  it("replay_dlq_returns_messages_to_main_stream", async () => {
    const names = newNames();
    const queue = newQueue(names, "consumer-1");
    await queue.ensureGroup();
    // Replay một DLQ rỗng: 0, không lỗi.
    expect(await queue.replayDlq(5)).toBe(0);

    const originals = [{ order: "1" }, { order: "2" }, { order: "3" }];
    for (const fields of originals) await queue.publish(fields);
    // maxAttempts 1: mỗi message lỗi một lần là vào thẳng DLQ.
    const failed = await queue.consumeWithDlq({ maxAttempts: 1 }, async () => {
      throw new Error("lỗi do bug đã được sửa sau đó");
    });
    expect(failed.deadLettered).toBe(3);
    expect(await queue.dlqEntries()).toHaveLength(3);

    // limit nhỏ hơn kích thước DLQ: chỉ chuyển đúng limit entry, cũ nhất trước.
    expect(await queue.replayDlq(2)).toBe(2);
    expect(await queue.dlqEntries()).toHaveLength(1);
    // limit lớn hơn kích thước DLQ: trả số entry thực sự đã chuyển.
    expect(await queue.replayDlq(100)).toBe(1);
    expect(await queue.dlqEntries()).toEqual([]);
    // DLQ đã rỗng.
    expect(await queue.replayDlq(100)).toBe(0);

    // Main stream nhận lại đúng field gốc (3 entry gốc, rồi 3 entry replay theo cùng thứ tự).
    const main = await redis.xrange(names.stream, "-", "+");
    const mainFields = main.map(([, flat]) => Object.fromEntries(pairs(flat)));
    expect(mainFields).toEqual([...originals, ...originals]);

    // Bug đã sửa: consumer xử lý thành công các message vừa replay.
    const seen: Record<string, string>[] = [];
    const ok = await queue.consumeWithDlq({ maxAttempts: 3 }, async (delivery) => {
      seen.push(delivery.fields);
    });
    expect(ok).toEqual({ succeeded: 3, failed: 0, deadLettered: 0 });
    expect(seen).toEqual(originals);
  });

  it("replay_dlq_rejects_a_non_positive_limit", async () => {
    const names = newNames();
    const queue = newQueue(names, "consumer-1");
    for (const limit of [0, -1, 1.5]) {
      await expect(queue.replayDlq(limit)).rejects.toThrow(/limit/);
    }
  });
});

describe("chaos", () => {
  it("chaos_consumer_killed_mid_processing_message_is_reclaimed", async () => {
    const names = newNames();
    const parent = newQueue(names, "rescuer");
    await parent.ensureGroup();
    const id = await parent.publish({ job: "send-invoice" });

    // Child process: một consumer thật đọc message, báo "đã bắt đầu xử lý" qua marker key rồi treo ở handler.
    const workerPath = fileURLToPath(new URL("./chaos-worker.ts", import.meta.url));
    const repoRoot = fileURLToPath(new URL("../../../", import.meta.url));
    const childConsumer = uniqueName("victim");
    const child = spawn(process.execPath, ["--import", "tsx", workerPath], {
      cwd: repoRoot,
      env: {
        ...process.env,
        REDIS_URL: url,
        STREAM: names.stream,
        GROUP: names.group,
        DLQ_STREAM: names.dlqStream,
        CONSUMER: childConsumer,
        MARKER_KEY: names.marker,
        MIN_IDLE_MS: String(MIN_IDLE_MS),
      },
      stdio: ["ignore", "ignore", "pipe"],
    });
    children.push(child);
    let childStderr = "";
    child.stderr?.on("data", (chunk: Buffer) => (childStderr += chunk.toString()));
    const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) =>
      child.once("exit", (code, signal) => resolve({ code, signal })),
    );

    try {
      // Chờ tới khi child báo đã nhận message (không sleep, không đoán thời gian khởi động).
      const startedId = await eventually(async () => (await redis.get(names.marker)) ?? undefined, {
        timeoutMs: 30_000,
      });
      expect(startedId).toBe(id);
      const [pending] = await parent.pendingEntries();
      expect(pending).toMatchObject({ id, consumer: childConsumer, deliveryCount: 1 });

      // SIGKILL: process chết ngay giữa handler, không kịp XACK hay dọn gì.
      child.kill("SIGKILL");
      const result = await exited;
      expect(result.signal).toBe("SIGKILL");
    } catch (error) {
      throw new Error(`${String(error)}\nstderr của child:\n${childStderr}`, { cause: error });
    }

    // Message vẫn nằm trong PEL của consumer đã chết. Chờ tới khi idle gấp đôi minIdle,
    // để lần claim cách xa ranh giới min-idle-time.
    await eventually(
      async () => {
        const [entry] = await parent.pendingEntries();
        return entry !== undefined && entry.idleMs >= 2 * MIN_IDLE_MS;
      },
      { timeoutMs: 10_000 },
    );

    const handled: Delivery[] = [];
    const stats = await parent.consumeWithDlq({ maxAttempts: 3 }, async (delivery) => {
      handled.push(delivery);
    });

    // Message được consumer thứ hai tiếp quản (lần giao thứ hai) và xử lý thành công.
    expect(handled).toEqual([{ id, fields: { job: "send-invoice" }, deliveryCount: 2 }]);
    expect(stats).toEqual({ succeeded: 1, failed: 0, deadLettered: 0 });
    expect(await parent.pendingEntries()).toEqual([]);
    expect(await parent.dlqEntries()).toEqual([]);
  });
});

function pairs(flat: string[]): [string, string][] {
  const out: [string, string][] = [];
  for (let i = 0; i + 1 < flat.length; i += 2) out.push([flat[i]!, flat[i + 1]!]);
  return out;
}
