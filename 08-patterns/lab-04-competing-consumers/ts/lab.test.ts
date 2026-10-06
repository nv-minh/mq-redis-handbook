import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { afterAll, afterEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { eventually, uniqueName } from "@handbook/testkit";
import { WorkerGroup, type Delivery, type WorkerHandle } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const redis = new Redis(url);
const keys: string[] = [];
const handles: WorkerHandle[] = [];
const children: ChildProcess[] = [];
const BLOCK_MS = 100;

interface Names {
  stream: string;
  group: string;
  marker: string;
}

/** Đăng ký mọi key trước khi dùng để afterEach luôn xóa, kể cả khi test fail giữa chừng. */
function newNames(): Names {
  const stream = uniqueName("lab04-stream");
  keys.push(stream, `${stream}:started`);
  return { stream, group: uniqueName("lab04-group"), marker: `${stream}:started` };
}

function newGroup(names: Names, minIdleMs: number): WorkerGroup {
  return new WorkerGroup({
    redis,
    stream: names.stream,
    group: names.group,
    minIdleMs,
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
    // allSettled: một stop() lỗi không được làm các worker còn lại bị bỏ sót. Lỗi đầu tiên vẫn được ném ra.
    const results = await Promise.allSettled(handles.splice(0).map((handle) => handle.stop()));
    const failed = results.find((r) => r.status === "rejected");
    if (failed) throw (failed as PromiseRejectedResult).reason;
  } finally {
    try {
      await reapChildren();
    } finally {
      // DEL xóa stream cùng với các consumer group và pending list của chúng.
      if (keys.length > 0) await redis.del(...keys.splice(0));
    }
  }
});

afterAll(() => {
  redis.disconnect();
});

const pendingCount = async (group: WorkerGroup) => (await group.pendingEntries()).length;

describe("lab-04 competing consumers: Redis Streams consumer group", () => {
  it("four_workers_process_each_message_exactly_once_without_failures", async () => {
    const names = newNames();
    const group = newGroup(names, 10_000); // minIdle lớn: không có lần claim nào xảy ra khi không ai lỗi
    await group.ensureGroup();
    const total = 40;
    const published: string[] = [];
    for (let i = 0; i < total; i++) published.push(await group.publish({ n: String(i) }));

    const counts = new Map<string, number>();
    const consumers = new Set<string>();
    const handle = group.startWorkers(4, async (delivery) => {
      counts.set(delivery.id, (counts.get(delivery.id) ?? 0) + 1);
      consumers.add(delivery.consumer);
    });
    handles.push(handle);
    expect(new Set(handle.consumerNames).size).toBe(4);

    await eventually(async () => counts.size === total && (await pendingCount(group)) === 0, {
      timeoutMs: 20_000,
    });
    await handle.stop();

    // Mỗi id được xử lý đúng một lần, không có id nào thiếu hay thừa. Không assert cách chia việc giữa các worker.
    expect([...counts.keys()].sort(compareIds)).toEqual(published);
    expect([...counts.values()].every((count) => count === 1)).toBe(true);
    for (const consumer of consumers) expect(handle.consumerNames).toContain(consumer);
    expect(await pendingCount(group)).toBe(0);
  });

  it("failed_handler_leaves_message_pending_until_reclaimed", async () => {
    const names = newNames();
    const group = newGroup(names, 300);
    await group.ensureGroup();
    const id = await group.publish({ job: "flaky" });

    const attempts: number[] = [];
    const ids: string[] = [];
    const handle = group.startWorkers(2, async (delivery) => {
      attempts.push(delivery.deliveryCount);
      ids.push(delivery.id);
      if (delivery.deliveryCount === 1) throw new Error("lỗi tạm thời ở lần giao đầu");
    });
    handles.push(handle);

    // Lần đầu lỗi nên không ack, message nằm lại PEL, một worker claim lại sau minIdle và lần hai thành công.
    await eventually(async () => attempts.length >= 2 && (await pendingCount(group)) === 0, {
      timeoutMs: 20_000,
    });
    await handle.stop();
    expect(attempts).toEqual([1, 2]);
    expect(ids).toEqual([id, id]);
  });

  it("stop_waits_for_in_flight_handler_and_acks_it", async () => {
    const names = newNames();
    const group = newGroup(names, 10_000);
    await group.ensureGroup();
    await group.publish({ job: "slow" });

    let started = false;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const handle = group.startWorkers(1, async () => {
      started = true;
      await gate;
    });
    handles.push(handle);
    await eventually(async () => started, { timeoutMs: 10_000 });

    let stopped = false;
    const stopping = handle.stop().then(() => (stopped = true));
    // Handler còn đang chạy nên stop() chưa được xong: sau một round trip tới Redis nó vẫn chưa resolve.
    expect(await pendingCount(group)).toBe(1);
    expect(stopped).toBe(false);

    release();
    await stopping;
    // Sau stop(): handler đã xong và message đã được ack.
    expect(await pendingCount(group)).toBe(0);
  });

  it("start_workers_rejects_a_non_positive_worker_count", () => {
    const group = newGroup(newNames(), 1000);
    for (const n of [0, -1, 1.5]) {
      expect(() => group.startWorkers(n, async () => {})).toThrow(/n/);
    }
  });
});

describe("chaos", () => {
  it("chaos_worker_killed_mid_processing_message_is_processed_by_another", async () => {
    const names = newNames();
    const minIdleMs = 300;
    const group = newGroup(names, minIdleMs);
    await group.ensureGroup();
    const id = await group.publish({ job: "send-invoice" });

    // Child process: một worker thật đọc message, báo "đã bắt đầu xử lý" qua marker key rồi treo ở handler.
    const workerPath = fileURLToPath(new URL("./chaos-worker.ts", import.meta.url));
    const repoRoot = fileURLToPath(new URL("../../../", import.meta.url));
    const child = spawn(process.execPath, ["--import", "tsx", workerPath], {
      cwd: repoRoot,
      env: {
        ...process.env,
        REDIS_URL: url,
        STREAM: names.stream,
        GROUP: names.group,
        MARKER_KEY: names.marker,
        MIN_IDLE_MS: String(minIdleMs),
      },
      stdio: ["ignore", "ignore", "pipe"],
    });
    children.push(child);
    let childStderr = "";
    child.stderr?.on("data", (chunk: Buffer) => (childStderr += chunk.toString()));
    const exited = new Promise<{ signal: NodeJS.Signals | null }>((resolve) =>
      child.once("exit", (_code, signal) => resolve({ signal })),
    );

    let victim: string;
    try {
      // Chờ tới khi child báo đã nhận message (không sleep, không đoán thời gian khởi động).
      const startedId = await eventually(async () => (await redis.get(names.marker)) ?? undefined, {
        timeoutMs: 30_000,
      });
      expect(startedId).toBe(id);
      const [pending] = await group.pendingEntries();
      expect(pending).toMatchObject({ id, deliveryCount: 1 });
      victim = pending!.consumer;

      // SIGKILL: process chết ngay giữa handler, không kịp XACK hay dọn gì.
      child.kill("SIGKILL");
      expect((await exited).signal).toBe("SIGKILL");
    } catch (error) {
      throw new Error(`${String(error)}\nstderr của child:\n${childStderr}`, { cause: error });
    }

    // Chờ tới khi idle gấp đôi minIdle, để lần claim cách xa ranh giới min-idle-time.
    await eventually(
      async () => {
        const [entry] = await group.pendingEntries();
        return entry !== undefined && entry.idleMs >= 2 * minIdleMs;
      },
      { timeoutMs: 10_000 },
    );

    // Hai worker trong process này cạnh tranh nhau: một trong hai claim message của worker đã chết.
    const handled: Delivery[] = [];
    const handle = group.startWorkers(2, async (delivery) => {
      handled.push(delivery);
    });
    handles.push(handle);
    await eventually(async () => handled.length >= 1 && (await pendingCount(group)) === 0, {
      timeoutMs: 20_000,
    });
    await handle.stop();

    // At-least-once: message được xử lý ít nhất một lần và đã ack (PEL cạn). Không assert "đúng một lần".
    const delivery = handled.find((d) => d.id === id);
    expect(delivery).toBeDefined();
    expect(delivery!.deliveryCount).toBeGreaterThanOrEqual(2);
    expect(delivery!.consumer).not.toBe(victim);
    expect(delivery!.fields).toEqual({ job: "send-invoice" });
    expect(await pendingCount(group)).toBe(0);
  });
});

/** So sánh id stream `<ms>-<seq>` theo số. */
function compareIds(a: string, b: string): number {
  const [aMs, aSeq] = a.split("-").map(Number) as [number, number];
  const [bMs, bSeq] = b.split("-").map(Number) as [number, number];
  return aMs - bMs || aSeq - bSeq;
}
