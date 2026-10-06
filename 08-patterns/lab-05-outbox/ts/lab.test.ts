import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import type { Pool } from "pg";
import type { ChannelModel, ConfirmChannel } from "amqplib";
import { eventually, uniqueName } from "@handbook/testkit";
import {
  amqpPublisher,
  createSchema,
  createTables,
  declareQueue,
  dropSchema,
  openConnection,
  openPool,
  placeOrder,
  runRelay,
  startConsumer,
  type OutboxEvent,
} from "./lab.js";

const databaseUrl =
  process.env.DATABASE_URL ?? "postgres://handbook:handbook@127.0.0.1:5432/handbook";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const amqpUrl = process.env.AMQP_URL ?? "amqp://guest:guest@127.0.0.1:5672";

// Mọi resource được đăng ký TRƯỚC khi dùng, để afterEach dọn dù test fail giữa chừng.
const schemas: string[] = [];
const pools: Pool[] = [];
const connections: ChannelModel[] = [];
const queues: string[] = [];
const redisKeys: string[] = [];
const children: ChildProcess[] = [];
const redis = new Redis(redisUrl);

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
  // Thứ tự có chủ đích: child chết trước (nhả lock), rồi pool đóng, rồi mới DROP SCHEMA.
  const errors: unknown[] = [];
  const attempt = async (step: () => Promise<unknown>) => {
    try {
      await step();
    } catch (error) {
      errors.push(error);
    }
  };
  await attempt(reapChildren);
  await attempt(() => runAll(pools.splice(0).map((pool) => () => pool.end())));
  await attempt(async () => {
    const names = queues.splice(0);
    const cleaner = await openConnection();
    try {
      const channel = await cleaner.createChannel();
      for (const queue of names) await channel.deleteQueue(queue);
    } finally {
      await cleaner.close().catch(() => undefined);
    }
  });
  await attempt(() =>
    runAll(connections.splice(0).map((c) => () => c.close().catch(() => undefined))),
  );
  await attempt(() => runAll(schemas.splice(0).map((schema) => () => dropSchema(schema))));
  await attempt(async () => {
    if (redisKeys.length > 0) await redis.del(...redisKeys.splice(0));
  });
  if (errors.length > 0) throw errors[0];
});

/** Mỗi test một schema riêng (tên duy nhất), pool có search_path trỏ vào schema đó. */
async function newPool(): Promise<Pool> {
  const schema = uniqueName("lab05");
  schemas.push(schema);
  await createSchema(schema);
  const pool = openPool(schema);
  pools.push(pool);
  await createTables(pool);
  return pool;
}

interface Broker {
  channel: ConfirmChannel;
  queue: string;
  /** Số lần mỗi event id được giao cho consumer. */
  deliveries: Map<string, number>;
  /** Event id mà effect đã được áp dụng, theo thứ tự. Consumer dedupe theo event id nên mỗi id xuất hiện tối đa một lần. */
  applied: string[];
}

/** Queue quorum duy nhất cộng một consumer ack thủ công, đếm số lần giao của từng event id. */
async function newBroker(): Promise<Broker> {
  const queue = uniqueName("lab05-outbox");
  queues.push(queue);
  const connection = await openConnection();
  connections.push(connection);
  const channel = await connection.createConfirmChannel();
  await declareQueue(channel, queue);
  const deliveries = new Map<string, number>();
  const applied: string[] = [];
  const seen = new Set<string>();
  const consumerChannel = await connection.createChannel();
  await startConsumer(consumerChannel, queue, uniqueName("lab05-consumer"), (event) => {
    deliveries.set(event.eventId, (deliveries.get(event.eventId) ?? 0) + 1);
    // Dedupe theo event id: bản giao lặp được ack nhưng không áp dụng effect lần nữa.
    if (seen.has(event.eventId)) return;
    seen.add(event.eventId);
    applied.push(event.eventId);
  });
  return { channel, queue, deliveries, applied };
}

const count = async (pool: Pool, sql: string): Promise<number> =>
  Number((await pool.query<{ count: string }>(sql)).rows[0]?.count);

const outboxIds = async (pool: Pool): Promise<string[]> =>
  (await pool.query<{ id: string }>("select id::text as id from outbox order by id")).rows.map(
    (row) => row.id,
  );

describe("lab-05 transactional outbox: Postgres và RabbitMQ", () => {
  it("order_and_outbox_row_commit_atomically", async () => {
    const pool = await newPool();
    await placeOrder(pool, { id: "o-1", amount: 42.5 });

    // Hai dòng có cùng xmin (cùng transaction id) và cùng created_at (now() cố định trong một transaction).
    const { rows } = await pool.query(
      `select o.xmin::text = ob.xmin::text as same_transaction,
              o.created_at = ob.created_at as same_timestamp,
              ob.aggregate_id, ob.type, ob.payload, ob.published_at
         from orders o join outbox ob on ob.aggregate_id = o.id`,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      same_transaction: true,
      same_timestamp: true,
      aggregate_id: "o-1",
      type: "OrderPlaced",
      payload: { orderId: "o-1", amount: 42.5 },
      published_at: null,
    });
    expect(await count(pool, "select count(*) from orders")).toBe(1);
    expect(await count(pool, "select count(*) from outbox")).toBe(1);
  });

  it("rollback_leaves_no_outbox_row", async () => {
    const pool = await newPool();
    // Ép lỗi ở câu INSERT thứ hai: câu INSERT order đã chạy xong trong transaction, rồi outbox từ chối dòng này.
    await pool.query(
      "alter table outbox add constraint outbox_no_poison check (aggregate_id <> 'poison')",
    );

    await expect(placeOrder(pool, { id: "poison", amount: 1 })).rejects.toThrow(/outbox_no_poison/);

    expect(await count(pool, "select count(*) from orders")).toBe(0);
    expect(await count(pool, "select count(*) from outbox")).toBe(0);
    // Pool vẫn dùng được sau lỗi (connection đã ROLLBACK và được trả về pool, không kẹt trong transaction).
    await placeOrder(pool, { id: "ok", amount: 2 });
    expect(await count(pool, "select count(*) from orders")).toBe(1);
  });

  it("relay_publishes_each_row_and_marks_it_published", async () => {
    const pool = await newPool();
    const broker = await newBroker();
    for (const id of ["o-1", "o-2", "o-3"]) await placeOrder(pool, { id, amount: 10 });
    const ids = await outboxIds(pool);

    const published = await runRelay(pool, amqpPublisher(broker.channel, broker.queue), {
      batchSize: 10,
    });
    expect(published).toBe(3);

    // Mỗi event tới consumer đúng một lần, đúng thứ tự chèn (một relay duy nhất giữ thứ tự).
    await eventually(async () => broker.applied.length === 3 || undefined, { timeoutMs: 10_000 });
    expect(broker.applied).toEqual(ids);
    expect([...broker.deliveries.values()]).toEqual([1, 1, 1]);
    expect(await count(pool, "select count(*) from outbox where published_at is null")).toBe(0);

    // Lần chạy thứ hai không còn gì để publish.
    expect(
      await runRelay(pool, amqpPublisher(broker.channel, broker.queue), { batchSize: 10 }),
    ).toBe(0);
    expect(broker.deliveries.size).toBe(3);
  });

  it("empty_outbox_relay_returns_zero_without_publishing", async () => {
    const pool = await newPool();
    const seen: OutboxEvent[] = [];
    const published = await runRelay(
      pool,
      async (event) => {
        seen.push(event);
      },
      { batchSize: 5 },
    );
    expect(published).toBe(0);
    expect(seen).toEqual([]);
  });

  it("batch_size_limits_the_rows_published_per_call", async () => {
    const pool = await newPool();
    for (let i = 1; i <= 5; i++) await placeOrder(pool, { id: `o-${i}`, amount: i });
    const ids = await outboxIds(pool);
    const seen: string[] = [];
    const publish = async (event: OutboxEvent) => {
      seen.push(event.id);
    };

    // Backlog 5, batchSize 2: 2 + 2 + 1 + 0, mỗi lần theo thứ tự id.
    const batches = [
      await runRelay(pool, publish, { batchSize: 2 }),
      await runRelay(pool, publish, { batchSize: 2 }),
      await runRelay(pool, publish, { batchSize: 2 }),
      await runRelay(pool, publish, { batchSize: 2 }),
    ];
    expect(batches).toEqual([2, 2, 1, 0]);
    expect(seen).toEqual(ids);
  });

  it("publish_failure_rolls_back_the_whole_batch_and_propagates_the_error", async () => {
    const pool = await newPool();
    for (const id of ["o-1", "o-2", "o-3"]) await placeOrder(pool, { id, amount: 10 });
    const ids = await outboxIds(pool);
    const calls: string[] = [];

    // Event thứ hai lỗi: cả batch nằm trong một transaction nên ROLLBACK, không dòng nào được đánh dấu.
    await expect(
      runRelay(
        pool,
        async (event) => {
          calls.push(event.id);
          if (calls.length === 2) throw new Error("broker từ chối event thứ hai");
        },
        { batchSize: 10 },
      ),
    ).rejects.toThrow(/broker từ chối/);
    expect(calls).toEqual([ids[0], ids[1]]);
    expect(await count(pool, "select count(*) from outbox where published_at is null")).toBe(3);

    // Lần chạy sau publish lại event đầu (đã được publish ở lần lỗi): đó là at-least-once, không phải mất dữ liệu.
    calls.length = 0;
    expect(
      await runRelay(
        pool,
        async (event) => {
          calls.push(event.id);
        },
        { batchSize: 10 },
      ),
    ).toBe(3);
    expect(calls).toEqual(ids);
    expect(await count(pool, "select count(*) from outbox where published_at is null")).toBe(0);
  });

  it("several_relays_with_skip_locked_do_not_preserve_insertion_order", async () => {
    const pool = await newPool();
    for (let i = 1; i <= 4; i++) await placeOrder(pool, { id: `o-${i}`, amount: i });
    const ids = await outboxIds(pool);
    const log: string[] = [];

    // Relay A khóa dòng 1 và 2 rồi đứng chờ ở event đầu. Relay B chạy trọn vẹn trong lúc đó:
    // SKIP LOCKED bỏ qua dòng 1 và 2 nên B publish dòng 3 và 4 trước.
    let started = false;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const relayA = runRelay(
      pool,
      async (event) => {
        if (!started) {
          started = true;
          await gate;
        }
        log.push(event.id);
      },
      { batchSize: 2 },
    );
    try {
      await eventually(async () => started || undefined, { timeoutMs: 10_000 });
      const publishedByB = await runRelay(
        pool,
        async (event) => {
          log.push(event.id);
        },
        { batchSize: 2 },
      );
      expect(publishedByB).toBe(2);
    } finally {
      release();
    }
    expect(await relayA).toBe(2);

    expect(log).toEqual([ids[2], ids[3], ids[0], ids[1]]);
    expect(await count(pool, "select count(*) from outbox where published_at is null")).toBe(0);
  });

  it("relay_rejects_a_non_positive_batch_size", async () => {
    const pool = await newPool();
    for (const batchSize of [0, -1, 1.5]) {
      await expect(runRelay(pool, async () => {}, { batchSize })).rejects.toThrow(/batchSize/);
    }
  });
});

describe("chaos", () => {
  it("chaos_relay_crash_after_publish_before_mark_republishes_and_consumer_dedupes", async () => {
    const pool = await newPool();
    const schema = schemas[schemas.length - 1]!;
    const broker = await newBroker();
    const marker = `${broker.queue}:published`;
    redisKeys.push(marker);
    for (const id of ["o-1", "o-2"]) await placeOrder(pool, { id, amount: 10 });
    const [first, second] = await outboxIds(pool);

    // Child: relay thật publish (có confirm) event đầu, ghi marker vào Redis rồi treo TRƯỚC khi đánh dấu published_at.
    const workerPath = fileURLToPath(new URL("./chaos-worker.ts", import.meta.url));
    const repoRoot = fileURLToPath(new URL("../../../", import.meta.url));
    const child = spawn(process.execPath, ["--import", "tsx", workerPath], {
      cwd: repoRoot,
      env: {
        ...process.env,
        DATABASE_URL: databaseUrl,
        AMQP_URL: amqpUrl,
        REDIS_URL: redisUrl,
        SCHEMA: schema,
        QUEUE: broker.queue,
        MARKER_KEY: marker,
      },
      stdio: ["ignore", "ignore", "pipe"],
    });
    children.push(child);
    let childStderr = "";
    child.stderr?.on("data", (chunk: Buffer) => (childStderr += chunk.toString()));
    const exited = new Promise<{ signal: NodeJS.Signals | null }>((resolve) =>
      child.once("exit", (_code, signal) => resolve({ signal })),
    );

    try {
      // Chờ child báo "đã publish và confirm xong event đầu" (marker ngắn hơn timeout của test).
      const markedId = await eventually(async () => (await redis.get(marker)) ?? undefined, {
        timeoutMs: 30_000,
      });
      expect(markedId).toBe(first);
      await eventually(async () => (broker.deliveries.get(first!) ?? 0) >= 1 || undefined, {
        timeoutMs: 10_000,
      });

      // Child đang giữ transaction mở: chưa dòng nào được đánh dấu, và cả hai dòng đang bị khóa
      // (SKIP LOCKED của parent không thấy dòng nào).
      expect(await count(pool, "select count(*) from outbox where published_at is null")).toBe(2);
      const locked = await pool.query(
        "select id from outbox where published_at is null order by id for update skip locked",
      );
      expect(locked.rows).toEqual([]);

      // SIGKILL: relay chết sau publish, trước UPDATE và COMMIT.
      child.kill("SIGKILL");
      expect((await exited).signal).toBe("SIGKILL");
    } catch (error) {
      throw new Error(`${String(error)}\nstderr của child:\n${childStderr}`, { cause: error });
    }

    // Relay mới trong parent. Connection của child chết nên Postgres rollback transaction và nhả lock,
    // nhưng việc đó không tức thì: trong lúc lock còn giữ, SKIP LOCKED trả 0 dòng. Do đó chờ bằng eventually.
    const publisher = amqpPublisher(broker.channel, broker.queue);
    const republished = await eventually(
      async () => {
        const published = await runRelay(pool, publisher, { batchSize: 10 });
        return published > 0 ? published : undefined;
      },
      { timeoutMs: 20_000 },
    );
    expect(republished).toBe(2);

    await eventually(
      async () =>
        ((broker.deliveries.get(first!) ?? 0) >= 2 && (broker.deliveries.get(second!) ?? 0) >= 1) ||
        undefined,
      { timeoutMs: 10_000 },
    );

    // Event đầu được giao ít nhất hai lần (một từ child, một từ relay mới), nhưng effect chỉ áp dụng một lần.
    expect(broker.deliveries.get(first!)).toBeGreaterThanOrEqual(2);
    expect(broker.applied).toEqual([first, second]);
    // Không dòng nào bị mất: tất cả đã được đánh dấu.
    expect(await count(pool, "select count(*) from outbox where published_at is null")).toBe(0);
    expect(await count(pool, "select count(*) from outbox")).toBe(2);
  }, 90_000);
});
