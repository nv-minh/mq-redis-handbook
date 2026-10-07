import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import type { Pool } from "pg";
import { eventually, uniqueName } from "@handbook/testkit";
import { parseEvent } from "./events.js";
import {
  createSchema,
  createTables,
  dropSchema,
  inventoryHandler,
  openPool,
  paymentHandler,
  placeOrder,
  runRelay,
  seedStock,
  createOrderService,
  type DomainEvent,
  type OrderService,
} from "./lab.js";
import {
  consumerLag,
  createProducer,
  createTopic,
  deleteGroup,
  deleteTopic,
  readTopic,
  sendRaw,
  type RunningConsumer,
  type TopicRecord,
} from "./kafka.js";

const PARTITIONS = 3;
const SKU = "sku-1";

// Mọi resource được đăng ký TRƯỚC khi dùng, để afterEach dọn dù test fail giữa chừng.
const schemas: string[] = [];
const pools: Pool[] = [];
const topics: string[] = [];
const groups: string[] = [];
const services: OrderService[] = [];
const consumers: RunningConsumer[] = [];
const children: ChildProcess[] = [];
const producers: Awaited<ReturnType<typeof createProducer>>[] = [];

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
  // Các pha chạy TUẦN TỰ và có chủ đích: consumer dừng trước (không còn handler nào dùng pool), rồi child chết
  // (nhả lock), rồi HTTP server và producer, rồi pool, rồi mới DROP SCHEMA và xóa topic cùng group.
  // Trong mỗi pha, bước lỗi không cản bước còn lại; lỗi đầu tiên được ném lại ở cuối.
  const errors: unknown[] = [];
  const phase = async (steps: (() => Promise<unknown>)[]): Promise<void> => {
    const results = await Promise.allSettled(steps.map((step) => step()));
    for (const result of results) if (result.status === "rejected") errors.push(result.reason);
  };
  await phase(consumers.splice(0).map((consumer) => () => consumer.stop()));
  await phase([reapChildren]);
  await phase(services.splice(0).map((service) => () => service.close()));
  await phase(producers.splice(0).map((producer) => () => producer.disconnect()));
  await phase(pools.splice(0).map((pool) => () => pool.end()));
  await phase(schemas.splice(0).map((schema) => () => dropSchema(schema)));
  await phase(groups.splice(0).map((group) => () => deleteGroup(group)));
  await phase(topics.splice(0).map((topic) => () => deleteTopic(topic)));
  if (errors.length > 0) throw errors[0];
});

interface Env {
  pool: Pool;
  schema: string;
  topic: string;
  service: OrderService;
}

/** Mỗi test một schema, một topic 3 partition và một service riêng (tên duy nhất, đăng ký trước khi tạo). */
async function newEnv(): Promise<Env> {
  const schema = uniqueName("uc02");
  schemas.push(schema);
  await createSchema(schema);
  const pool = openPool(schema);
  pools.push(pool);
  await createTables(pool);
  const topic = uniqueName("uc02-orders");
  topics.push(topic);
  await createTopic(topic, PARTITIONS);
  const groupPrefix = uniqueName("uc02-group");
  groups.push(`${groupPrefix}-inventory`, `${groupPrefix}-payment`);
  const service = await createOrderService({ pool, topic, groupPrefix });
  services.push(service);
  return { pool, schema, topic, service };
}

async function startConsumer(
  env: Env,
  name: "inventory" | "payment",
  options?: Parameters<OrderService["startConsumer"]>[1],
): Promise<RunningConsumer> {
  const consumer = await env.service.startConsumer(name, options);
  consumers.push(consumer);
  return consumer;
}

async function post(env: Env, body: unknown): Promise<{ status: number; json: unknown }> {
  const response = await fetch(`${env.service.url}/orders`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: typeof body === "string" ? body : JSON.stringify(body),
  });
  return { status: response.status, json: await response.json() };
}

async function scalar<T>(pool: Pool, sql: string, params: unknown[] = []): Promise<T> {
  const { rows } = await pool.query<{ value: T }>(sql, params);
  return rows[0]!.value;
}

const count = async (pool: Pool, sql: string, params: unknown[] = []): Promise<number> =>
  Number(await scalar<string>(pool, sql, params));

const stockOf = async (pool: Pool, sku = SKU): Promise<number> =>
  Number(await scalar<number>(pool, "select quantity as value from stock where sku = $1", [sku]));

const statusOf = async (pool: Pool, orderId: string): Promise<string> =>
  scalar<string>(pool, "select status as value from orders where id = $1", [orderId]);

const trailOf = async (pool: Pool, orderId: string): Promise<string[]> =>
  (
    await pool.query<{ status: string }>(
      "select status from order_history where order_id = $1 order by id",
      [orderId],
    )
  ).rows.map((row) => row.status);

/** Đọc giá trị một series từ endpoint `/metrics` thật (định dạng text của Prometheus), 0 nếu series chưa có. */
async function metric(
  service: OrderService,
  name: string,
  labels: Record<string, string>,
): Promise<number> {
  const text = await (await fetch(service.metricsUrl)).text();
  for (const line of text.split("\n")) {
    if (!line.startsWith(`${name}{`)) continue;
    const ok = Object.entries(labels).every(([key, value]) => line.includes(`${key}="${value}"`));
    if (ok) return Number(line.slice(line.lastIndexOf(" ") + 1));
  }
  return 0;
}

const events = (records: TopicRecord[], type?: string): DomainEvent[] =>
  records.flatMap((r) =>
    r.event && (type === undefined || r.event.type === type) ? [r.event] : [],
  );

/** Envelope của event OrderPlaced đang nằm trong outbox của order (để test tự publish bản trùng lên topic). */
async function placedEnvelope(pool: Pool, orderId: string): Promise<DomainEvent> {
  const { rows } = await pool.query<{ event_id: string; payload: DomainEvent["data"] }>(
    "select event_id, payload from outbox where aggregate_id = $1 and type = 'OrderPlaced'",
    [orderId],
  );
  return {
    eventId: rows[0]!.event_id,
    type: "OrderPlaced",
    orderId,
    data: rows[0]!.payload,
  };
}

const wire = (event: DomainEvent): string => JSON.stringify(event);

describe("usecase-02 order events: outbox, Kafka, idempotent consumers, saga", () => {
  it("post_orders_emits_exactly_one_order_placed_event", async () => {
    const env = await newEnv();
    const response = await post(env, { amount: 25.5, sku: SKU, quantity: 2 });
    expect(response.status).toBe(201);
    const { id, status } = response.json as { id: string; status: string };
    expect(id).toMatch(/^ord-/);
    expect(status).toBe("placed");

    // Order, dòng history và event nằm trong outbox cùng lúc, event chưa publish.
    expect(await count(env.pool, "select count(*) as value from orders")).toBe(1);
    expect(
      await count(env.pool, "select count(*) as value from outbox where published_at is null"),
    ).toBe(1);
    const eventId = await scalar<string>(env.pool, "select event_id as value from outbox");

    expect(await env.service.runRelay()).toBe(1);

    // Đọc topic từ đầu bằng group mới: đúng một OrderPlaced, key là order id, payload đúng.
    const records = await readTopic(env.topic);
    expect(records).toHaveLength(1);
    expect(records[0]!.key).toBe(id);
    expect(events(records, "OrderPlaced")).toEqual([
      { eventId, type: "OrderPlaced", orderId: id, data: { amount: 25.5, sku: SKU, quantity: 2 } },
    ]);

    // Relay chạy lần hai không publish gì thêm và outbox đã được đánh dấu.
    expect(await env.service.runRelay()).toBe(0);
    expect(await readTopic(env.topic)).toHaveLength(1);
    expect(
      await count(env.pool, "select count(*) as value from outbox where published_at is null"),
    ).toBe(0);
  }, 60_000);

  it("invalid_post_body_returns_400_and_inserts_nothing", async () => {
    const env = await newEnv();
    const bad: unknown[] = [
      "{không phải json",
      "[]",
      "null",
      {},
      { amount: 0, sku: SKU },
      { amount: -5, sku: SKU },
      { amount: "12", sku: SKU },
      { amount: 10 },
      { amount: 10, sku: "" },
      { amount: 10, sku: SKU, quantity: 0 },
      { amount: 10, sku: SKU, quantity: 1.5 },
      { amount: 10, sku: SKU, id: "có khoảng trắng" },
    ];
    for (const body of bad) {
      const response = await post(env, body);
      expect(response.status, JSON.stringify(body)).toBe(400);
      expect(typeof (response.json as { error: unknown }).error).toBe("string");
    }
    expect(await count(env.pool, "select count(*) as value from orders")).toBe(0);
    expect(await count(env.pool, "select count(*) as value from outbox")).toBe(0);
    expect(await count(env.pool, "select count(*) as value from order_history")).toBe(0);

    // Id do client chọn: lần đầu 201, lần hai cùng id là 409 và không ghi thêm gì.
    expect((await post(env, { id: "order-fixed", amount: 10, sku: SKU })).status).toBe(201);
    expect((await post(env, { id: "order-fixed", amount: 10, sku: SKU })).status).toBe(409);
    expect(await count(env.pool, "select count(*) as value from orders")).toBe(1);
    expect(await count(env.pool, "select count(*) as value from outbox")).toBe(1);

    // Đường dẫn và method không hỗ trợ.
    expect((await fetch(`${env.service.url}/nope`)).status).toBe(404);
    expect((await fetch(`${env.service.url}/orders`)).status).toBe(405);
  }, 60_000);

  it("relay_with_an_empty_outbox_returns_zero_without_publishing", async () => {
    const env = await newEnv();
    expect(await env.service.runRelay()).toBe(0);
    const seen: string[] = [];
    expect(
      await runRelay(env.pool, async (event) => void seen.push(event.eventId), { batchSize: 5 }),
    ).toBe(0);
    expect(seen).toEqual([]);
    expect(await readTopic(env.topic)).toEqual([]);
  }, 60_000);

  it("duplicate_event_does_not_double_charge", async () => {
    const env = await newEnv();
    await seedStock(env.pool, SKU, 10);
    const orderId = "order-dup";
    await placeOrder(env.pool, { id: orderId, amount: 40, sku: SKU, quantity: 2 });
    const placed = await placedEnvelope(env.pool, orderId);
    await startConsumer(env, "inventory");
    await startConsumer(env, "payment");

    // Cùng một event (cùng eventId) tới ba lần: hai lần cùng key (cùng partition, offset khác nhau)
    // và một lần với key khác (có thể là partition khác).
    const producer = await createProducer();
    producers.push(producer);
    await sendRaw(producer, env.topic, orderId, wire(placed));
    await sendRaw(producer, env.topic, orderId, wire(placed));
    await sendRaw(producer, env.topic, "key-khac", wire(placed));
    for (const consumer of ["inventory", "payment"]) {
      await eventually(
        async () =>
          (await metric(env.service, "consumer_events_total", { consumer, result: "applied" })) +
            (await metric(env.service, "consumer_events_total", {
              consumer,
              result: "duplicate",
            })) ===
            3 || undefined,
        { timeoutMs: 30_000 },
      );
      expect(
        await metric(env.service, "consumer_events_total", { consumer, result: "applied" }),
      ).toBe(1);
      expect(
        await metric(env.service, "consumer_events_total", { consumer, result: "duplicate" }),
      ).toBe(2);
    }

    // Một dòng payment, một reservation, kho trừ đúng một lần, mỗi consumer đúng một dòng dedup.
    expect(await count(env.pool, "select count(*) as value from payments")).toBe(1);
    expect(await count(env.pool, "select count(*) as value from reservations")).toBe(1);
    expect(await stockOf(env.pool)).toBe(8);
    expect(await count(env.pool, "select count(*) as value from processed_events")).toBe(2);
    expect(await statusOf(env.pool, orderId)).toBe("paid");

    // Dòng dedup và effect commit nguyên tử: lỗi chen giữa hai bước rollback cả hai, nên redelivery áp dụng được.
    const second = "order-atomic";
    await placeOrder(env.pool, { id: second, amount: 30, sku: SKU, quantity: 1 });
    const event = await placedEnvelope(env.pool, second);
    const failing = paymentHandler(env.pool, {
      afterDedupInsert: async () => {
        throw new Error("lỗi chèn giữa dedup và effect");
      },
    });
    await expect(failing(event)).rejects.toThrow(/lỗi chèn giữa/);
    expect(
      await count(env.pool, "select count(*) as value from processed_events where event_id = $1", [
        event.eventId,
      ]),
    ).toBe(0);
    expect(
      await count(env.pool, "select count(*) as value from payments where order_id = $1", [second]),
    ).toBe(0);

    const payment = paymentHandler(env.pool);
    expect(await payment(event)).toBe("applied");
    expect(await payment(event)).toBe("duplicate");
    expect(
      await count(env.pool, "select count(*) as value from payments where order_id = $1", [second]),
    ).toBe(1);

    const inventoryFailing = inventoryHandler(env.pool, {
      afterDedupInsert: async () => {
        throw new Error("lỗi chèn giữa dedup và effect");
      },
    });
    const stockBefore = await stockOf(env.pool);
    await expect(inventoryFailing(event)).rejects.toThrow(/lỗi chèn giữa/);
    expect(await stockOf(env.pool)).toBe(stockBefore);
    expect(
      await count(env.pool, "select count(*) as value from reservations where order_id = $1", [
        second,
      ]),
    ).toBe(0);
    const inventory = inventoryHandler(env.pool);
    expect(await inventory(event)).toBe("applied");
    expect(await inventory(event)).toBe("duplicate");
    expect(await stockOf(env.pool)).toBe(stockBefore - 1);
  }, 90_000);

  it("payment_failure_triggers_inventory_release_via_saga", async () => {
    const env = await newEnv();
    await seedStock(env.pool, SKU, 10);
    await startConsumer(env, "inventory");
    // Luật lỗi xác định: thanh toán lỗi khi amount vượt hạn mức.
    await startConsumer(env, "payment", { paymentLimit: 1000 });

    const response = await post(env, { amount: 5000, sku: SKU, quantity: 3 });
    const { id } = response.json as { id: string };

    // Bơm relay tới khi saga chạy xong: OrderPlaced, rồi PaymentFailed và InventoryReserved, rồi InventoryReleased.
    await eventually(
      async () => {
        await env.service.runRelay();
        return (await statusOf(env.pool, id)) === "cancelled" || undefined;
      },
      { timeoutMs: 40_000 },
    );
    await eventually(async () => (await env.service.runRelay()) === 0 || undefined, {
      timeoutMs: 20_000,
    });

    // Kho trở về giá trị ban đầu, reservation ở trạng thái released, payment ở trạng thái failed.
    expect(await stockOf(env.pool)).toBe(10);
    expect(
      await scalar<string>(
        env.pool,
        "select status as value from reservations where order_id = $1",
        [id],
      ),
    ).toBe("released");
    expect(
      await scalar<string>(env.pool, "select status as value from payments where order_id = $1", [
        id,
      ]),
    ).toBe("failed");

    // Dấu vết trạng thái theo thứ tự, và event trên topic.
    expect(await trailOf(env.pool, id)).toEqual(["placed", "payment_failed", "cancelled"]);
    const types = events(await readTopic(env.topic)).map((e) => e.type);
    expect(types[0]).toBe("OrderPlaced");
    expect([...types].sort()).toEqual(
      ["InventoryReleased", "InventoryReserved", "OrderPlaced", "PaymentFailed"].sort(),
    );
    expect(types.indexOf("PaymentFailed")).toBeLessThan(types.indexOf("InventoryReleased"));

    // Redelivery của PaymentFailed: cùng eventId (bị dedup) và một event mới cùng order (guard trạng thái reservation).
    const failed = events(await readTopic(env.topic), "PaymentFailed")[0]!;
    const producer = await createProducer();
    producers.push(producer);
    await sendRaw(producer, env.topic, id, wire(failed));
    await sendRaw(producer, env.topic, id, wire({ ...failed, eventId: "pf-lan-hai" }));
    await eventually(
      async () =>
        ((await metric(env.service, "consumer_events_total", {
          consumer: "inventory",
          result: "duplicate",
        })) === 1 &&
          (await metric(env.service, "consumer_events_total", {
            consumer: "inventory",
            result: "applied",
          })) === 3) ||
        undefined,
      { timeoutMs: 30_000 },
    );
    await env.service.runRelay();
    expect(await stockOf(env.pool)).toBe(10);
    expect(events(await readTopic(env.topic), "InventoryReleased")).toHaveLength(1);
    expect(await trailOf(env.pool, id)).toEqual(["placed", "payment_failed", "cancelled"]);
  }, 120_000);

  it("consumer_lag_reaches_zero_after_catching_up", async () => {
    const env = await newEnv();
    await seedStock(env.pool, SKU, 100);
    const N = 5;
    for (let i = 0; i < N; i++) {
      expect((await post(env, { amount: 10 + i, sku: SKU })).status).toBe(201);
    }
    expect(await env.service.runRelay()).toBe(N);

    // Consumer chưa chạy nên group chưa commit gì: lag tính từ offset đầu, đúng bằng N, với một topic không có offset đã commit.
    const inventoryGroup = env.service.groupId("inventory");
    const paymentGroup = env.service.groupId("payment");
    expect(await consumerLag(inventoryGroup, env.topic)).toBe(N);
    expect(await consumerLag(paymentGroup, env.topic)).toBe(N);
    const labels = (group: string) => ({ group, topic: env.topic });
    expect(await metric(env.service, "consumer_group_lag", labels(inventoryGroup))).toBe(N);

    // Cho consumer chạy: lag về 0 và gauge trên /metrics theo đó.
    await startConsumer(env, "inventory");
    await startConsumer(env, "payment");
    for (const group of [inventoryGroup, paymentGroup]) {
      await eventually(async () => (await consumerLag(group, env.topic)) === 0 || undefined, {
        timeoutMs: 40_000,
      });
    }
    expect(await metric(env.service, "consumer_group_lag", labels(inventoryGroup))).toBe(0);
    expect(await metric(env.service, "consumer_group_lag", labels(paymentGroup))).toBe(0);
    expect(await count(env.pool, "select count(*) as value from payments")).toBe(N);
  }, 90_000);

  it("consumer_crash_after_effect_before_offset_commit_redelivers_and_is_deduped", async () => {
    const env = await newEnv();
    await seedStock(env.pool, SKU, 10);
    const { json } = await post(env, { amount: 10, sku: SKU, quantity: 4 });
    const { id } = json as { id: string };
    expect(await env.service.runRelay()).toBe(1);

    // Consumer thứ nhất áp dụng effect (transaction đã commit) rồi "crash" TRƯỚC khi commit offset.
    const first = await startConsumer(env, "inventory", { crashBeforeCommit: () => true });
    await eventually(async () => first.crashed() || undefined, { timeoutMs: 40_000 });
    await first.stop();
    expect(await stockOf(env.pool)).toBe(6);
    expect(await consumerLag(env.service.groupId("inventory"), env.topic)).toBe(1);

    // Consumer thay thế cùng group nhận lại event từ offset đã commit: dedup bỏ qua, effect vẫn đúng một lần.
    await startConsumer(env, "inventory");
    await eventually(
      async () =>
        (await consumerLag(env.service.groupId("inventory"), env.topic)) === 0 || undefined,
      { timeoutMs: 40_000 },
    );
    expect(await stockOf(env.pool)).toBe(6);
    expect(
      await count(env.pool, "select count(*) as value from reservations where order_id = $1", [id]),
    ).toBe(1);
    expect(
      await metric(env.service, "consumer_events_total", {
        consumer: "inventory",
        result: "applied",
      }),
    ).toBe(1);
    expect(
      await metric(env.service, "consumer_events_total", {
        consumer: "inventory",
        result: "duplicate",
      }),
    ).toBe(1);
  }, 90_000);

  it("poison_event_is_skipped_counted_and_does_not_block_the_partition", async () => {
    const env = await newEnv();
    await seedStock(env.pool, SKU, 10);
    const { json } = await post(env, { amount: 10, sku: SKU });
    const { id } = json as { id: string };
    // Event hỏng nằm TRƯỚC event hợp lệ trong cùng partition (cùng key).
    const producer = await createProducer();
    producers.push(producer);
    await sendRaw(producer, env.topic, id, "{không phải json");
    await sendRaw(producer, env.topic, id, JSON.stringify({ eventId: 7 }));
    expect(await env.service.runRelay()).toBe(1);
    await startConsumer(env, "inventory");
    await startConsumer(env, "payment");

    // Lag về 0 nghĩa là cả hai event hỏng đã bị bỏ qua (offset đã commit) và event hợp lệ phía sau đã được xử lý.
    for (const name of ["inventory", "payment"] as const) {
      await eventually(
        async () => (await consumerLag(env.service.groupId(name), env.topic)) === 0 || undefined,
        { timeoutMs: 40_000 },
      );
      expect(
        await metric(env.service, "consumer_events_total", { consumer: name, result: "poison" }),
      ).toBe(2);
    }
    expect(await statusOf(env.pool, id)).toBe("paid");
    expect(await stockOf(env.pool)).toBe(9);
  }, 90_000);

  /**
   * Order hết hàng: kho chỉ có 1 mà order cần 3. Hai consumer chạy độc lập nên có hai thứ tự xen kẽ, và test chạy cả hai
   * bằng cách chỉ khởi động MỘT consumer trước (cổng): `first` chạy trước, consumer còn lại chạy sau.
   * Kết quả phải giống nhau: reservation `rejected`, kho không đổi, `InventoryRejected` trên topic, order `cancelled`.
   */
  async function outOfStockScenario(first: "inventory" | "payment"): Promise<void> {
    const env = await newEnv();
    await seedStock(env.pool, SKU, 1);
    const { json } = await post(env, { amount: 10, sku: SKU, quantity: 3 });
    const { id } = json as { id: string };
    expect(await env.service.runRelay()).toBe(1);

    await startConsumer(env, first);
    await eventually(
      async () =>
        (await metric(env.service, "consumer_events_total", {
          consumer: first,
          result: "applied",
        })) === 1 || undefined,
      { timeoutMs: 40_000 },
    );
    const second = first === "inventory" ? "payment" : "inventory";
    await startConsumer(env, second);
    await eventually(async () => (await statusOf(env.pool, id)) === "cancelled" || undefined, {
      timeoutMs: 40_000,
    });
    await eventually(
      async () =>
        (await metric(env.service, "consumer_events_total", {
          consumer: second,
          result: "applied",
        })) === 1 || undefined,
      { timeoutMs: 40_000 },
    );
    await eventually(async () => (await env.service.runRelay()) === 0 || undefined, {
      timeoutMs: 20_000,
    });

    expect(
      await scalar<string>(
        env.pool,
        "select status as value from reservations where order_id = $1",
        [id],
      ),
    ).toBe("rejected");
    expect(await stockOf(env.pool)).toBe(1);
    expect(events(await readTopic(env.topic), "InventoryRejected")).toHaveLength(1);
    expect(await statusOf(env.pool, id)).toBe("cancelled");
    // Giới hạn đã biết, README ghi rõ: payment chạy song song nên vẫn charge order bị hủy và không có bước hoàn tiền.
    expect(
      await scalar<string>(env.pool, "select status as value from payments where order_id = $1", [
        id,
      ]),
    ).toBe("charged");
    expect(await trailOf(env.pool, id)).toEqual(
      first === "payment" ? ["placed", "paid", "cancelled"] : ["placed", "cancelled"],
    );

    // Giao lại OrderPlaced (cùng eventId rồi eventId khác): không hủy lần hai, không event follow-up mới.
    const placed = await placedEnvelope(env.pool, id);
    const producer = await createProducer();
    producers.push(producer);
    await sendRaw(producer, env.topic, id, wire(placed));
    await sendRaw(producer, env.topic, id, wire({ ...placed, eventId: "op-lan-hai" }));
    await eventually(
      async () =>
        ((await metric(env.service, "consumer_events_total", {
          consumer: "inventory",
          result: "duplicate",
        })) === 1 &&
          (await metric(env.service, "consumer_events_total", {
            consumer: "inventory",
            result: "applied",
          })) === 2) ||
        undefined,
      { timeoutMs: 40_000 },
    );
    await env.service.runRelay();
    expect(events(await readTopic(env.topic), "InventoryRejected")).toHaveLength(1);
    expect(
      await count(
        env.pool,
        "select count(*) as value from outbox where type = 'InventoryRejected'",
      ),
    ).toBe(1);
    expect(await stockOf(env.pool)).toBe(1);
    expect(await statusOf(env.pool, id)).toBe("cancelled");
  }

  it("out_of_stock_order_is_rejected_and_cancelled_when_payment_ran_first", async () => {
    await outOfStockScenario("payment");
  }, 120_000);

  it("out_of_stock_order_is_rejected_and_cancelled_when_inventory_ran_first", async () => {
    await outOfStockScenario("inventory");
  }, 120_000);

  it("metrics_endpoint_returns_500_when_kafka_is_unreachable", async () => {
    const env = await newEnv();
    // Cổng 1 không có ai nghe: admin client của bộ thu lag bị từ chối kết nối. Producer của service đã kết nối từ trước.
    const previous = process.env.KAFKA_BROKERS;
    process.env.KAFKA_BROKERS = "127.0.0.1:1";
    const started = Date.now();
    try {
      const response = await fetch(env.service.metricsUrl, { signal: AbortSignal.timeout(40_000) });
      expect(response.status).toBe(500);
      expect((await response.text()).length).toBeGreaterThan(0);
    } finally {
      if (previous === undefined) delete process.env.KAFKA_BROKERS;
      else process.env.KAFKA_BROKERS = previous;
    }
    // Lỗi phải có hạn: scrape không được treo hàng phút khi broker không với tới.
    expect(Date.now() - started).toBeLessThan(30_000);
  }, 60_000);

  it("sku_length_counts_code_points_not_utf16_units", async () => {
    const env = await newEnv();
    // Ký tự ngoài BMP chiếm 2 đơn vị UTF-16 nhưng là 1 code point: 64 ký tự được nhận, 65 ký tự bị từ chối.
    expect((await post(env, { amount: 10, sku: "😀".repeat(64) })).status).toBe(201);
    expect((await post(env, { amount: 10, sku: "😀".repeat(65) })).status).toBe(400);
    expect(await count(env.pool, "select count(*) as value from orders")).toBe(1);
  }, 60_000);

  it("parse_event_treats_order_placed_without_valid_amount_as_poison", () => {
    const base = { eventId: "e1", type: "OrderPlaced", orderId: "o1" };
    const data = { sku: SKU, quantity: 1 };
    expect(parseEvent(JSON.stringify({ ...base, data: { ...data, amount: 5 } }))).toBeDefined();
    for (const bad of [data, { ...data, amount: null }, { ...data, amount: "5" }]) {
      expect(
        parseEvent(JSON.stringify({ ...base, data: bad })),
        JSON.stringify(bad),
      ).toBeUndefined();
    }
  });
});

describe("chaos", () => {
  it("chaos_relay_killed_between_publish_and_mark_still_yields_single_effect_downstream", async () => {
    const env = await newEnv();
    await seedStock(env.pool, SKU, 10);
    await env.pool.query("create table chaos_marker (event_id text primary key)");
    // Consumer chạy trong parent: chúng nhận bản publish của child rồi nhận bản republish.
    await startConsumer(env, "inventory");
    await startConsumer(env, "payment");
    const { json } = await post(env, { amount: 10, sku: SKU, quantity: 2 });
    const { id } = json as { id: string };
    const placed = await placedEnvelope(env.pool, id);

    // Child: relay thật publish event lên Kafka (acks=all), ghi marker vào Postgres rồi treo TRƯỚC khi đánh dấu published_at.
    const workerPath = fileURLToPath(new URL("./chaos-worker.ts", import.meta.url));
    const repoRoot = fileURLToPath(new URL("../../../", import.meta.url));
    const child = spawn(process.execPath, ["--import", "tsx", workerPath], {
      cwd: repoRoot,
      env: { ...process.env, SCHEMA: env.schema, TOPIC: env.topic },
      stdio: ["ignore", "ignore", "pipe"],
    });
    children.push(child);
    let childStderr = "";
    child.stderr?.on("data", (chunk: Buffer) => (childStderr += chunk.toString()));
    const exited = new Promise<{ signal: NodeJS.Signals | null }>((resolve) =>
      child.once("exit", (_code, signal) => resolve({ signal })),
    );

    try {
      // Chờ child báo "đã publish và confirm xong". Child thoát hoặc không khởi động được thì báo lỗi ngay,
      // không đợi hết timeout, để stderr của child (ghép vào lỗi ở khối catch) hiện ra nhanh.
      let gone: string | undefined;
      child.once("error", (error) => (gone ??= `child không khởi động được: ${error.message}`));
      child.once(
        "close",
        (code, signal) =>
          (gone ??= `child đã thoát trước khi báo marker (code ${code}, signal ${signal})`),
      );
      const marked = await eventually(
        async () => {
          const { rows } = await env.pool.query<{ event_id: string }>(
            "select event_id from chaos_marker",
          );
          if (rows[0] !== undefined) return { marker: rows[0].event_id };
          return gone === undefined ? undefined : { gone };
        },
        { timeoutMs: 40_000 },
      );
      if ("gone" in marked) throw new Error(marked.gone);
      expect(marked.marker).toBe(placed.eventId);

      // Bản giao đầu tiên đã được áp dụng một lần ở cả hai consumer.
      for (const consumer of ["inventory", "payment"]) {
        await eventually(
          async () =>
            (await metric(env.service, "consumer_events_total", {
              consumer,
              result: "applied",
            })) === 1 || undefined,
          { timeoutMs: 40_000 },
        );
      }
      // Child đang giữ transaction mở: outbox row chưa được đánh dấu và đang bị khóa.
      expect(
        await count(
          env.pool,
          "select count(*) as value from outbox where type = 'OrderPlaced' and published_at is null",
        ),
      ).toBe(1);

      // SIGKILL: relay chết sau publish, trước UPDATE và COMMIT.
      child.kill("SIGKILL");
      expect((await exited).signal).toBe("SIGKILL");
    } catch (error) {
      throw new Error(`${String(error)}\nstderr của child:\n${childStderr}`, { cause: error });
    }

    // Relay mới trong parent. Postgres rollback transaction của child và nhả lock, nhưng không tức thì:
    // trong lúc lock còn giữ, SKIP LOCKED trả 0 dòng. Bơm relay bằng eventually tới khi republish xong
    // và mọi event follow-up (InventoryReserved, PaymentCompleted) cũng đã được publish.
    await eventually(
      async () => {
        await env.service.runRelay();
        const unpublished = await count(
          env.pool,
          "select count(*) as value from outbox where published_at is null",
        );
        const total = await count(env.pool, "select count(*) as value from outbox");
        return (unpublished === 0 && total === 3) || undefined;
      },
      { timeoutMs: 40_000 },
    );

    // Event OrderPlaced nằm trên topic ít nhất hai lần (một từ child, một từ relay mới).
    const records = await readTopic(env.topic);
    expect(
      events(records, "OrderPlaced").filter((e) => e.eventId === placed.eventId).length,
    ).toBeGreaterThanOrEqual(2);
    // Consumer thấy bản trùng nhưng effect chỉ một lần.
    for (const consumer of ["inventory", "payment"]) {
      await eventually(
        async () =>
          (await metric(env.service, "consumer_events_total", { consumer, result: "duplicate" })) >=
            1 || undefined,
        { timeoutMs: 40_000 },
      );
      expect(
        await metric(env.service, "consumer_events_total", { consumer, result: "applied" }),
      ).toBe(1);
    }
    expect(await count(env.pool, "select count(*) as value from payments")).toBe(1);
    expect(await count(env.pool, "select count(*) as value from reservations")).toBe(1);
    expect(await stockOf(env.pool)).toBe(8);
    expect(await statusOf(env.pool, id)).toBe("paid");
    // Follow-up cũng đúng một lần: bản trùng không sinh thêm event.
    expect(events(records, "InventoryReserved")).toHaveLength(1);
    expect(events(records, "PaymentCompleted")).toHaveLength(1);
  }, 120_000);
});
