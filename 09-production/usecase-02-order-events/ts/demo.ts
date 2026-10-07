// Demo luồng order hướng sự kiện: POST /orders, outbox, relay, Kafka, hai consumer idempotent, saga hoàn kho
// khi thanh toán lỗi, event bị giao trùng và lag trên /metrics. Schema, topic và group bị xóa ở cuối.
import { setTimeout as sleep } from "node:timers/promises";
import type { Pool } from "pg";
import { eventually, uniqueName } from "@handbook/testkit";
import {
  createOrderService,
  createSchema,
  createTables,
  dropSchema,
  openPool,
  seedStock,
  type OrderService,
} from "./lab.js";
import {
  createProducer,
  createTopic,
  deleteGroup,
  deleteTopic,
  readTopic,
  sendRaw,
} from "./kafka.js";

const SKU = "ao-thun";
const schema = uniqueName("demo-orders");
const topic = uniqueName("demo-order-events");
const groupPrefix = uniqueName("demo-group");

const cleanupSteps: (() => Promise<unknown>)[] = [];
let pool: Pool | undefined;
let service: OrderService | undefined;
let relayRunning = false;
let relayLoop: Promise<void> | undefined;
let primaryFailure: unknown;

async function post(body: unknown): Promise<{ status: number; json: Record<string, unknown> }> {
  const response = await fetch(`${service!.url}/orders`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  return { status: response.status, json: (await response.json()) as Record<string, unknown> };
}

async function scalar<T>(sql: string, params: unknown[] = []): Promise<T> {
  const { rows } = await pool!.query<{ value: T }>(sql, params);
  return rows[0]!.value;
}

async function metric(name: string, labels: Record<string, string>): Promise<number> {
  const text = await (await fetch(service!.metricsUrl)).text();
  for (const line of text.split("\n")) {
    if (!line.startsWith(`${name}{`)) continue;
    if (Object.entries(labels).every(([key, value]) => line.includes(`${key}="${value}"`))) {
      return Number(line.slice(line.lastIndexOf(" ") + 1));
    }
  }
  return 0;
}

const status = (id: string) =>
  scalar<string>("select status as value from orders where id = $1", [id]);
const stock = async () =>
  Number(await scalar<number>("select quantity as value from stock where sku = $1", [SKU]));
async function trail(id: string): Promise<string> {
  const { rows } = await pool!.query<{ status: string }>(
    "select status from order_history where order_id = $1 order by id",
    [id],
  );
  return rows.map((row) => row.status).join(" -> ");
}

try {
  // Chuẩn bị: schema, topic 3 partition, kho 10 áo và service (HTTP + producer + metrics).
  cleanupSteps.push(() => deleteTopic(topic));
  cleanupSteps.push(() => deleteGroup(`${groupPrefix}-payment`));
  cleanupSteps.push(() => deleteGroup(`${groupPrefix}-inventory`));
  await createSchema(schema);
  cleanupSteps.push(() => dropSchema(schema));
  pool = openPool(schema);
  cleanupSteps.push(() => pool!.end());
  await createTables(pool);
  await createTopic(topic, 3);
  await seedStock(pool, SKU, 10);
  service = await createOrderService({ pool, topic, groupPrefix });
  cleanupSteps.push(() => service!.close());
  console.log(`Service đang chạy tại ${service.url}, metrics tại ${service.metricsUrl}`);
  console.log(`Kho ban đầu: ${await stock()} sản phẩm ${SKU}\n`);

  await service.startConsumer("inventory");
  await service.startConsumer("payment", { paymentLimit: 1000 });
  // Relay chạy nền: mỗi 200 ms gom các dòng outbox chưa gửi và publish lên Kafka.
  relayRunning = true;
  relayLoop = (async () => {
    while (relayRunning) {
      try {
        await service!.runRelay();
      } catch (error) {
        console.error("[relay] lỗi, sẽ thử lại:", error);
      }
      await sleep(200);
    }
  })();
  cleanupSteps.unshift(async () => {
    relayRunning = false;
    await relayLoop;
  });

  console.log("== 1. Đặt order hợp lệ (amount 250): reserve kho, charge, order thành paid ==");
  const ok = await post({ amount: 250, sku: SKU, quantity: 2 });
  const okId = ok.json.id as string;
  console.log(`POST /orders -> ${ok.status}, id ${okId}`);
  await eventually(async () => ((await status(okId)) === "paid" ? true : undefined), {
    timeoutMs: 30_000,
  });
  console.log(`trạng thái: ${await trail(okId)}`);
  console.log(`kho còn ${await stock()} (trừ 2)\n`);

  console.log("== 2. Thanh toán lỗi (amount 5000 vượt hạn mức 1000): saga hoàn kho ==");
  const bad = await post({ amount: 5000, sku: SKU, quantity: 3 });
  const badId = bad.json.id as string;
  console.log(`POST /orders -> ${bad.status}, id ${badId}`);
  await eventually(async () => ((await status(badId)) === "cancelled" ? true : undefined), {
    timeoutMs: 30_000,
  });
  console.log(`trạng thái: ${await trail(badId)}`);
  console.log(`kho còn ${await stock()}: 3 sản phẩm đã được giữ rồi trả lại (compensation)\n`);

  console.log(
    "== 3. Event bị giao trùng: consumer dedupe theo eventId trong cùng transaction với effect ==",
  );
  const { rows } = await pool.query<{ event_id: string; payload: Record<string, unknown> }>(
    "select event_id, payload from outbox where aggregate_id = $1 and type = 'OrderPlaced'",
    [okId],
  );
  const producer = await createProducer();
  cleanupSteps.unshift(() => producer.disconnect());
  const envelope = JSON.stringify({
    eventId: rows[0]!.event_id,
    type: "OrderPlaced",
    orderId: okId,
    data: rows[0]!.payload,
  });
  await sendRaw(producer, topic, okId, envelope);
  await sendRaw(producer, topic, okId, envelope);
  await eventually(
    async () =>
      (await metric("consumer_events_total", { consumer: "payment", result: "duplicate" })) >= 2
        ? true
        : undefined,
    { timeoutMs: 30_000 },
  );
  const payments = await scalar<string>(
    "select count(*) as value from payments where order_id = $1",
    [okId],
  );
  console.log(`đã publish lại OrderPlaced của ${okId} hai lần`);
  console.log(
    `payment duplicate = ${await metric("consumer_events_total", { consumer: "payment", result: "duplicate" })}, số dòng payment của order = ${payments}, kho còn ${await stock()}`,
  );
  console.log("effect vẫn chỉ áp dụng một lần.\n");

  console.log("== 4. Lag của consumer group trên /metrics ==");
  await eventually(
    async () => {
      const lag = await metric("consumer_group_lag", {
        group: service!.groupId("inventory"),
        topic,
      });
      return lag === 0 ? true : undefined;
    },
    { timeoutMs: 30_000 },
  );
  for (const name of ["inventory", "payment"] as const) {
    const lag = await metric("consumer_group_lag", { group: service.groupId(name), topic });
    console.log(`consumer_group_lag{group="${service.groupId(name)}"} = ${lag}`);
  }
  console.log("lag 0: hai consumer đã xử lý và commit offset mọi event trên topic.\n");

  console.log("== 5. Các event trên topic (key là order id) ==");
  relayRunning = false;
  await relayLoop;
  await service.runRelay();
  for (const record of await readTopic(topic)) {
    const event = record.event;
    console.log(
      `partition ${record.partition} offset ${record.offset}  ${event?.type ?? "poison"}  order ${event?.orderId}`,
    );
  }
  console.log(
    "\nGiao hàng là at-least-once từ đầu tới cuối; tính đúng đắn (effectively-once) đến từ dedup trong transaction của Postgres.",
  );
} catch (error) {
  primaryFailure = error;
}

// Dọn dẹp tuần tự theo thứ tự đăng ký ngược (relay, consumer và HTTP trước, rồi pool, schema, group, topic).
// Mỗi bước chạy dù bước trước lỗi. Lỗi dọn dẹp không che lỗi của phần chính.
const cleanupErrors: unknown[] = [];
for (const step of [...cleanupSteps].reverse()) {
  try {
    await step();
  } catch (error) {
    cleanupErrors.push(error);
  }
}
if (primaryFailure) throw primaryFailure;
if (cleanupErrors.length > 0) throw cleanupErrors[0];
console.log("Đã dọn schema, topic và group của demo.");
