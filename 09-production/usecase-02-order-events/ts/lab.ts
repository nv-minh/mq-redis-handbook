import { randomUUID } from "node:crypto";
import http from "node:http";
import type { AddressInfo } from "node:net";
import pg from "pg";
import { Counter, Gauge, Registry } from "@prometheus-io/client";
import type { KafkaJS } from "@confluentinc/kafka-javascript";
import type { DomainEvent, HandleResult } from "./events.js";
import {
  createProducer,
  lagWith,
  startConsumer,
  withAdmin,
  type RunningConsumer,
} from "./kafka.js";

export type { DomainEvent, HandleResult } from "./events.js";

const { Client, Pool } = pg;

/** URL của Postgres. Use case đọc `DATABASE_URL`, mặc định là Postgres của `make up`. */
export function databaseUrl(): string {
  return process.env.DATABASE_URL ?? "postgres://handbook:handbook@127.0.0.1:5432/handbook";
}

/** Đặt tên schema trong dấu nháy kép: tên do `uniqueName` sinh ra có dấu "-" nên phải quote. */
function quoteIdent(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

/** Tạo schema riêng cho một test hoặc một lần chạy demo. */
export async function createSchema(schema: string): Promise<void> {
  const client = new Client({ connectionString: databaseUrl() });
  await client.connect();
  try {
    await client.query(`create schema ${quoteIdent(schema)}`);
  } finally {
    await client.end();
  }
}

/** Xóa schema cùng mọi bảng bên trong. Gọi sau khi mọi pool của schema đã đóng. */
export async function dropSchema(schema: string): Promise<void> {
  const client = new Client({ connectionString: databaseUrl() });
  await client.connect();
  try {
    // lock_timeout: nếu một transaction treo còn giữ bảng thì báo lỗi thay vì chờ mãi.
    await client.query("set lock_timeout = '10s'");
    await client.query(`drop schema if exists ${quoteIdent(schema)} cascade`);
  } finally {
    await client.end();
  }
}

/**
 * Pool có `search_path` trỏ vào schema, nên mọi câu SQL (không ghi tên schema) chỉ chạm bảng của schema đó.
 * Listener 'error' rỗng: connection rảnh bị server ngắt không được làm sập process.
 */
export function openPool(schema: string, max = 10): pg.Pool {
  const pool = new Pool({
    connectionString: databaseUrl(),
    max,
    options: `-c search_path=${quoteIdent(schema)}`,
  });
  pool.on("error", () => undefined);
  return pool;
}

/**
 * Các bảng của use case. Mỗi bảng thuộc một "service" logic, nhưng chúng nằm chung một database để demo gọn:
 *
 * - `orders` và `order_history`: trạng thái order và dấu vết các lần đổi trạng thái.
 * - `outbox`: event chờ publish. `event_id` do Postgres sinh một lần khi ghi dòng, nên mọi lần relay publish lại đều mang cùng id.
 *   `published_at` null nghĩa là chưa publish, partial index chỉ chứa các dòng đó nên không phình theo lịch sử.
 * - `stock` và `reservations`: kho. `quantity >= 0` là hàng rào cuối: không bao giờ bán âm.
 * - `payments`: `order_id` là khóa chính và `event_id` unique, nên một order không thể có hai khoản thanh toán.
 * - `processed_events`: bảng dedup (inbox). Khóa gồm tên consumer và event id, vì hai consumer độc lập cùng đọc một event.
 */
export async function createTables(pool: pg.Pool): Promise<void> {
  const ddl = [
    `create table orders (
      id text primary key,
      amount numeric not null,
      sku text not null,
      quantity integer not null,
      status text not null default 'placed',
      created_at timestamptz not null default now()
    )`,
    `create table order_history (
      id bigserial primary key,
      order_id text not null,
      status text not null,
      created_at timestamptz not null default now()
    )`,
    `create table outbox (
      id bigserial primary key,
      event_id text not null unique default gen_random_uuid()::text,
      aggregate_id text not null,
      type text not null,
      payload jsonb not null,
      created_at timestamptz not null default now(),
      published_at timestamptz
    )`,
    "create index outbox_unpublished_idx on outbox (id) where published_at is null",
    "create table stock (sku text primary key, quantity integer not null check (quantity >= 0))",
    `create table reservations (
      order_id text primary key,
      sku text not null,
      quantity integer not null,
      status text not null
    )`,
    `create table payments (
      order_id text primary key,
      event_id text not null unique,
      amount numeric not null,
      status text not null
    )`,
    `create table processed_events (
      consumer text not null,
      event_id text not null,
      primary key (consumer, event_id)
    )`,
  ];
  for (const statement of ddl) await pool.query(statement);
}

/** Đặt số lượng tồn kho của một sku (ghi đè nếu đã có). */
export async function seedStock(pool: pg.Pool, sku: string, quantity: number): Promise<void> {
  await pool.query(
    `insert into stock (sku, quantity) values ($1, $2)
     on conflict (sku) do update set quantity = excluded.quantity`,
    [sku, quantity],
  );
}

/** Chạy `work` trong một transaction. Lỗi thì ROLLBACK (lỗi của ROLLBACK không che lỗi gốc) và ném lại lỗi gốc. */
async function inTransaction<T>(
  pool: pg.Pool,
  work: (client: pg.PoolClient) => Promise<T>,
): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query("begin");
    const result = await work(client);
    await client.query("commit");
    return result;
  } catch (error) {
    await client.query("rollback").catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

/** Ghi một event vào outbox trong transaction đang mở: nó chỉ tồn tại nếu transaction commit. */
async function emit(
  client: pg.PoolClient,
  type: string,
  orderId: string,
  data: Record<string, unknown>,
): Promise<void> {
  await client.query(
    "insert into outbox (aggregate_id, type, payload) values ($1, $2, $3::jsonb)",
    [orderId, type, JSON.stringify(data)],
  );
}

/**
 * Đổi trạng thái order và ghi dấu vết, chỉ khi trạng thái hiện tại nằm trong `from`.
 * Guard này là lớp bảo vệ thứ hai sau bảng dedup: một event lặp không thể kéo order lùi về trạng thái cũ.
 */
async function setStatus(
  client: pg.PoolClient,
  orderId: string,
  to: string,
  from: string[],
): Promise<void> {
  const result = await client.query(
    "update orders set status = $2 where id = $1 and status = any($3::text[])",
    [orderId, to, from],
  );
  if (result.rowCount === 1) {
    await client.query("insert into order_history (order_id, status) values ($1, $2)", [
      orderId,
      to,
    ]);
  }
}

export interface NewOrder {
  id: string;
  amount: number;
  sku: string;
  quantity: number;
}

/** Lỗi khi một order với cùng id đã tồn tại (HTTP 409). */
export class DuplicateOrderError extends Error {
  constructor(orderId: string) {
    super(`order ${orderId} đã tồn tại`);
    this.name = "DuplicateOrderError";
  }
}

/** Lỗi khi body của `POST /orders` không hợp lệ (HTTP 400). Message dành cho người gọi API. */
export class InvalidOrderError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidOrderError";
  }
}

const ORDER_ID_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;

/**
 * Kiểm tra body của `POST /orders` và điền giá trị mặc định. Hợp đồng:
 *
 * - `amount`: số hữu hạn lớn hơn 0 (bắt buộc).
 * - `sku`: chuỗi không rỗng, tối đa 64 ký tự (bắt buộc).
 * - `quantity`: số nguyên từ 1 tới 1000000, mặc định 1.
 * - `id`: tùy chọn, 1 tới 64 ký tự trong `A-Za-z0-9_-`. Thiếu thì server sinh `ord-<uuid>`.
 */
export function parseOrderRequest(body: unknown): NewOrder {
  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    throw new InvalidOrderError("body phải là một JSON object");
  }
  const { id, amount, sku, quantity } = body as Record<string, unknown>;
  if (typeof amount !== "number" || !Number.isFinite(amount) || amount <= 0) {
    throw new InvalidOrderError("amount phải là số lớn hơn 0");
  }
  if (typeof sku !== "string" || sku === "" || [...sku].length > 64) {
    throw new InvalidOrderError("sku phải là chuỗi không rỗng, tối đa 64 ký tự");
  }
  const qty = quantity === undefined ? 1 : quantity;
  if (typeof qty !== "number" || !Number.isInteger(qty) || qty < 1 || qty > 1_000_000) {
    throw new InvalidOrderError("quantity phải là số nguyên từ 1 tới 1000000");
  }
  if (id !== undefined && (typeof id !== "string" || !ORDER_ID_PATTERN.test(id))) {
    throw new InvalidOrderError("id chỉ gồm 1 tới 64 ký tự trong A-Z a-z 0-9 _ -");
  }
  return { id: id ?? `ord-${randomUUID()}`, amount, sku, quantity: qty };
}

/**
 * Ghi order, dòng history đầu tiên và event `OrderPlaced` vào outbox trong MỘT transaction:
 * cả ba cùng commit hoặc cùng biến mất. Việc gửi lên Kafka nằm ở relay, không nằm ở đây.
 */
export async function placeOrder(pool: pg.Pool, order: NewOrder): Promise<void> {
  try {
    await inTransaction(pool, async (client) => {
      await client.query("insert into orders (id, amount, sku, quantity) values ($1, $2, $3, $4)", [
        order.id,
        order.amount,
        order.sku,
        order.quantity,
      ]);
      await client.query("insert into order_history (order_id, status) values ($1, 'placed')", [
        order.id,
      ]);
      await emit(client, "OrderPlaced", order.id, {
        amount: order.amount,
        sku: order.sku,
        quantity: order.quantity,
      });
    });
  } catch (error) {
    // 23505 = unique_violation: duy nhất khóa chính orders.id có thể vi phạm trong transaction này.
    if ((error as { code?: string }).code === "23505") throw new DuplicateOrderError(order.id);
    throw error;
  }
}

/** Event trong bảng outbox. `eventId` là id ổn định mà consumer dùng để dedupe. */
export interface OutboxEvent {
  eventId: string;
  aggregateId: string;
  type: string;
  payload: unknown;
}

/** Hàm publish do caller cung cấp. Reject nghĩa là event chưa được broker nhận (relay sẽ rollback batch). */
export type PublishFn = (event: OutboxEvent) => Promise<void>;

/**
 * Một lượt của polling relay. Trong MỘT transaction:
 *
 * 1. Chọn tối đa `batchSize` dòng chưa publish theo thứ tự id với `FOR UPDATE SKIP LOCKED`
 *    (relay khác đang giữ dòng nào thì bỏ qua dòng đó).
 * 2. Gọi `publish(event)` lần lượt cho từng dòng và chờ nó xong (với Kafka là chờ ack của acks=all).
 * 3. Đặt `published_at` cho cả batch rồi COMMIT.
 *
 * Trả về số event đã publish (0 khi outbox rỗng hoặc mọi dòng đang bị relay khác khóa).
 *
 * At-least-once: nếu process chết sau publish và trước COMMIT, Postgres rollback transaction, nhả lock và dòng
 * quay lại trạng thái chưa publish, nên lượt sau publish lại event đó với CÙNG `eventId`. Consumer phải dedupe.
 * Nếu `publish` ném lỗi, cả batch ROLLBACK và lỗi được ném ra. Đổi lại, transaction (và lock) mở suốt thời gian publish.
 * Thứ tự: nhiều relay song song với SKIP LOCKED có thể publish đảo thứ tự, và một relay là điều kiện cần chứ chưa đủ
 * (hai transaction commit đảo thứ tự id vẫn có thể làm id nhỏ được publish sau id lớn, xem chương 08 lab 05). Thứ tự mà use case
 * này cần đến từ chỗ khác: event của một order cùng key nên cùng partition, và event follow-up chỉ được ghi sau khi event
 * trước đó đã được publish và consumer đã xử lý nó (quan hệ nhân quả).
 */
export async function runRelay(
  pool: pg.Pool,
  publish: PublishFn,
  opts: { batchSize: number },
): Promise<number> {
  if (!Number.isInteger(opts.batchSize) || opts.batchSize < 1) {
    throw new RangeError(`batchSize phải là số nguyên dương, nhận ${opts.batchSize}`);
  }
  return inTransaction(pool, async (client) => {
    const { rows } = await client.query<{
      id: string;
      event_id: string;
      aggregate_id: string;
      type: string;
      payload: unknown;
    }>(
      `select id::text as id, event_id, aggregate_id, type, payload
         from outbox
        where published_at is null
        order by outbox.id
        limit $1
        for update skip locked`,
      [opts.batchSize],
    );
    for (const row of rows) {
      await publish({
        eventId: row.event_id,
        aggregateId: row.aggregate_id,
        type: row.type,
        payload: row.payload,
      });
    }
    if (rows.length > 0) {
      await client.query("update outbox set published_at = now() where id = any($1::bigint[])", [
        rows.map((row) => row.id),
      ]);
    }
    return rows.length;
  });
}

/**
 * Tạo hàm publish cho relay: gửi envelope JSON lên topic với key là order id (một order luôn nằm trong một partition,
 * nên thứ tự event của order được giữ). Producer chờ ack của acks=all rồi mới resolve.
 */
export function kafkaPublisher(producer: KafkaJS.Producer, topic: string): PublishFn {
  return async (event) => {
    const envelope: DomainEvent = {
      eventId: event.eventId,
      type: event.type as DomainEvent["type"],
      orderId: event.aggregateId,
      data: event.payload as DomainEvent["data"],
    };
    await producer.send({
      topic,
      messages: [{ key: event.aggregateId, value: JSON.stringify(envelope) }],
    });
  };
}

/** Hạn mức mặc định của luật lỗi thanh toán: amount lớn hơn hạn mức thì thanh toán thất bại. */
export const DEFAULT_PAYMENT_LIMIT = 1000;

export interface HandlerOptions {
  /**
   * Chỉ để mô phỏng lỗi: chạy NGAY SAU khi dòng dedup được chèn và TRƯỚC khi effect chạy, trong cùng transaction.
   * Ném lỗi ở đây phải rollback cả dòng dedup lẫn effect.
   */
  afterDedupInsert?: () => Promise<void>;
}

export interface PaymentOptions extends HandlerOptions {
  /** Amount lớn hơn giá trị này thì thanh toán lỗi (luật lỗi xác định để test saga). Mặc định 1000. */
  paymentLimit?: number;
}

export type Handler = (event: DomainEvent) => Promise<HandleResult>;

/**
 * Chèn dòng dedup `(consumer, eventId)`. Trả true nếu đây là lần đầu consumer thấy event.
 * Unique constraint là trọng tài: nếu hai bản của cùng một event chạy đồng thời, bản thứ hai chờ bản đầu commit
 * rồi gặp conflict, nên chỉ một bản áp dụng effect.
 */
async function firstTime(
  client: pg.PoolClient,
  consumer: string,
  eventId: string,
): Promise<boolean> {
  const result = await client.query(
    "insert into processed_events (consumer, event_id) values ($1, $2) on conflict do nothing",
    [consumer, eventId],
  );
  return result.rowCount === 1;
}

/**
 * Consumer `inventory`. Xử lý hai loại event, mỗi event trong MỘT transaction gồm dòng dedup, effect và event follow-up:
 *
 * - `OrderPlaced`: giữ hàng. Đủ hàng thì trừ kho, ghi reservation `reserved` và phát `InventoryReserved`.
 *   Không đủ hàng thì ghi reservation `rejected`, hủy order và phát `InventoryRejected`.
 * - `PaymentFailed`: compensation của saga. Reservation `reserved` được chuyển thành `released`, kho được cộng lại,
 *   order thành `cancelled` và phát `InventoryReleased`. Reservation không còn ở `reserved` thì không làm gì (không release hai lần).
 */
export function inventoryHandler(pool: pg.Pool, options: HandlerOptions = {}): Handler {
  return async (event) => {
    if (event.type !== "OrderPlaced" && event.type !== "PaymentFailed") return "ignored";
    return inTransaction(pool, async (client) => {
      if (!(await firstTime(client, "inventory", event.eventId))) return "duplicate";
      await options.afterDedupInsert?.();
      if (event.type === "OrderPlaced") {
        const sku = event.data.sku as string;
        const quantity = event.data.quantity as number;
        const reserved = await client.query(
          `insert into reservations (order_id, sku, quantity, status) values ($1, $2, $3, 'reserved')
           on conflict (order_id) do nothing`,
          [event.orderId, sku, quantity],
        );
        // Order đã có reservation (event khác id cho cùng order): không trừ kho lần nữa.
        if (reserved.rowCount === 0) return "applied";
        const decremented = await client.query(
          "update stock set quantity = quantity - $2 where sku = $1 and quantity >= $2",
          [sku, quantity],
        );
        if (decremented.rowCount === 1) {
          await emit(client, "InventoryReserved", event.orderId, { sku, quantity });
        } else {
          await client.query("update reservations set status = 'rejected' where order_id = $1", [
            event.orderId,
          ]);
          // Từ `placed` hoặc `paid`: payment chạy song song nên order có thể đã `paid` khi kho báo hết hàng.
          await setStatus(client, event.orderId, "cancelled", ["placed", "paid"]);
          await emit(client, "InventoryRejected", event.orderId, {
            sku,
            quantity,
            reason: "insufficient_stock",
          });
        }
      } else {
        const released = await client.query<{ sku: string; quantity: number }>(
          `update reservations set status = 'released'
            where order_id = $1 and status = 'reserved' returning sku, quantity`,
          [event.orderId],
        );
        const row = released.rows[0];
        if (row !== undefined) {
          await client.query("update stock set quantity = quantity + $2 where sku = $1", [
            row.sku,
            row.quantity,
          ]);
          await emit(client, "InventoryReleased", event.orderId, {
            sku: row.sku,
            quantity: row.quantity,
          });
        }
        await setStatus(client, event.orderId, "cancelled", ["payment_failed"]);
      }
      return "applied";
    });
  };
}

/**
 * Consumer `payment`. Chỉ xử lý `OrderPlaced`, trong MỘT transaction gồm dòng dedup, dòng payment và event follow-up.
 * Amount không vượt hạn mức thì ghi payment `charged`, order thành `paid` và phát `PaymentCompleted`.
 * Vượt hạn mức thì ghi payment `failed`, order thành `payment_failed` và phát `PaymentFailed` (kích hoạt compensation của kho).
 * Lab giả lập cổng thanh toán bằng luật này nên không có lời gọi mạng nào nằm ngoài transaction:
 * với cổng thật, lời gọi phải mang idempotency key (xem README).
 */
export function paymentHandler(pool: pg.Pool, options: PaymentOptions = {}): Handler {
  const limit = options.paymentLimit ?? DEFAULT_PAYMENT_LIMIT;
  return async (event) => {
    if (event.type !== "OrderPlaced") return "ignored";
    return inTransaction(pool, async (client) => {
      if (!(await firstTime(client, "payment", event.eventId))) return "duplicate";
      await options.afterDedupInsert?.();
      const amount = event.data.amount as number;
      const failed = amount > limit;
      const inserted = await client.query(
        `insert into payments (order_id, event_id, amount, status) values ($1, $2, $3, $4)
         on conflict (order_id) do nothing`,
        [event.orderId, event.eventId, amount, failed ? "failed" : "charged"],
      );
      // Order đã có payment (event khác id cho cùng order): không charge lần nữa.
      if (inserted.rowCount === 0) return "applied";
      if (failed) {
        await setStatus(client, event.orderId, "payment_failed", ["placed"]);
        await emit(client, "PaymentFailed", event.orderId, {
          amount,
          limit,
          reason: "amount_exceeds_limit",
        });
      } else {
        await setStatus(client, event.orderId, "paid", ["placed"]);
        await emit(client, "PaymentCompleted", event.orderId, { amount });
      }
      return "applied";
    });
  };
}

export type ConsumerName = "inventory" | "payment";
const CONSUMER_NAMES: ConsumerName[] = ["inventory", "payment"];
const RESULTS = ["applied", "duplicate", "ignored", "poison"] as const;

export interface OrderServiceOptions {
  pool: pg.Pool;
  topic: string;
  /** Group id của consumer là `<groupPrefix>-inventory` và `<groupPrefix>-payment`. */
  groupPrefix: string;
  /** Số event tối đa một lượt relay. Mặc định 10. */
  batchSize?: number;
}

export interface StartConsumerOptions extends PaymentOptions {
  /** Chỉ để mô phỏng lỗi, xem `ConsumerSpec.crashBeforeCommit`. */
  crashBeforeCommit?: (event: DomainEvent) => boolean;
}

export interface OrderService {
  /** Địa chỉ gốc của HTTP server (cổng do hệ điều hành chọn), ví dụ `http://127.0.0.1:41234`. */
  readonly url: string;
  /** URL của `GET /metrics`. */
  readonly metricsUrl: string;
  /** Group id của consumer `name`. */
  groupId(name: ConsumerName): string;
  /** Một lượt relay: publish các dòng outbox chưa gửi lên topic. Trả về số event đã publish. */
  runRelay(): Promise<number>;
  /** Chạy consumer `name` (offset chỉ commit sau khi transaction của effect đã commit). */
  startConsumer(name: ConsumerName, options?: StartConsumerOptions): Promise<RunningConsumer>;
  /** Dừng consumer, đóng HTTP server và producer. Không đóng pool (pool thuộc về caller). Gọi nhiều lần an toàn. */
  close(): Promise<void>;
}

const MAX_BODY_BYTES = 64 * 1024;

/** Hạn cho một lần đo lag khi scrape `/metrics`. */
const LAG_TIMEOUT_MS = 10_000;

/** Lỗi khi body vượt giới hạn (HTTP 413). */
class BodyTooLargeError extends Error {}

function readBody(request: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    request.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        reject(new BodyTooLargeError());
        return;
      }
      chunks.push(chunk);
    });
    request.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    request.on("error", reject);
  });
}

function sendJson(response: http.ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { "Content-Type": "application/json" }).end(JSON.stringify(body));
}

/**
 * Nối HTTP API, relay, consumer và metrics thành một service nhỏ.
 *
 * - `POST /orders` (xem `parseOrderRequest`): 201 `{id, status: "placed"}`, 400 `{error}` khi body sai, 409 khi id đã tồn tại,
 *   413 khi body quá 64 KiB.
 * - `GET /metrics`: registry riêng của instance với `consumer_events_total{consumer,result}` và gauge
 *   `consumer_group_lag{group,topic}`. Lag được đo MỖI LẦN scrape (không cache); nếu Kafka không trả lời thì scrape lỗi 500.
 */
export async function createOrderService(options: OrderServiceOptions): Promise<OrderService> {
  const { pool, topic, groupPrefix, batchSize = 10 } = options;
  const groupId = (name: ConsumerName): string => `${groupPrefix}-${name}`;

  const registry = new Registry();
  const events = new Counter({
    name: "consumer_events_total",
    help: "Số message consumer đã xử lý, theo kết quả (applied, duplicate, ignored, poison).",
    labelNames: ["consumer", "result"],
    registers: [registry],
  });
  // Khởi tạo mọi series bằng 0 để endpoint luôn in ra đủ, kể cả khi chưa có message nào.
  for (const consumer of CONSUMER_NAMES)
    for (const result of RESULTS) events.inc({ consumer, result }, 0);
  const lagGauge = new Gauge({
    name: "consumer_group_lag",
    help: "Tổng số message chưa xử lý của group trên topic (log end offset trừ offset đã commit).",
    labelNames: ["group", "topic"],
    registers: [registry],
    async collect() {
      // Một admin client cho cả hai group trong một lần scrape. Hạn 10 giây (như bản Go): broker không với tới thì
      // scrape lỗi trong thời gian có hạn thay vì treo.
      let timer: NodeJS.Timeout | undefined;
      const timeout = new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`đo lag quá ${LAG_TIMEOUT_MS} ms, Kafka không trả lời`)),
          LAG_TIMEOUT_MS,
        );
      });
      const measure = withAdmin(async (admin) =>
        Promise.all(
          CONSUMER_NAMES.map(
            async (name) => [groupId(name), await lagWith(admin, groupId(name), topic)] as const,
          ),
        ),
      );
      // Nếu hết hạn trước, kết quả muộn của measure bị bỏ: gắn catch để nó không thành unhandled rejection.
      measure.catch(() => undefined);
      const lags = await Promise.race([measure, timeout]).finally(() => clearTimeout(timer));
      lagGauge.reset();
      for (const [group, lag] of lags) lagGauge.set({ group, topic }, lag);
    },
  });

  const producer = await createProducer();
  const publish = kafkaPublisher(producer, topic);
  const running = new Set<RunningConsumer>();

  const server = http.createServer((request, response) => {
    handle(request, response).catch((error: unknown) => {
      if (response.headersSent) return void response.end();
      if (error instanceof BodyTooLargeError) {
        response.setHeader("Connection", "close");
        sendJson(response, 413, { error: "body vượt quá 64 KiB" });
        request.destroy();
        return;
      }
      sendJson(response, 500, { error: String(error) });
    });
  });

  async function handle(
    request: http.IncomingMessage,
    response: http.ServerResponse,
  ): Promise<void> {
    const path = new URL(request.url ?? "/", "http://localhost").pathname;
    if (path === "/metrics" && request.method === "GET") {
      // Tính body TRƯỚC khi gửi header: nếu bộ thu lag lỗi (Kafka không với tới) thì trả 500 (Prometheus thấy up = 0),
      // không phải 200 với body rỗng.
      const body = await registry.metrics();
      response.writeHead(200, { "Content-Type": registry.contentType }).end(body);
      return;
    }
    if (path !== "/orders") {
      sendJson(response, 404, { error: "không có đường dẫn này" });
      return;
    }
    if (request.method !== "POST") {
      response.setHeader("Allow", "POST");
      sendJson(response, 405, { error: "chỉ hỗ trợ POST" });
      return;
    }
    const raw = await readBody(request);
    let order: NewOrder;
    try {
      order = parseOrderRequest(JSON.parse(raw));
    } catch (error) {
      if (error instanceof InvalidOrderError) {
        sendJson(response, 400, { error: error.message });
      } else if (error instanceof SyntaxError) {
        sendJson(response, 400, { error: "body không phải JSON hợp lệ" });
      } else {
        throw error;
      }
      return;
    }
    try {
      await placeOrder(pool, order);
    } catch (error) {
      if (error instanceof DuplicateOrderError) {
        sendJson(response, 409, { error: error.message });
        return;
      }
      throw error;
    }
    sendJson(response, 201, { id: order.id, status: "placed" });
  }

  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
  } catch (error) {
    await producer.disconnect().catch(() => undefined);
    throw error;
  }
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

  let closing: Promise<void> | undefined;
  const doClose = async (): Promise<void> => {
    // Mỗi bước chạy dù bước trước lỗi; lỗi đầu tiên được ném lại ở cuối.
    const errors: unknown[] = [];
    const step = async (fn: () => Promise<unknown>): Promise<void> => {
      try {
        await fn();
      } catch (error) {
        errors.push(error);
      }
    };
    await Promise.all([...running].map((consumer) => step(() => consumer.stop())));
    await step(
      () =>
        new Promise<void>((resolve, reject) => {
          server.close((error) => (error ? reject(error) : resolve()));
          server.closeAllConnections();
        }),
    );
    await step(() => producer.disconnect());
    if (errors.length > 0) throw errors[0];
  };

  return {
    url,
    metricsUrl: `${url}/metrics`,
    groupId,
    runRelay: () => runRelay(pool, publish, { batchSize }),
    async startConsumer(name, consumerOptions = {}) {
      const { crashBeforeCommit, ...handlerOptions } = consumerOptions;
      const handle =
        name === "inventory"
          ? inventoryHandler(pool, handlerOptions)
          : paymentHandler(pool, handlerOptions);
      const consumer = await startConsumer({
        topic,
        groupId: groupId(name),
        handle,
        onResult: (result) => events.inc({ consumer: name, result }),
        ...(crashBeforeCommit === undefined ? {} : { crashBeforeCommit }),
      });
      running.add(consumer);
      return consumer;
    },
    close() {
      closing ??= doClose();
      return closing;
    },
  };
}
