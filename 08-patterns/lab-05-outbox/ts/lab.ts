import { connect } from "amqplib";
import type { Channel, ChannelModel, ConfirmChannel } from "amqplib";
import pg from "pg";

const { Client, Pool } = pg;

/** URL của Postgres. Lab đọc `DATABASE_URL`, mặc định là Postgres của `make up`. */
export function databaseUrl(): string {
  return process.env.DATABASE_URL ?? "postgres://handbook:handbook@127.0.0.1:5432/handbook";
}

/** URL của broker. Lab đọc `AMQP_URL`, mặc định là RabbitMQ của `make up`. */
export function amqpUrl(): string {
  return process.env.AMQP_URL ?? "amqp://guest:guest@127.0.0.1:5672";
}

/**
 * Event trong bảng outbox. `id` là khóa tăng dần của bảng và cũng là event id ổn định mà consumer dùng để dedupe.
 * `id` là string vì `pg` trả bigint dưới dạng string (tránh mất chính xác khi vượt 2^53).
 */
export interface OutboxEvent {
  id: string;
  aggregateId: string;
  type: string;
  payload: unknown;
}

export interface Order {
  id: string;
  amount: number;
}

/** Hàm publish do caller cung cấp. Reject nghĩa là event chưa được broker nhận (relay sẽ rollback batch). */
export type PublishFn = (event: OutboxEvent) => Promise<void>;

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
 * Pool có `search_path` trỏ vào schema, nên mọi câu SQL của lab (không ghi tên schema) chỉ chạm bảng của schema đó.
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
 * Hai bảng của lab. `outbox.published_at` null nghĩa là event chưa publish.
 * Partial index chỉ chứa các dòng chưa publish: relay luôn quét "dòng chưa publish theo id", nên index nhỏ
 * và không phình theo lịch sử event đã gửi.
 */
export async function createTables(pool: pg.Pool): Promise<void> {
  await pool.query(`
    create table orders (
      id text primary key,
      amount numeric not null,
      created_at timestamptz not null default now()
    )`);
  await pool.query(`
    create table outbox (
      id bigserial primary key,
      aggregate_id text not null,
      type text not null,
      payload jsonb not null,
      created_at timestamptz not null default now(),
      published_at timestamptz
    )`);
  await pool.query("create index outbox_unpublished_idx on outbox (id) where published_at is null");
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

/**
 * Ghi order và event `OrderPlaced` vào outbox trong MỘT transaction: cả hai cùng commit hoặc cùng biến mất.
 * Đây là phần "transactional" của outbox. Việc gửi lên broker nằm ở relay, không nằm ở đây.
 */
export async function placeOrder(pool: pg.Pool, order: Order): Promise<void> {
  await inTransaction(pool, async (client) => {
    await client.query("insert into orders (id, amount) values ($1, $2)", [order.id, order.amount]);
    await client.query(
      "insert into outbox (aggregate_id, type, payload) values ($1, $2, $3::jsonb)",
      [order.id, "OrderPlaced", JSON.stringify({ orderId: order.id, amount: order.amount })],
    );
  });
}

/**
 * Một lượt của polling relay. Trong MỘT transaction:
 *
 * 1. Chọn tối đa `batchSize` dòng chưa publish theo thứ tự id với `FOR UPDATE SKIP LOCKED`
 *    (relay khác đang giữ dòng nào thì bỏ qua dòng đó, hai relay không bao giờ lấy trùng một dòng).
 * 2. Gọi `publish(event)` lần lượt cho từng dòng và chờ nó xong (với RabbitMQ là chờ publisher confirm).
 * 3. Đặt `published_at` cho cả batch rồi COMMIT.
 *
 * Trả về số event đã publish (0 khi outbox rỗng hoặc mọi dòng đang bị relay khác khóa).
 *
 * At-least-once: nếu process chết sau publish và trước COMMIT, Postgres rollback transaction, nhả lock và dòng
 * quay lại trạng thái chưa publish, nên lượt sau publish lại event đó. Consumer phải dedupe theo `event.id`.
 * Nếu `publish` ném lỗi cho một dòng, cả batch ROLLBACK và lỗi được ném ra: những dòng đã publish trước đó trong
 * batch vẫn chưa được đánh dấu và sẽ được publish lại. Đổi lại, transaction (và lock) mở suốt thời gian publish.
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
      aggregate_id: string;
      type: string;
      payload: unknown;
    }>(
      `select id::text as id, aggregate_id, type, payload
         from outbox
        where published_at is null
        order by outbox.id
        limit $1
        for update skip locked`,
      [opts.batchSize],
    );
    for (const row of rows) {
      await publish({
        id: row.id,
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
 * Mở một connection tới broker. Listener 'error' rỗng để lỗi connection không làm sập process
 * (EventEmitter ném lỗi nếu 'error' không có listener), lỗi vẫn được báo qua 'close' và Promise của channel.
 */
export async function openConnection(): Promise<ChannelModel> {
  const connection = await connect(amqpUrl());
  connection.on("error", () => undefined);
  return connection;
}

/** Khai báo queue quorum (tường minh `x-queue-type`, không phụ thuộc cấu hình mặc định của broker). */
export async function declareQueue(channel: Channel, queue: string): Promise<void> {
  await channel.assertQueue(queue, { durable: true, arguments: { "x-queue-type": "quorum" } });
}

/**
 * Tạo hàm publish cho relay: publish vào queue qua default exchange với publisher confirm, chờ broker ack.
 * Event id đi vào thuộc tính `messageId` (consumer dùng nó để dedupe), type vào `type`, aggregate id vào header.
 * Confirm bị nack hoặc channel đóng thì Promise reject và relay rollback batch.
 * Lưu ý: publish qua default exchange tới queue chưa tồn tại vẫn được confirm (message bị bỏ), nên queue phải khai báo trước.
 */
export function amqpPublisher(channel: ConfirmChannel, queue: string): PublishFn {
  return (event) =>
    new Promise<void>((resolve, reject) => {
      channel.publish(
        "",
        queue,
        Buffer.from(JSON.stringify(event.payload)),
        {
          persistent: true,
          messageId: event.id,
          type: event.type,
          headers: { aggregateId: event.aggregateId },
        },
        (err) => (err ? reject(err) : resolve()),
      );
    });
}

export interface ReceivedEvent {
  /** Event id của outbox (thuộc tính `messageId`). */
  eventId: string;
  type: string;
  body: unknown;
}

/**
 * Consumer ack thủ công: gọi `onEvent` rồi mới ack. Mỗi lần broker giao là một lần gọi, kể cả bản trùng:
 * việc dedupe theo `eventId` là việc của `onEvent`.
 */
export async function startConsumer(
  channel: Channel,
  queue: string,
  consumerTag: string,
  onEvent: (event: ReceivedEvent) => void,
): Promise<void> {
  channel.on("error", () => undefined);
  await channel.prefetch(10);
  await channel.consume(
    queue,
    (msg) => {
      // msg là null khi broker hủy consumer (ví dụ queue bị xóa).
      if (msg === null) return;
      onEvent({
        eventId: String(msg.properties.messageId),
        type: String(msg.properties.type),
        body: JSON.parse(msg.content.toString()),
      });
      channel.ack(msg);
    },
    { noAck: false, consumerTag },
  );
}
