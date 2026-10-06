// Demo transactional outbox: đặt 3 order (mỗi order một dòng outbox trong cùng transaction), mô phỏng relay chết
// sau khi publish event đầu và trước khi đánh dấu, rồi chạy lại relay. Consumer dedupe theo event id nên mỗi effect
// chỉ áp dụng một lần dù event đầu được giao hai lần. Schema và queue bị xóa ở cuối.
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

const schema = uniqueName("demo-outbox");
const queue = uniqueName("demo-outbox");
const connection = await openConnection();
let pool: ReturnType<typeof openPool> | undefined;

try {
  await createSchema(schema);
  pool = openPool(schema);
  await createTables(pool);
  const publishChannel = await connection.createConfirmChannel();
  await declareQueue(publishChannel, queue);
  const publish = amqpPublisher(publishChannel, queue);

  // Consumer dedupe theo event id: bản giao lặp được ack nhưng không áp dụng effect lần nữa.
  const deliveries = new Map<string, number>();
  const applied: string[] = [];
  const consumerChannel = await connection.createChannel();
  await startConsumer(consumerChannel, queue, uniqueName("demo-consumer"), (event) => {
    const n = (deliveries.get(event.eventId) ?? 0) + 1;
    deliveries.set(event.eventId, n);
    if (n === 1) applied.push(event.eventId);
    console.log(
      `  consumer: event ${event.eventId} (lần giao ${n}) -> ${n === 1 ? "áp dụng effect" : "trùng, bỏ qua effect"}`,
    );
  });

  console.log("== 1. placeOrder ghi order và dòng outbox trong cùng một transaction ==");
  for (const id of ["order-1", "order-2", "order-3"]) await placeOrder(pool, { id, amount: 100 });
  const unpublished = async () =>
    Number(
      (await pool!.query("select count(*) from outbox where published_at is null")).rows[0].count,
    );
  console.log(`đã ghi 3 order và 3 dòng outbox, chưa publish: ${await unpublished()}`);

  console.log("\n== 2. Relay chết SAU khi publish event đầu và TRƯỚC khi commit ==");
  const published: OutboxEvent[] = [];
  try {
    await runRelay(
      pool,
      async (event) => {
        await publish(event);
        published.push(event);
        throw new Error("mô phỏng: relay chết ngay sau khi broker confirm");
      },
      { batchSize: 10 },
    );
  } catch (error) {
    console.log(`relay lỗi: ${(error as Error).message}`);
  }
  console.log(`event ${published[0]?.id} đã tới broker nhưng transaction rollback`);
  console.log(`dòng chưa publish vẫn là ${await unpublished()} (không dòng nào được đánh dấu)`);

  console.log("\n== 3. Relay mới chạy lại, batchSize 2 ==");
  for (;;) {
    const n = await runRelay(pool, publish, { batchSize: 2 });
    console.log(`runRelay publish ${n} event`);
    if (n === 0) break;
  }
  await eventually(async () => applied.length === 3 || undefined, { timeoutMs: 10_000 });
  await eventually(async () => (deliveries.get(published[0]!.id) ?? 0) >= 2 || undefined, {
    timeoutMs: 10_000,
  });

  console.log("\n== Kết quả ==");
  for (const [id, n] of deliveries) console.log(`  event ${id}: giao ${n} lần`);
  console.log(`effect đã áp dụng ${applied.length} lần cho ${deliveries.size} event khác nhau`);
  console.log(
    "Kết luận: at-least-once làm event đầu bị giao hai lần, dedupe theo event id giữ effect đúng một lần.",
  );
} finally {
  // Dọn theo thứ tự, mỗi bước nằm trong try riêng để một bước lỗi không bỏ sót bước sau.
  try {
    await pool?.end();
  } finally {
    try {
      const channel = await connection.createChannel();
      await channel.deleteQueue(queue);
    } catch {
      // queue chưa được tạo
    } finally {
      await connection.close().catch(() => undefined);
      await dropSchema(schema);
    }
  }
}
