// Process con của test chaos (chạy bằng `node --import tsx ts/chaos-worker.ts`): một relay thật.
// Nó publish event đầu lên Kafka (acks=all), ghi marker vào Postgres (một connection khác, autocommit) rồi treo TRƯỚC khi
// relay đánh dấu published_at, tức là transaction vẫn mở và giữ lock. Process cha SIGKILL nó.
// Có thời hạn cứng 60 giây: nếu process cha chết trước thì child vẫn tự thoát, không bao giờ thành process mồ côi.
import { createProducer } from "./kafka.js";
import { kafkaPublisher, openPool, runRelay } from "./lab.js";

const env = (name: string): string => {
  const value = process.env[name];
  if (!value) throw new Error(`thiếu biến môi trường ${name}`);
  return value;
};

setTimeout(() => process.exit(0), 60_000);

const pool = openPool(env("SCHEMA"));
const producer = await createProducer();
const publish = kafkaPublisher(producer, env("TOPIC"));

runRelay(
  pool,
  async (event) => {
    await publish(event);
    // Báo cho process cha qua Postgres rồi treo: callback không bao giờ xong nên relay không bao giờ đánh dấu.
    await pool.query("insert into chaos_marker (event_id) values ($1)", [event.eventId]);
    await new Promise<never>(() => {});
  },
  { batchSize: 10 },
).catch((error: unknown) => {
  console.error("chaos-worker lỗi:", error);
  process.exit(1);
});
