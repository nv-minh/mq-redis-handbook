// Process con của test chaos (chạy bằng `node --import tsx ts/chaos-worker.ts`): một relay thật.
// Nó publish event đầu lên RabbitMQ (chờ confirm), ghi marker vào Redis rồi treo TRƯỚC khi relay đánh dấu
// published_at, tức là transaction vẫn mở và giữ lock. Process cha SIGKILL nó.
// Có thời hạn cứng 60 giây: nếu process cha chết trước thì child vẫn tự thoát, không bao giờ thành process mồ côi.
import { Redis } from "ioredis";
import { amqpPublisher, openConnection, openPool, runRelay } from "./lab.js";

const env = (name: string): string => {
  const value = process.env[name];
  if (!value) throw new Error(`thiếu biến môi trường ${name}`);
  return value;
};

setTimeout(() => process.exit(0), 60_000);

const pool = openPool(env("SCHEMA"));
const redis = new Redis(env("REDIS_URL"));
const connection = await openConnection();
const channel = await connection.createConfirmChannel();
const publish = amqpPublisher(channel, env("QUEUE"));

runRelay(
  pool,
  async (event) => {
    await publish(event);
    // Báo cho process cha qua Redis rồi treo: callback không bao giờ xong nên relay không bao giờ đánh dấu.
    await redis.set(env("MARKER_KEY"), event.id);
    await new Promise<never>(() => {});
  },
  { batchSize: 10 },
).catch((error: unknown) => {
  console.error("chaos-worker lỗi:", error);
  process.exit(1);
});
