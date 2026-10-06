// Process con của test chaos (chạy bằng `node --import tsx ts/chaos-worker.ts`): một consumer thật
// đọc message, ghi marker "đã bắt đầu xử lý message X" vào Redis rồi treo ở handler cho tới khi
// bị SIGKILL. Có thời hạn cứng 60 giây: nếu process cha chết trước thì child vẫn tự thoát,
// không bao giờ thành process mồ côi.
import { Redis } from "ioredis";
import { DlqQueue } from "./lab.js";

const env = (name: string): string => {
  const value = process.env[name];
  if (!value) throw new Error(`thiếu biến môi trường ${name}`);
  return value;
};

const url = env("REDIS_URL");
const redis = new Redis(url);
const blocking = new Redis(url);
const markerKey = env("MARKER_KEY");
const queue = new DlqQueue({
  redis,
  blocking,
  stream: env("STREAM"),
  group: env("GROUP"),
  dlqStream: env("DLQ_STREAM"),
  consumer: env("CONSUMER"),
  minIdleMs: Number(env("MIN_IDLE_MS")),
  blockMs: 100,
});

const hardDeadline = setTimeout(() => process.exit(0), 60_000);

const stopped = new AbortController();
queue
  .consumeWithDlq({ maxAttempts: 3, signal: stopped.signal }, async (delivery) => {
    // Báo cho process cha qua Redis rồi treo: handler không bao giờ xong.
    await redis.set(markerKey, delivery.id);
    await new Promise<never>(() => {});
  })
  .catch((error: unknown) => {
    console.error("chaos-worker lỗi:", error);
    clearTimeout(hardDeadline);
    process.exit(1);
  });
