// Process con của test chaos (chạy bằng `node --import tsx ts/chaos-worker.ts`): một worker thật
// đọc message, ghi marker "đã bắt đầu xử lý message X" vào Redis rồi treo ở handler cho tới khi
// bị SIGKILL. Có thời hạn cứng 60 giây: nếu process cha chết trước thì child vẫn tự thoát,
// không bao giờ thành process mồ côi.
import { Redis } from "ioredis";
import { WorkerGroup } from "./lab.js";

const env = (name: string): string => {
  const value = process.env[name];
  if (!value) throw new Error(`thiếu biến môi trường ${name}`);
  return value;
};

const redis = new Redis(env("REDIS_URL"));
const markerKey = env("MARKER_KEY");
const group = new WorkerGroup({
  redis,
  stream: env("STREAM"),
  group: env("GROUP"),
  minIdleMs: Number(env("MIN_IDLE_MS")),
  blockMs: 100,
});

setTimeout(() => process.exit(0), 60_000);

group.startWorkers(1, async (delivery) => {
  // Báo cho process cha qua Redis rồi treo: handler không bao giờ xong.
  await redis.set(markerKey, delivery.id);
  await new Promise<never>(() => {});
});
