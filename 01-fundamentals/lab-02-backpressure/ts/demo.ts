import { setTimeout as sleep } from "node:timers/promises";
import { BoundedQueue } from "./lab.js";

const capacity = 3;
const total = 10;
const consumerDelayMs = 40;

const queue = new BoundedQueue<number>(capacity);
const start = Date.now();
const at = () => `t=${String(Date.now() - start).padStart(4)}ms`;

queue.consume(async (msg) => {
  console.log(`${at()}  consumer  nhận ${msg}  (size=${queue.size()})`);
  await sleep(consumerDelayMs); // consumer chậm
});

// Producer cố ý không tự chờ: bản thân publish() làm nó chậm lại khi queue đầy.
for (let i = 1; i <= total; i++) {
  await queue.publish(i);
  console.log(`${at()}  producer  đã nhận ${i} (size=${queue.size()}/${capacity})`);
}
console.log(
  `${at()}  producer  xong: nó bị consumer điều tiết tốc độ, queue không bao giờ vượt quá ${capacity}`,
);
await sleep(consumerDelayMs * (capacity + 1)); // để consumer xử lý nốt phần đuôi
