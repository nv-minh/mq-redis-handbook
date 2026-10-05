import { setTimeout as sleep } from "node:timers/promises";
import { BoundedQueue } from "./lab.js";

const capacity = 3;
const total = 10;
const consumerDelayMs = 40;

const queue = new BoundedQueue<number>(capacity);
const start = Date.now();
const at = () => `t=${String(Date.now() - start).padStart(4)}ms`;

queue.consume(async (msg) => {
  console.log(`${at()}  consumer  takes ${msg}  (size=${queue.size()})`);
  await sleep(consumerDelayMs); // a slow consumer
});

// The producer never waits on purpose: publish() itself slows it down once the queue is full.
for (let i = 1; i <= total; i++) {
  await queue.publish(i);
  console.log(`${at()}  producer  accepted ${i} (size=${queue.size()}/${capacity})`);
}
console.log(
  `${at()}  producer  done: it was paced by the consumer, the queue never exceeded ${capacity}`,
);
await sleep(consumerDelayMs * (capacity + 1)); // let the consumer finish the tail
