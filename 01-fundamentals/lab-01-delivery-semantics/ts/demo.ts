import { createIdempotentHandler, createQueue, type Mode } from "./lab.js";

/** PRNG nhỏ có seed (mulberry32) để demo in ra cùng kết quả ở mọi lần chạy. */
function seeded(seed: number): () => number {
  let a = seed;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const ids = Array.from({ length: 6 }, (_, i) => `order-${i + 1}`);

async function run(mode: Mode, lossRate: number, dedup: boolean): Promise<void> {
  const queue = createQueue({ mode, lossRate, rand: seeded(7), maxDeliveries: 4 });
  const deliveries: string[] = [];
  const applied: string[] = [];
  const apply = createIdempotentHandler((id) => applied.push(id));
  queue.consume((id) => {
    deliveries.push(id);
    if (dedup) apply(id);
    else applied.push(id);
  });
  ids.forEach((id) => queue.publish(id));
  await queue.drain();

  const lost = ids.filter((id) => !applied.includes(id));
  console.log(`\n== ${mode}, lossRate=${lossRate}${dedup ? ", consumer idempotent" : ""}`);
  console.log(`đã publish       : ${ids.length}`);
  console.log(`số lần deliver  : ${deliveries.length} (${deliveries.join(" ")})`);
  console.log(`đã xử lý         : ${applied.length} (${applied.join(" ")})`);
  console.log(`chưa từng xử lý  : ${lost.length === 0 ? "không có" : lost.join(" ")}`);
  console.log(`xử lý trùng      : ${applied.length - new Set(applied).size}`);
}

await run("at-most-once", 0.4, false);
await run("at-least-once", 0.4, false);
await run("at-least-once", 0.4, true);
