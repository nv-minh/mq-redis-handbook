import { createIdempotentHandler, createQueue, type Mode } from "./lab.js";

/** Small seeded PRNG (mulberry32) so the demo prints the same thing every run. */
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
  console.log(`\n== ${mode}, lossRate=${lossRate}${dedup ? ", idempotent consumer" : ""}`);
  console.log(`published : ${ids.length}`);
  console.log(`deliveries: ${deliveries.length} (${deliveries.join(" ")})`);
  console.log(`applied   : ${applied.length} (${applied.join(" ")})`);
  console.log(`never applied: ${lost.length === 0 ? "none" : lost.join(" ")}`);
  console.log(`duplicates applied: ${applied.length - new Set(applied).size}`);
}

await run("at-most-once", 0.4, false);
await run("at-least-once", 0.4, false);
await run("at-least-once", 0.4, true);
