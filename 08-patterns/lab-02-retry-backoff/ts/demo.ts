// Demo full jitter backoff: mô phỏng 1000 client cùng retry sau một sự cố, dùng PRNG có seed
// nên mỗi lần chạy in ra đúng cùng một kết quả. Demo không cần broker.
import { backoffBound, backoffDelay, type BackoffOptions } from "./lab.js";

const opts: BackoffOptions = { baseMs: 100, capMs: 5_000 };
const clients = 1000;

/** PRNG có seed (mulberry32). */
function seeded(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const rand = seeded(2026);
console.log(`== Full jitter: base ${opts.baseMs}ms, cap ${opts.capMs}ms, ${clients} client ==`);
console.log("attempt | cận trên (không jitter) | min     | trung bình | max");
for (let attempt = 0; attempt <= 8; attempt++) {
  const delays = Array.from({ length: clients }, () => backoffDelay(attempt, opts, rand));
  const mean = delays.reduce((a, b) => a + b, 0) / clients;
  console.log(
    `${String(attempt).padStart(7)} | ${backoffBound(attempt, opts).toFixed(0).padStart(23)} | ` +
      `${Math.min(...delays)
        .toFixed(0)
        .padStart(7)} | ${mean.toFixed(0).padStart(10)} | ${Math.max(...delays).toFixed(0)}`,
  );
}

const attempt = 5;
const bound = backoffBound(attempt, opts);
console.log(
  `\n== Phân bố delay của ${clients} client ở attempt ${attempt} (cận trên ${bound}ms) ==`,
);
console.log(`Không jitter: cả ${clients} client cùng retry đúng lúc ${bound}ms (thundering herd).`);
console.log("Full jitter: các client rải đều trên [0, cận trên), mỗi # là 5 client:");
const buckets = new Array<number>(10).fill(0);
for (let i = 0; i < clients; i++) {
  const delay = backoffDelay(attempt, opts, rand);
  const index = Math.min(9, Math.floor((delay / bound) * 10));
  buckets[index] = (buckets[index] ?? 0) + 1;
}
buckets.forEach((count, i) => {
  const from = ((i * bound) / 10).toFixed(0).padStart(5);
  const to = (((i + 1) * bound) / 10).toFixed(0).padStart(5);
  console.log(`${from}-${to}ms | ${"#".repeat(Math.round(count / 5))} ${count}`);
});

console.log("\n== Attempt rất lớn không tràn số ==");
console.log(
  `attempt 1100: cận trên ${backoffBound(1100, opts)}ms (2 ** 1100 là Infinity, nhưng cap vẫn chặn).`,
);
