// Demo idempotent consumer: cùng một message được giao nhiều lần mà effect chỉ chạy một lần,
// effect lỗi thì key được nhả để retry, và cửa sổ "bản trùng đến trong lúc effect đầu còn chạy".
// Mọi key được xóa ở cuối.
import { Redis } from "ioredis";
import { uniqueName } from "@handbook/testkit";
import { dedupKey, processOnce } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const rdb = new Redis(url);
const ids: string[] = [];

function newId(): string {
  const id = uniqueName("demo-msg");
  ids.push(id);
  return id;
}

try {
  console.log("== 1. Giao trùng: broker giao cùng một message 3 lần (at-least-once) ==");
  const id1 = newId();
  let charged = 0;
  const charge = async () => {
    charged += 1;
  };
  for (let i = 1; i <= 3; i++) {
    console.log(`lần giao ${i}: ${await processOnce(rdb, id1, charge, 60)}`);
  }
  console.log(`effect (trừ tiền) đã chạy ${charged} lần, key còn ${await rdb.ttl(dedupKey(id1))}s`);

  console.log("\n== 2. Effect lỗi: key được nhả để lần giao lại còn xử lý được ==");
  const id2 = newId();
  await processOnce(
    rdb,
    id2,
    async () => {
      throw new Error("downstream timeout");
    },
    60,
  ).catch((error: Error) => console.log(`lần 1: effect lỗi (${error.message})`));
  console.log(`key còn tồn tại sau lỗi: ${(await rdb.exists(dedupKey(id2))) === 1}`);
  console.log(`lần 2 (redelivery): ${await processOnce(rdb, id2, charge, 60)}`);

  console.log("\n== 3. Cửa sổ đã biết: bản trùng đến khi effect đầu còn chạy ==");
  const id3 = newId();
  let release!: (error: Error) => void;
  const slow = new Promise<void>((_, reject) => {
    release = reject;
  });
  const first = processOnce(rdb, id3, () => slow, 60).catch((e: Error) => e.message);
  while ((await rdb.exists(dedupKey(id3))) === 0) await new Promise((r) => setImmediate(r));
  const second = await processOnce(rdb, id3, charge, 60);
  console.log(`bản trùng đến lúc effect đầu còn chạy: ${second} (consumer sẽ ack nó)`);
  release(new Error("effect đầu lỗi"));
  console.log(`effect đầu kết thúc: ${await first}`);
  console.log(
    `key còn tồn tại: ${(await rdb.exists(dedupKey(id3))) === 1}, nhưng bản trùng đã ack nên effect mất.`,
  );
  console.log("Cách giảm thiểu: trạng thái processing (TTL ngắn) và done (TTL dài), xem README.");
} finally {
  if (ids.length > 0) await rdb.del(...ids.map(dedupKey));
  rdb.disconnect();
}
