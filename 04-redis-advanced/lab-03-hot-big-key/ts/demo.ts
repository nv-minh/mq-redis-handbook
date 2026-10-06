// Demo: tạo một value 1 MiB và nhiều key nhỏ dưới một prefix duy nhất, tìm key lớn bằng SCAN + MEMORY USAGE,
// rồi giải phóng bằng UNLINK. Demo chỉ đụng tới các key dưới prefix của nó.
import { Redis } from "ioredis";
import { uniqueName } from "@handbook/testkit";
import { findBigKeys } from "./lab.js";

const rdb = new Redis(process.env.REDIS_URL ?? "redis://127.0.0.1:6379");
console.log("Demo tìm big key: 1000 key nhỏ và một chuỗi 1 MiB dưới cùng một prefix.");
const prefix = uniqueName("demo:bigkey");
const created: string[] = [];

try {
  const pipeline = rdb.pipeline();
  for (let i = 0; i < 1000; i++) {
    created.push(`${prefix}:small:${i}`);
    pipeline.set(`${prefix}:small:${i}`, "x");
  }
  await pipeline.exec();
  created.push(`${prefix}:big`);
  await rdb.set(`${prefix}:big`, "x".repeat(1024 * 1024));

  const small = Number(await rdb.call("MEMORY", "USAGE", `${prefix}:small:0`));
  const big = Number(await rdb.call("MEMORY", "USAGE", `${prefix}:big`));
  console.log(
    `MEMORY USAGE của key nhỏ: ${small} byte, của key lớn: ${big} byte (allocator làm tròn lên so với 1 MiB dữ liệu)`,
  );

  const started = Date.now();
  const found = await findBigKeys(rdb, 512 * 1024, `${prefix}:*`);
  console.log(
    `findBigKeys(512 KiB, "${prefix}:*") -> ${JSON.stringify(found)} trong ${Date.now() - started} ms`,
  );
  console.log(
    "Đã quét 1001 key, trả về",
    found.length,
    "key lớn: ngưỡng quyết định key nào được coi là big.",
  );
} finally {
  // UNLINK giải phóng bộ nhớ ở thread nền, còn DEL trên collection lớn sẽ chặn Redis.
  console.log("UNLINK đã xóa:", await rdb.unlink(...created), "key");
  rdb.disconnect();
}
