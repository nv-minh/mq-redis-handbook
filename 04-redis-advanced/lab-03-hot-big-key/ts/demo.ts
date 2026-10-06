// Demo: create one 1 MiB value and many tiny keys under a unique prefix, find the big one with
// SCAN + MEMORY USAGE, then free it with UNLINK. Only keys under the demo prefix are touched.
import { Redis } from "ioredis";
import { uniqueName } from "@handbook/testkit";
import { findBigKeys } from "./lab.js";

const rdb = new Redis(process.env.REDIS_URL ?? "redis://127.0.0.1:6379");
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
  console.log(`MEMORY USAGE small key: ${small} bytes, big key: ${big} bytes`);

  const started = Date.now();
  const found = await findBigKeys(rdb, 512 * 1024, `${prefix}:*`);
  console.log(
    `findBigKeys(512 KiB, "${prefix}:*") -> ${JSON.stringify(found)} in ${Date.now() - started} ms`,
  );
  console.log("scanned 1001 keys, returned", found.length);
} finally {
  // UNLINK frees the memory in a background thread. DEL on a big collection would block Redis.
  console.log("UNLINK removed:", await rdb.unlink(...created));
  rdb.disconnect();
}
