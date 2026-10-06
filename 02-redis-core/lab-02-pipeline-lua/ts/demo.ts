// Demo: số round trip của lệnh tuần tự so với pipeline, rồi check-and-decrement kiểu naive so với Lua
// dưới 50 caller đồng thời. Mọi key đều bị xóa ở cuối.
import { Redis } from "ioredis";
import { uniqueName } from "@handbook/testkit";
import {
  countSocketWrites,
  decrIfPositive,
  decrIfPositiveNaive,
  setPipelined,
  setSequential,
} from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const clients = Array.from({ length: 10 }, () => new Redis(url));
const redis = clients[0] as Redis;
const prefix = uniqueName("demo:pipeline");
const counters = { naive: `${prefix}:naive`, lua: `${prefix}:lua` };
const CALLERS = 50;
const UNITS = 10;

async function race(
  decrement: (client: Redis, key: string) => Promise<boolean>,
  key: string,
): Promise<{ successes: number; finalValue: string | null }> {
  await redis.set(key, String(UNITS));
  const results = await Promise.all(
    Array.from({ length: CALLERS }, (_, i) => decrement(clients[i % clients.length] as Redis, key)),
  );
  return { successes: results.filter(Boolean).length, finalValue: await redis.get(key) };
}

try {
  await Promise.all(clients.map((c) => c.ping()));

  console.log("== round trip: 100 lệnh SET ==");
  const writes = await countSocketWrites(redis);
  writes.reset();
  await setSequential(redis, `${prefix}:seq`, 100);
  console.log("tuần tự:  số write trên socket =", writes.writes());
  writes.reset();
  await setPipelined(redis, `${prefix}:pipe`, 100);
  console.log("pipeline: số write trên socket =", writes.writes());

  console.log(`== ${CALLERS} caller đồng thời dùng chung một counter đang giữ ${UNITS} ==`);
  const naive = await race(decrIfPositiveNaive, counters.naive);
  console.log(
    `naive GET rồi DECR: ${naive.successes} caller thành công, counter kết thúc ở ${naive.finalValue}`,
  );
  const lua = await race(decrIfPositive, counters.lua);
  console.log(
    `Lua script:         ${lua.successes} caller thành công, counter kết thúc ở ${lua.finalValue}`,
  );
} finally {
  const pipeline = redis.pipeline();
  for (let i = 0; i < 100; i++) pipeline.del(`${prefix}:seq:${i}`, `${prefix}:pipe:${i}`);
  pipeline.del(counters.naive, counters.lua);
  await pipeline.exec();
  for (const c of clients) c.disconnect();
}
