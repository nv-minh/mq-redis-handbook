// Demo: năm kiểu dữ liệu cốt lõi của Redis và bảng xếp hạng, trên các key sẽ bị xóa ở cuối.
import { Redis } from "ioredis";
import { uniqueName } from "@handbook/testkit";
import { Leaderboard } from "./lab.js";

const redis = new Redis(process.env.REDIS_URL ?? "redis://127.0.0.1:6379");
const prefix = uniqueName("demo:datatypes");
const keys = {
  string: `${prefix}:string`,
  hash: `${prefix}:hash`,
  list: `${prefix}:list`,
  set: `${prefix}:set`,
  board: `${prefix}:board`,
};

try {
  console.log("== string: bộ đếm với INCR ==");
  await redis.set(keys.string, "40");
  await redis.incr(keys.string);
  await redis.incrby(keys.string, 1);
  console.log("GET ->", await redis.get(keys.string));

  console.log("== hash: một object, nhiều field ==");
  await redis.hset(keys.hash, { name: "alice", plan: "pro" });
  await redis.hincrby(keys.hash, "logins", 3);
  console.log("HGETALL ->", await redis.hgetall(keys.hash));

  console.log("== list: có thứ tự, push ở đầu, pop ở cuối ==");
  await redis.lpush(keys.list, "c", "b", "a");
  console.log("LRANGE ->", await redis.lrange(keys.list, 0, -1));
  console.log("RPOP ->", await redis.rpop(keys.list));

  console.log("== set: không trùng, không có thứ tự ==");
  await redis.sadd(keys.set, "x", "y", "x");
  console.log("SCARD ->", await redis.scard(keys.set), "(x đã được thêm hai lần)");
  console.log("SISMEMBER y ->", await redis.sismember(keys.set, "y"));

  console.log("== sorted set: bảng xếp hạng ==");
  const board = new Leaderboard(redis, keys.board);
  for (const [name, score] of [
    ["alice", 50],
    ["bob", 90],
    ["carol", 70],
    ["dave", 10],
    ["erin", 80],
  ] as const) {
    await board.add(name, score);
  }
  console.log("top(3) ->", await board.top(3));
  console.log("topWithScores(2) ->", await board.topWithScores(2));
  console.log(
    "reply WITHSCORES thô (RESP3, ioredis 6) ->",
    await redis.zrange(keys.board, 0, "1", "REV", "WITHSCORES"),
  );
  console.log("top(3) trên bảng rỗng ->", await new Leaderboard(redis, `${prefix}:empty`).top(3));
} finally {
  await redis.del(...Object.values(keys));
  redis.disconnect();
}
