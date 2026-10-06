import type { Redis } from "ioredis";

/** Ba setting mà lab này thay đổi, dưới dạng chuỗi mà CONFIG GET trả về. */
export interface EvictionConfig {
  maxmemory: string;
  policy: string;
  samples: string;
}

const BATCH = 50;

async function configValue(redis: Redis, name: string): Promise<string> {
  // Đã đo với ioredis 6.0.0 (RESP3): CONFIG GET trả về một mảng PHẲNG ["name", "value"].
  const reply = (await redis.config("GET", name)) as string[];
  const value = reply[1];
  if (reply[0] !== name || value === undefined) {
    throw new Error(`reply của CONFIG GET ${name} không như mong đợi: ${JSON.stringify(reply)}`);
  }
  return value;
}

/** Đọc các setting hiện tại. Gọi hàm này TRƯỚC lần CONFIG SET đầu tiên để còn có cái mà khôi phục. */
export async function readConfig(redis: Redis): Promise<EvictionConfig> {
  return {
    maxmemory: await configValue(redis, "maxmemory"),
    policy: await configValue(redis, "maxmemory-policy"),
    samples: await configValue(redis, "maxmemory-samples"),
  };
}

/** Áp dụng setting. Policy được đặt trước, để maxmemory thấp hơn không bao giờ bị áp dụng với policy cũ. */
export async function writeConfig(redis: Redis, config: EvictionConfig): Promise<void> {
  await redis.config("SET", "maxmemory-policy", config.policy);
  await redis.config("SET", "maxmemory-samples", config.samples);
  await redis.config("SET", "maxmemory", config.maxmemory);
}

/** Parse một field dạng số của INFO. */
async function infoNumber(redis: Redis, section: string, field: string): Promise<number> {
  const info = await redis.info(section);
  const match = new RegExp(`^${field}:(\\d+)`, "m").exec(info);
  if (!match) throw new Error(`INFO ${section} không có ${field}`);
  return Number(match[1]);
}

export const usedMemory = (redis: Redis) => infoNumber(redis, "memory", "used_memory");

/** Tổng số key mà server đã evict kể từ lúc khởi động (INFO stats evicted_keys). */
export const evictedKeys = (redis: Redis) => infoNumber(redis, "stats", "evicted_keys");

/**
 * Giới hạn bộ nhớ theo mức đang dùng ngay lúc này, để lab chạy được trên mọi baseline:
 * maxmemory = used_memory + headroomBytes. samples=10 làm LRU xấp xỉ gần với LRU thật hơn.
 * Trả về maxmemory đã đặt, tính bằng byte.
 */
export async function limitMemory(
  redis: Redis,
  policy: string,
  headroomBytes: number,
): Promise<number> {
  const maxmemory = (await usedMemory(redis)) + headroomBytes;
  await writeConfig(redis, { policy, samples: "10", maxmemory: String(maxmemory) });
  return maxmemory;
}

/** Ghi `count` key `${prefix}:0 .. ${prefix}:count-1`, mỗi key chứa `valueSize` byte. */
export async function seedKeys(
  redis: Redis,
  prefix: string,
  count: number,
  valueSize: number,
): Promise<void> {
  const value = "x".repeat(valueSize);
  for (let start = 0; start < count; start += BATCH) {
    const pipeline = redis.pipeline();
    for (let i = start; i < Math.min(start + BATCH, count); i++)
      pipeline.set(`${prefix}:${i}`, value);
    const results = await pipeline.exec();
    const failed = results?.find(([error]) => error !== null);
    if (failed) throw failed[0];
  }
}

export interface FillOptions {
  /** Số byte của mỗi value. Mặc định 1024. */
  valueSize?: number;
  /** Một key được GET sau mỗi batch, để nó luôn "vừa được dùng". */
  touchKey?: string;
  /** Dừng khi đã có ít nhất chừng này key bị evict. Mặc định 1. */
  minEvicted?: number;
}

/**
 * Ghi liên tục `${prefix}:0 ..` (tối đa `maxKeys` key, theo batch 50) cho tới khi server đã
 * evict ít nhất `minEvicted` key. Trả về số key bị evict trong lúc gọi
 * (độ chênh của evicted_keys), 0 nếu chưa bao giờ chạm giới hạn.
 */
export async function fillUntilEviction(
  redis: Redis,
  prefix: string,
  maxKeys: number,
  opts: FillOptions = {},
): Promise<number> {
  const { valueSize = 1024, touchKey, minEvicted = 1 } = opts;
  const value = "x".repeat(valueSize);
  const before = await evictedKeys(redis);
  for (let start = 0; start < maxKeys; start += BATCH) {
    const pipeline = redis.pipeline();
    for (let i = start; i < Math.min(start + BATCH, maxKeys); i++)
      pipeline.set(`${prefix}:${i}`, value);
    const results = await pipeline.exec();
    const failed = results?.find(([error]) => error !== null);
    if (failed) throw failed[0];
    if (touchKey) await redis.get(touchKey);
    const evicted = (await evictedKeys(redis)) - before;
    if (evicted >= minEvicted) return evicted;
  }
  return (await evictedKeys(redis)) - before;
}

/**
 * Ghi liên tục `${prefix}:0 ..` cho tới khi server từ chối một lần ghi. Trả về message lỗi của
 * lần ghi đầu tiên bị từ chối, hoặc null nếu cả `maxKeys` lần ghi đều thành công. Các key ghi trước lúc bị từ chối vẫn còn.
 */
export async function fillUntilRejected(
  redis: Redis,
  prefix: string,
  maxKeys: number,
  valueSize = 1024,
): Promise<string | null> {
  const value = "x".repeat(valueSize);
  for (let start = 0; start < maxKeys; start += BATCH) {
    const pipeline = redis.pipeline();
    for (let i = start; i < Math.min(start + BATCH, maxKeys); i++)
      pipeline.set(`${prefix}:${i}`, value);
    const results = await pipeline.exec();
    const failed = results?.find(([error]) => error !== null);
    if (failed) return (failed[0] as Error).message;
  }
  return null;
}

/** Xóa mọi key bắt đầu bằng `${prefix}:`, dùng SCAN (không bao giờ dùng KEYS) và UNLINK. */
export async function deleteByPrefix(redis: Redis, prefix: string): Promise<void> {
  let cursor = "0";
  do {
    const [next, keys] = await redis.scan(cursor, "MATCH", `${prefix}:*`, "COUNT", 1000);
    cursor = next;
    if (keys.length > 0) await redis.unlink(...keys);
  } while (cursor !== "0");
}

/** Giá trị `dbfilename` mà infra/docker-compose.yml đặt cho Redis riêng của handbook. */
export const OWN_REDIS_MARKER = "mq-handbook.rdb";

export interface ServerState {
  dbfilename: string;
  maxmemory: string;
  policy: string;
}

/** Kiểm tra thuần: vì sao lab không được dùng server này, hoặc null khi an toàn. */
export function guardProblem(state: ServerState, marker: string): string | null {
  if (state.dbfilename !== marker) {
    return `refusing to run: this Redis reports dbfilename "${state.dbfilename}", not the handbook marker "${marker}", so it is not the compose Redis of this repo (REDIS_URL points elsewhere?). Nothing was changed.`;
  }
  if (state.maxmemory !== "0" || state.policy !== "noeviction") {
    return `refusing to run: maxmemory=${state.maxmemory} policy=${state.policy} is not pristine (maxmemory 0, noeviction). Leftover config from an earlier run, run \`make down && make up\`. Nothing was changed.`;
  }
  return null;
}

/**
 * Ném lỗi trừ khi server là Redis riêng của handbook ở trạng thái nguyên vẹn.
 * Chỉ đọc (CONFIG GET): gọi trước lần CONFIG SET đầu tiên, và không bao giờ khôi phục sau khi bị từ chối.
 */
export async function assertOwnRedis(redis: Redis, marker = OWN_REDIS_MARKER): Promise<void> {
  const problem = guardProblem(
    {
      dbfilename: await configValue(redis, "dbfilename"),
      maxmemory: await configValue(redis, "maxmemory"),
      policy: await configValue(redis, "maxmemory-policy"),
    },
    marker,
  );
  if (problem) throw new Error(problem);
}
