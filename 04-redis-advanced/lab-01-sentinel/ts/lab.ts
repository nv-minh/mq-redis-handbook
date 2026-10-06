import { Redis, type RedisOptions } from "ioredis";

/** Tên master mà Sentinel giám sát, đặt trong cấu hình sentinel của infra/docker-compose.yml. */
export const MASTER_NAME = "mymaster";

export interface Address {
  host: string;
  port: number;
}

/** Ba sentinel như máy host nhìn thấy (port được publish 1:1 nên trùng với port trong container). */
export const SENTINELS: Address[] = [
  { host: "127.0.0.1", port: 26379 },
  { host: "127.0.0.1", port: 26380 },
  { host: "127.0.0.1", port: 26381 },
];

/**
 * Sentinel và các node Redis tự khai địa chỉ bằng tên Docker DNS (redis-master:6380), máy host không phân giải được tên này.
 * natMap đổi mọi địa chỉ đã khai sang port đã publish trên localhost.
 * Bảng phải liệt kê cả sentinel: ioredis học danh sách sentinel còn lại từ SENTINEL SENTINELS
 * và nếu thiếu bảng cho chúng thì nó sẽ cố dial sentinel-2:26380, địa chỉ không tồn tại trên host.
 */
export const NAT_MAP: Record<string, Address> = {
  "redis-master:6380": { host: "127.0.0.1", port: 6380 },
  "redis-replica-1:6381": { host: "127.0.0.1", port: 6381 },
  "redis-replica-2:6382": { host: "127.0.0.1", port: 6382 },
  "sentinel-1:26379": { host: "127.0.0.1", port: 26379 },
  "sentinel-2:26380": { host: "127.0.0.1", port: 26380 },
  "sentinel-3:26381": { host: "127.0.0.1", port: 26381 },
};

/** Đổi địa chỉ node tự khai (redis-master:6380) sang địa chỉ mà máy host dial được. */
export function hostAddress(announced: Address): Address {
  return NAT_MAP[`${announced.host}:${announced.port}`] ?? announced;
}

/**
 * Client luôn nói chuyện với master hiện tại.
 * Nó hỏi các sentinel master đang ở đâu (SENTINEL get-master-addr-by-name), kết nối, và hỏi lại ở mỗi lần reconnect,
 * nên sau failover nó tự đi theo node vừa được promote mà không cần khởi động lại.
 * `extra` ghi đè bất kỳ option nào (ví dụ commandTimeout trong test chaos).
 */
export function connectViaSentinel(extra: Partial<RedisOptions> = {}): Redis {
  return new Redis({
    sentinels: SENTINELS,
    name: MASTER_NAME,
    natMap: NAT_MAP,
    // Timeout ngắn khi hỏi sentinel: một sentinel đang chết không được làm chậm việc tìm master.
    sentinelCommandTimeout: 1_000,
    connectTimeout: 2_000,
    // Retry không giới hạn với backoff ngắn: client chờ qua failover thay vì trả lỗi cho các lệnh đang đợi.
    sentinelRetryStrategy: (times) => Math.min(times * 100, 500),
    retryStrategy: (times) => Math.min(times * 100, 500),
    ...extra,
  });
}

/** Chạy `fn` trên sentinel đầu tiên trả lời được, bỏ qua sentinel đang chết. */
async function withSentinel<T>(fn: (sentinel: Redis) => Promise<T>): Promise<T> {
  let lastError: unknown = new Error("chưa cấu hình sentinel nào");
  for (const address of SENTINELS) {
    // protocol 2: reply của SENTINEL là mảng phẳng các cặp field/value (RESP3 sẽ trả dạng khác).
    const sentinel = new Redis({
      ...address,
      protocol: 2,
      lazyConnect: true,
      retryStrategy: null,
      connectTimeout: 1_000,
      commandTimeout: 2_000,
    });
    sentinel.on("error", () => undefined);
    try {
      await sentinel.connect();
      return await fn(sentinel);
    } catch (error) {
      lastError = error;
    } finally {
      sentinel.disconnect();
    }
  }
  throw lastError;
}

function pairs(reply: unknown): Record<string, string> {
  const flat = reply as string[];
  const out: Record<string, string> = {};
  for (let i = 0; i + 1 < flat.length; i += 2) out[flat[i]!] = flat[i + 1]!;
  return out;
}

/** Master theo lời Sentinel báo (tên Docker DNS, chưa đổi sang localhost). */
export async function currentMaster(): Promise<Address> {
  return withSentinel(async (sentinel) => {
    const reply = (await sentinel.call("SENTINEL", "get-master-addr-by-name", MASTER_NAME)) as
      [string, string] | null;
    if (reply === null) throw new Error(`sentinel không biết master ${MASTER_NAME}`);
    return { host: reply[0], port: Number(reply[1]) };
  });
}

export interface ReplicaState extends Address {
  /** Cờ Sentinel của replica, ví dụ "slave" hoặc "slave,s_down,disconnected". */
  flags: string;
  /** Trạng thái link replication tới master do chính replica báo ("ok" là khỏe). */
  linkStatus: string;
  /** Host của master mà replica này nói là nó đang theo. */
  masterHost: string;
}

export interface Topology {
  master: Address;
  masterFlags: string;
  replicas: ReplicaState[];
  /** Số sentinel thấy nhau, tính cả sentinel được hỏi. */
  sentinels: number;
}

/** Điều một sentinel tin về master, các replica và các sentinel khác. */
export async function readTopology(): Promise<Topology> {
  return withSentinel(async (sentinel) => {
    const master = pairs(await sentinel.call("SENTINEL", "master", MASTER_NAME));
    const replicas = ((await sentinel.call("SENTINEL", "replicas", MASTER_NAME)) as unknown[]).map(
      (r) => pairs(r),
    );
    return {
      master: { host: master["ip"]!, port: Number(master["port"]) },
      masterFlags: master["flags"] ?? "",
      replicas: replicas.map((r) => ({
        host: r["ip"]!,
        port: Number(r["port"]),
        flags: r["flags"] ?? "",
        linkStatus: r["master-link-status"] ?? "",
        masterHost: r["master-host"] ?? "",
      })),
      sentinels: Number(master["num-other-sentinels"] ?? 0) + 1,
    };
  });
}

/** Khỏe nghĩa là: một master bình thường, hai replica bình thường đã nối tới đúng master đó, và đủ ba sentinel. */
export function isHealthy(topology: Topology): boolean {
  return (
    topology.masterFlags === "master" &&
    topology.sentinels === 3 &&
    topology.replicas.length === 2 &&
    topology.replicas.every(
      (r) => r.flags === "slave" && r.linkStatus === "ok" && r.masterHost === topology.master.host,
    )
  );
}
