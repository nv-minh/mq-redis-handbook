import { Cluster, Redis } from "ioredis";

export interface Address {
  host: string;
  port: number;
}

/** Sáu node cluster như máy host nhìn thấy (port được publish 1:1 nên trùng với port trong container). */
export const CLUSTER_PORTS = [7001, 7002, 7003, 7004, 7005, 7006];

/**
 * Các node tự khai tên Docker DNS (redis-cluster-1:7001) trong CLUSTER SLOTS và trong redirect MOVED.
 * Máy host không phân giải được tên này, nên natMap đổi mọi node đã khai sang port đã publish trên localhost.
 */
export const NAT_MAP: Record<string, Address> = Object.fromEntries(
  CLUSTER_PORTS.map((port, i) => [`redis-cluster-${i + 1}:${port}`, { host: "127.0.0.1", port }]),
);

/** Đổi địa chỉ node tự khai (redis-cluster-2:7002) sang địa chỉ mà máy host dial được. */
export function hostAddress(announced: Address): Address {
  return NAT_MAP[`${announced.host}:${announced.port}`] ?? announced;
}

/** Cluster client tự theo MOVED và làm mới slot map, kèm natMap để chạy được với cluster trong Docker. */
export function connectCluster(): Cluster {
  return new Cluster(
    CLUSTER_PORTS.slice(0, 3).map((port) => ({ host: "127.0.0.1", port })),
    { natMap: NAT_MAP },
  );
}

const SLOTS = 16384;

// CRC16/XMODEM (poly 0x1021, init 0, không reflect, xorout 0), biến thể mà Redis Cluster dùng.
const CRC_TABLE: number[] = Array.from({ length: 256 }, (_, byte) => {
  let crc = byte << 8;
  for (let bit = 0; bit < 8; bit++)
    crc = crc & 0x8000 ? ((crc << 1) ^ 0x1021) & 0xffff : (crc << 1) & 0xffff;
  return crc;
});

function crc16(bytes: Uint8Array): number {
  let crc = 0;
  for (const b of bytes) crc = ((crc << 8) & 0xffff) ^ CRC_TABLE[((crc >> 8) ^ b) & 0xff]!;
  return crc;
}

/**
 * Phần của key được đem đi băm.
 * Nếu key có `{`, có `}` đứng sau nó, và có ít nhất một byte giữa `{` ĐẦU TIÊN và `}` đầu tiên sau nó
 * thì chỉ phần đó được băm.
 * Ngược lại (không có `{`, không có `}` phía sau, hoặc tag rỗng `{}`) thì băm cả key.
 * Làm việc trên byte như Redis: `{` và `}` là một byte trong UTF-8,
 * nên tìm trên mảng byte hay trên chuỗi đều ra cùng một tag.
 */
function hashPart(key: Uint8Array): Uint8Array {
  const open = key.indexOf(0x7b); // {
  if (open === -1) return key;
  const close = key.indexOf(0x7d, open + 1); // } đứng sau { đầu tiên
  if (close === -1 || close === open + 1) return key;
  return key.subarray(open + 1, close);
}

/** Hash slot của một key: CRC16(phần được băm) mod 16384, cùng giá trị với CLUSTER KEYSLOT. */
export function slotFor(key: string): number {
  return crc16(hashPart(Buffer.from(key, "utf8"))) % SLOTS;
}

/** Master sở hữu `slot`, theo lời cluster báo (tên Docker DNS, chưa đổi sang localhost). */
export async function slotOwner(slot: number): Promise<Address> {
  let lastError: unknown = new Error("chưa cấu hình node cluster nào");
  for (const port of CLUSTER_PORTS) {
    // protocol 2: CLUSTER SLOTS trả mảng lồng nhau thuần.
    const node = new Redis({
      host: "127.0.0.1",
      port,
      protocol: 2,
      lazyConnect: true,
      retryStrategy: null,
      connectTimeout: 1_000,
      commandTimeout: 2_000,
    });
    node.on("error", () => undefined);
    try {
      await node.connect();
      const ranges = (await node.call("CLUSTER", "SLOTS")) as [number, number, [string, number]][];
      for (const [start, end, master] of ranges) {
        if (slot >= start && slot <= end) return { host: master[0], port: Number(master[1]) };
      }
      throw new Error(`slot ${slot} chưa có master nào sở hữu`);
    } catch (error) {
      lastError = error;
    } finally {
      node.disconnect();
    }
  }
  throw lastError;
}
