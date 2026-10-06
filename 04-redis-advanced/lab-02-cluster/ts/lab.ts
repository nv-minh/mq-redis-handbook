import { Cluster, Redis } from "ioredis";

export interface Address {
  host: string;
  port: number;
}

/** The six cluster nodes as the host reaches them (ports are published 1:1). */
export const CLUSTER_PORTS = [7001, 7002, 7003, 7004, 7005, 7006];

/**
 * Nodes announce their Docker DNS name (redis-cluster-1:7001) in CLUSTER SLOTS and in MOVED
 * redirections. The host cannot resolve it, so natMap rewrites every announced node to the
 * published port on localhost.
 */
export const NAT_MAP: Record<string, Address> = Object.fromEntries(
  CLUSTER_PORTS.map((port, i) => [`redis-cluster-${i + 1}:${port}`, { host: "127.0.0.1", port }]),
);

/** Map an announced address (redis-cluster-2:7002) to the address the host can dial. */
export function hostAddress(announced: Address): Address {
  return NAT_MAP[`${announced.host}:${announced.port}`] ?? announced;
}

/** A cluster client that follows MOVED and refreshes its slot map, with natMap for Docker. */
export function connectCluster(): Cluster {
  return new Cluster(
    CLUSTER_PORTS.slice(0, 3).map((port) => ({ host: "127.0.0.1", port })),
    { natMap: NAT_MAP },
  );
}

const SLOTS = 16384;

// CRC16/XMODEM (poly 0x1021, init 0, no reflection, xorout 0), the variant Redis Cluster uses.
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
 * The part of the key that is hashed. If the key has a `{`, a `}` after it, and at least one
 * byte between the FIRST `{` and the first `}` after it, only that part is hashed.
 * Otherwise (no `{`, no `}` after it, or `{}`) the whole key is hashed.
 * Works on bytes, like Redis: `{` and `}` are single bytes in UTF-8, so searching the byte
 * array and searching the string find the same tag.
 */
function hashPart(key: Uint8Array): Uint8Array {
  const open = key.indexOf(0x7b); // {
  if (open === -1) return key;
  const close = key.indexOf(0x7d, open + 1); // } after the first {
  if (close === -1 || close === open + 1) return key;
  return key.subarray(open + 1, close);
}

/** Hash slot of a key: CRC16(hashed part) mod 16384, the same value as CLUSTER KEYSLOT. */
export function slotFor(key: string): number {
  return crc16(hashPart(Buffer.from(key, "utf8"))) % SLOTS;
}

/** The master that owns `slot`, as the cluster announces it (a Docker DNS name, not mapped). */
export async function slotOwner(slot: number): Promise<Address> {
  let lastError: unknown = new Error("no cluster node configured");
  for (const port of CLUSTER_PORTS) {
    // protocol 2: CLUSTER SLOTS replies as plain nested arrays.
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
      throw new Error(`slot ${slot} is not covered by any master`);
    } catch (error) {
      lastError = error;
    } finally {
      node.disconnect();
    }
  }
  throw lastError;
}
