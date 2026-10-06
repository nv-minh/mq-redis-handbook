import { Redis, type RedisOptions } from "ioredis";

/** Name given to the monitored master in the sentinel config of infra/docker-compose.yml. */
export const MASTER_NAME = "mymaster";

export interface Address {
  host: string;
  port: number;
}

/** The three sentinels as the host reaches them (ports are published 1:1). */
export const SENTINELS: Address[] = [
  { host: "127.0.0.1", port: 26379 },
  { host: "127.0.0.1", port: 26380 },
  { host: "127.0.0.1", port: 26381 },
];

/**
 * Sentinel and the Redis nodes announce Docker DNS names (redis-master:6380), which the host
 * cannot resolve. natMap rewrites every announced address to the published port on localhost.
 * It must list the sentinels too: ioredis learns the other sentinels from SENTINEL SENTINELS
 * and would otherwise try to dial sentinel-2:26380.
 */
export const NAT_MAP: Record<string, Address> = {
  "redis-master:6380": { host: "127.0.0.1", port: 6380 },
  "redis-replica-1:6381": { host: "127.0.0.1", port: 6381 },
  "redis-replica-2:6382": { host: "127.0.0.1", port: 6382 },
  "sentinel-1:26379": { host: "127.0.0.1", port: 26379 },
  "sentinel-2:26380": { host: "127.0.0.1", port: 26380 },
  "sentinel-3:26381": { host: "127.0.0.1", port: 26381 },
};

/** Map an announced address (redis-master:6380) to the address the host can dial. */
export function hostAddress(announced: Address): Address {
  return NAT_MAP[`${announced.host}:${announced.port}`] ?? announced;
}

/**
 * A client that always talks to the current master: it asks the sentinels where the master is
 * (SENTINEL get-master-addr-by-name), connects, and asks again on every reconnect, so after a
 * failover it follows the newly promoted node without a restart. `extra` overrides any option.
 */
export function connectViaSentinel(extra: Partial<RedisOptions> = {}): Redis {
  return new Redis({
    sentinels: SENTINELS,
    name: MASTER_NAME,
    natMap: NAT_MAP,
    // Short sentinel timeouts: a stopped sentinel must not stall discovery.
    sentinelCommandTimeout: 1_000,
    connectTimeout: 2_000,
    // Retry forever with a short backoff: the client rides out the failover instead of flushing commands.
    sentinelRetryStrategy: (times) => Math.min(times * 100, 500),
    retryStrategy: (times) => Math.min(times * 100, 500),
    ...extra,
  });
}

/** Run `fn` against the first sentinel that answers. */
async function withSentinel<T>(fn: (sentinel: Redis) => Promise<T>): Promise<T> {
  let lastError: unknown = new Error("no sentinel configured");
  for (const address of SENTINELS) {
    // protocol 2: SENTINEL replies are flat arrays of field/value pairs.
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

/** The master as Sentinel announces it (a Docker DNS name, not mapped to localhost). */
export async function currentMaster(): Promise<Address> {
  return withSentinel(async (sentinel) => {
    const reply = (await sentinel.call("SENTINEL", "get-master-addr-by-name", MASTER_NAME)) as
      [string, string] | null;
    if (reply === null) throw new Error(`sentinel does not know master ${MASTER_NAME}`);
    return { host: reply[0], port: Number(reply[1]) };
  });
}

export interface ReplicaState extends Address {
  /** Sentinel flags of the replica, for example "slave" or "slave,s_down,disconnected". */
  flags: string;
  /** Replication link of the replica to its master as the replica reports it. */
  linkStatus: string;
  /** Host of the master this replica says it follows. */
  masterHost: string;
}

export interface Topology {
  master: Address;
  masterFlags: string;
  replicas: ReplicaState[];
  /** Number of sentinels that see each other, including the one asked. */
  sentinels: number;
}

/** What one sentinel believes about the master, its replicas and the other sentinels. */
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

/** Healthy means: one plain master, two plain replicas linked to it, three sentinels. */
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
