import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { eventually, uniqueName, waitForPort } from "@handbook/testkit";
import {
  connectViaSentinel,
  currentMaster,
  hostAddress,
  isHealthy,
  readTopology,
  SENTINELS,
  type Address,
} from "./lab.js";
import { startService, stopService } from "./chaos.js";

const HINT =
  'Redis Sentinel is not up. Start both topologies with: make up PROFILE="sentinel cluster"';

// Fail fast with a clear message when the sentinel profile is not running.
beforeAll(async () => {
  try {
    for (const { port } of SENTINELS) await waitForPort("127.0.0.1", port, 3_000);
    for (const port of [6380, 6381, 6382]) await waitForPort("127.0.0.1", port, 3_000);
  } catch (error) {
    throw new Error(`${error instanceof Error ? error.message : String(error)}\n${HINT}`, {
      cause: error,
    });
  }
}, 30_000);

/** Wait until Sentinel sees 1 master + 2 replicas, all reachable, and 3 sentinels. */
async function waitForHealthyTopology(timeoutMs = 90_000): Promise<void> {
  await eventually(async () => isHealthy(await readTopology()), { timeoutMs, intervalMs: 500 });
}

describe("lab-01 sentinel: client discovery", () => {
  const clients: Redis[] = [];
  const keys: string[] = [];
  afterEach(async () => {
    const [client] = clients;
    if (client !== undefined && keys.length > 0) await client.del(...keys.splice(0));
    for (const c of clients.splice(0)) c.disconnect();
  });

  it("client_writes_through_the_master_that_sentinel_reports", async () => {
    const client = connectViaSentinel();
    clients.push(client);
    const key = uniqueName("lab04-sentinel");
    keys.push(key);

    await client.set(key, "hello");
    expect(await client.get(key)).toBe("hello");

    // The connection must be the master and its address must be the one Sentinel announces.
    const role = (await client.role()) as unknown[];
    expect(role[0]).toBe("master");
    const master = hostAddress(await currentMaster());
    const direct = new Redis({ host: master.host, port: master.port });
    clients.push(direct);
    expect(await direct.get(key)).toBe("hello");
  });

  it("sentinel_sees_one_master_and_two_replicas", async () => {
    await waitForHealthyTopology();
    const topology = await readTopology();
    expect(topology.replicas).toHaveLength(2);
    expect(topology.sentinels).toBe(3);
    expect(
      new Set([topology.master, ...topology.replicas].map((a) => `${a.host}:${a.port}`)).size,
    ).toBe(3);
  });
});

describe("chaos", () => {
  // Every service this file stops is restored here, even when the test failed half way.
  const stopped: string[] = [];
  const clients: Redis[] = [];
  const keys: string[] = [];

  async function stopCurrentMaster(): Promise<Address> {
    const master = await currentMaster();
    stopped.push(master.host); // announced host == compose service name
    await stopService(master.host);
    return master;
  }

  afterEach(async () => {
    const errors: unknown[] = [];
    for (const service of stopped.splice(0).reverse()) {
      try {
        await startService(service);
      } catch (error) {
        errors.push(error);
      }
    }
    await waitForHealthyTopology();
    // Keys live on whichever node is master now.
    if (keys.length > 0) {
      const cleaner = connectViaSentinel();
      try {
        await cleaner.del(...keys.splice(0));
      } finally {
        cleaner.disconnect();
      }
    }
    for (const c of clients.splice(0)) c.disconnect();
    if (errors.length > 0) throw errors[0];
  }, 180_000);

  afterAll(async () => {
    await waitForHealthyTopology();
  }, 120_000);

  it("chaos_client_writes_succeed_within_30s_after_master_is_stopped", async () => {
    // commandTimeout keeps one attempt from hanging in ioredis' offline queue past the budget.
    const client = connectViaSentinel({ commandTimeout: 2_000 });
    clients.push(client);
    // Connection errors during the failover are expected: count them instead of letting ioredis log each one.
    let connectionErrors = 0;
    client.on("error", () => (connectionErrors += 1));
    const key = uniqueName("lab04-sentinel-chaos");
    keys.push(key);
    await client.set(key, "before");

    const oldMaster = await stopCurrentMaster();
    const stoppedAt = Date.now();
    await eventually(
      async () => {
        try {
          return (await client.set(key, "after")) === "OK";
        } catch {
          return false;
        }
      },
      { timeoutMs: 30_000, intervalMs: 100 },
    );
    const elapsedMs = Date.now() - stoppedAt;
    console.log(
      `failover measured: first successful write ${elapsedMs} ms after the master stopped (${connectionErrors} connection errors seen by the client)`,
    );
    expect(elapsedMs).toBeLessThan(30_000);

    // The write went to a different node than the one that was stopped.
    const newMaster = await eventually(
      async () => {
        const m = await currentMaster();
        return m.host !== oldMaster.host ? m : undefined;
      },
      { timeoutMs: 10_000 },
    );
    const direct = new Redis({ ...hostAddress(newMaster), commandTimeout: 2_000 });
    clients.push(direct);
    expect(await direct.get(key)).toBe("after");
  }, 90_000);

  it("chaos_old_master_rejoins_as_replica", async () => {
    const oldMaster = await stopCurrentMaster();
    const newMaster = await eventually(
      async () => {
        const m = await currentMaster();
        return m.host !== oldMaster.host ? m : undefined;
      },
      { timeoutMs: 30_000, intervalMs: 200 },
    );

    // Bring the old master back. redis-master starts as a master (no --replicaof) and Sentinel
    // demotes it; a replica container starts with --replicaof redis-master and Sentinel repoints it.
    // Either way it must end up as a replica of the NEW master.
    await startService(oldMaster.host);
    stopped.length = 0;
    const rejoined = await eventually(
      async () => {
        const node = new Redis({
          ...hostAddress(oldMaster),
          commandTimeout: 2_000,
          lazyConnect: true,
        });
        try {
          await node.connect();
          const role = (await node.role()) as unknown[];
          return role[0] === "slave" && String(role[1]) === newMaster.host ? role : undefined;
        } catch {
          return undefined;
        } finally {
          node.disconnect();
        }
      },
      { timeoutMs: 60_000, intervalMs: 500 },
    );
    // ROLE of a replica: ["slave", masterHost, masterPort, state, offset]
    expect(rejoined[0]).toBe("slave");
    expect(String(rejoined[1])).toBe(newMaster.host);
    expect(Number(rejoined[2])).toBe(newMaster.port);

    await waitForHealthyTopology();
    const topology = await readTopology();
    expect(topology.replicas.map((a) => a.host)).toContain(oldMaster.host);
    expect(topology.master.host).toBe(newMaster.host);
  }, 150_000);
});
