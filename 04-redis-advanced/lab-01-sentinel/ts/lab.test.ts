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
  'Redis Sentinel chưa chạy. Hãy bật cả hai topology bằng lệnh: make up PROFILE="sentinel cluster"';

// Fail fast: nếu profile sentinel chưa chạy thì dừng ngay và nói rõ cần bật gì, thay vì treo tới timeout.
// Test này cần cả sentinel lẫn cluster (chủ đề 04 dùng chung một lệnh make up).
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

/** Chờ tới khi Sentinel thấy đúng 1 master, 2 replica nối được và đủ 3 sentinel (topology khỏe). */
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

    // Connection của client phải là master, và master đó phải đúng là node mà Sentinel đang báo.
    // Client chạy trên host nên địa chỉ Docker DNS (redis-master:6380) được đổi về 127.0.0.1 bằng natMap.
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
  // Mọi service mà test chaos đã dừng đều được start lại ở afterEach, kể cả khi test lỗi giữa chừng.
  // Nhờ vậy một test hỏng không để lại topology thiếu node cho test sau.
  const stopped: string[] = [];
  const clients: Redis[] = [];
  const keys: string[] = [];

  async function stopCurrentMaster(): Promise<Address> {
    const master = await currentMaster();
    // Ghi nhận TRƯỚC khi dừng để afterEach luôn khôi phục được.
    // Host mà Sentinel báo chính là tên service trong compose (redis-master, redis-replica-1, ...).
    stopped.push(master.host);
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
    // Key nằm trên node nào đang là master lúc này (có thể khác node ban đầu sau failover),
    // nên xóa qua client đi theo Sentinel.
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
    // commandTimeout giữ cho mỗi lần thử không bị treo trong offline queue của ioredis quá ngân sách 30 giây.
    const client = connectViaSentinel({ commandTimeout: 2_000 });
    clients.push(client);
    // Lỗi connection trong lúc failover là chuyện bình thường: đếm chúng thay vì để ioredis log từng lỗi.
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
      `Đo failover: lần ghi thành công đầu tiên sau ${elapsedMs} ms kể từ khi dừng master (client thấy ${connectionErrors} lỗi connection)`,
    );
    expect(elapsedMs).toBeLessThan(30_000);

    // Lần ghi mới phải nằm trên một node khác với node đã bị dừng, vì node đó đã được thay bằng replica được promote.
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

    // Bật master cũ lại.
    // Container redis-master khởi động như một master (không có --replicaof) và Sentinel sẽ hạ nó xuống làm replica.
    // Container replica khởi động với --replicaof redis-master và Sentinel trỏ nó sang master mới.
    // Dù trường hợp nào, cuối cùng node đó phải là replica của master MỚI, nên test chờ đúng điều kiện này
    // thay vì tin vào trạng thái "slave" nhất thời (có thể vẫn đang trỏ về master cũ).
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
    // ROLE của replica trả về: ["slave", masterHost, masterPort, state, offset]
    expect(rejoined[0]).toBe("slave");
    expect(String(rejoined[1])).toBe(newMaster.host);
    expect(Number(rejoined[2])).toBe(newMaster.port);

    await waitForHealthyTopology();
    const topology = await readTopology();
    expect(topology.replicas.map((a) => a.host)).toContain(oldMaster.host);
    expect(topology.master.host).toBe(newMaster.host);
  }, 150_000);
});
