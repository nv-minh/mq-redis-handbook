// Demo: ghi dữ liệu qua Sentinel, dừng master, đo xem ghi lỗi trong bao lâu, rồi khôi phục node.
// Cần `make up PROFILE="sentinel cluster"`. Node bị dừng sẽ được bật lại ở cuối.
import { eventually, uniqueName } from "@handbook/testkit";
import { connectViaSentinel, currentMaster, isHealthy, readTopology } from "./lab.js";
import { startService, stopService } from "./chaos.js";

const show = async (label: string): Promise<void> => {
  const t = await readTopology();
  const replicas = t.replicas.map((r) => `${r.host}:${r.port}[${r.flags}/${r.linkStatus}]`);
  console.log(
    `${label}: master=${t.master.host}:${t.master.port} replicas=${replicas.join(", ")} sentinels=${t.sentinels}`,
  );
};

console.log("Demo Sentinel failover: client đi theo master mới sau khi master cũ bị dừng.");
const client = connectViaSentinel({ commandTimeout: 2_000 });
client.on("error", () => undefined); // lỗi connection trong lúc failover là bình thường, không cần in từng lỗi
const key = uniqueName("demo:sentinel");
let stopped: string | undefined;

try {
  await show("Bắt đầu (topology khỏe)");
  console.log("SET qua Sentinel:", await client.set(key, "before"));

  const master = await currentMaster();
  console.log(`Dừng ${master.host} (master hiện tại, tìm bằng SENTINEL get-master-addr-by-name)`);
  stopped = master.host;
  await stopService(master.host);
  const stoppedAt = Date.now();
  let failed = 0;
  await eventually(
    async () => {
      try {
        return (await client.set(key, "after")) === "OK";
      } catch {
        failed += 1;
        return false;
      }
    },
    { timeoutMs: 60_000, intervalMs: 100 },
  );
  console.log(
    `Lần ghi thành công đầu tiên sau ${Date.now() - stoppedAt} ms kể từ khi dừng master (${failed} lần ghi lỗi trước đó)`,
  );
  const newMaster = await currentMaster();
  console.log(`Master mới theo Sentinel: ${newMaster.host}:${newMaster.port}`);
  console.log(
    "GET sau failover:",
    await client.get(key),
    "(dữ liệu ghi sau failover nằm trên master mới)",
  );
} finally {
  if (stopped !== undefined) {
    console.log(`Bật lại ${stopped} (nó sẽ quay về làm replica của master mới)`);
    await startService(stopped);
    await eventually(async () => isHealthy(await readTopology()), {
      timeoutMs: 90_000,
      intervalMs: 500,
    });
  }
  await show("Kết thúc (topology khỏe lại: 1 master, 2 replica, 3 sentinel)");
  await client.del(key).catch(() => undefined);
  client.disconnect();
}
