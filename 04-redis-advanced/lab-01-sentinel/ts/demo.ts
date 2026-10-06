// Demo: write through Sentinel, stop the master, measure how long writes fail, restore the node.
// Needs `make up PROFILE="sentinel cluster"`. The node it stops is restarted at the end.
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

const client = connectViaSentinel({ commandTimeout: 2_000 });
const key = uniqueName("demo:sentinel");
let stopped: string | undefined;

try {
  await show("start");
  console.log("SET through Sentinel:", await client.set(key, "before"));

  const master = await currentMaster();
  console.log(`stopping ${master.host} (the current master)`);
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
    `first successful write ${Date.now() - stoppedAt} ms after the stop (${failed} failed attempts before it)`,
  );
  const newMaster = await currentMaster();
  console.log(`new master according to Sentinel: ${newMaster.host}:${newMaster.port}`);
  console.log("GET after failover:", await client.get(key));
} finally {
  if (stopped !== undefined) {
    console.log(`restarting ${stopped}`);
    await startService(stopped);
    await eventually(async () => isHealthy(await readTopology()), {
      timeoutMs: 90_000,
      intervalMs: 500,
    });
  }
  await show("end");
  await client.del(key).catch(() => undefined);
  client.disconnect();
}
