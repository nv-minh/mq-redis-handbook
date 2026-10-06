// Demo: hash slots and hash tags on the Docker cluster, and the CROSSSLOT error.
// Needs `make up PROFILE="sentinel cluster"`. Keys are removed at the end.
import { Redis } from "ioredis";
import { uniqueName } from "@handbook/testkit";
import { connectCluster, hostAddress, slotFor, slotOwner } from "./lab.js";

const cluster = connectCluster();
const node = new Redis({ host: "127.0.0.1", port: 7001, protocol: 2 });
const created: string[] = [];

const describeKey = async (key: string): Promise<void> => {
  const slot = slotFor(key);
  const keyslot = Number(await node.call("CLUSTER", "KEYSLOT", key));
  const owner = await slotOwner(slot);
  console.log(
    `${JSON.stringify(key).padEnd(34)} slotFor=${String(slot).padEnd(5)} CLUSTER KEYSLOT=${String(keyslot).padEnd(5)} owner=${owner.host}:${owner.port} -> localhost:${hostAddress(owner).port}`,
  );
};

try {
  console.log("--- hash tag rules (slotFor must equal CLUSTER KEYSLOT) ---");
  for (const key of [
    "foo",
    "{user1000}.following",
    "{user1000}.followers",
    "foo{}{bar}",
    "foo{{bar}}zap",
    "foo{bar}{zap}",
    "日本語",
  ]) {
    await describeKey(key);
  }

  console.log("--- MSET across slots on ONE node ---");
  const prefix = uniqueName("demo:cross");
  const a = `${prefix}-a`;
  let b = `${prefix}-b`;
  for (let i = 0; slotFor(a) === slotFor(b); i++) b = `${prefix}-b${i}`;
  created.push(a, b);
  console.log(
    `MSET ${a} ${b} (slots ${slotFor(a)} and ${slotFor(b)}) ->`,
    await node.mset(a, "1", b, "2").catch((e: Error) => e.message),
  );

  console.log("--- same keys with a shared hash tag, through the cluster client ---");
  const tag = uniqueName("demo:tag");
  const tagged = [`{${tag}}:pending`, `{${tag}}:processing`];
  created.push(...tagged);
  console.log(`MSET ${tagged.join(" ")} ->`, await cluster.mset(tagged[0]!, "1", tagged[1]!, "2"));
  console.log("MGET ->", await cluster.mget(...tagged));
} finally {
  for (const key of created) await cluster.del(key).catch(() => undefined);
  cluster.disconnect();
  node.disconnect();
}
