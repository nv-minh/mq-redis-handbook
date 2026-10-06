// Demo: hash slot và hash tag trên cluster Docker, và lỗi CROSSSLOT.
// Cần `make up PROFILE="sentinel cluster"`. Các key tạo ra được xóa ở cuối.
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
    `${JSON.stringify(key).padEnd(34)} slotFor=${String(slot).padEnd(5)} CLUSTER KEYSLOT=${String(keyslot).padEnd(5)} master=${owner.host}:${owner.port} -> localhost:${hostAddress(owner).port}`,
  );
};

console.log("Demo Redis Cluster: hash slot, hash tag và CROSSSLOT.");
try {
  console.log("--- Quy tắc hash tag (slotFor tự cài phải bằng CLUSTER KEYSLOT của server) ---");
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
  console.log(
    "Nhận xét: hai key user1000 cùng slot nhờ hash tag, còn tag rỗng `{}` thì băm cả key.",
  );

  console.log("--- MSET hai key khác slot gửi tới MỘT node ---");
  const prefix = uniqueName("demo:cross");
  const a = `${prefix}-a`;
  let b = `${prefix}-b`;
  for (let i = 0; slotFor(a) === slotFor(b); i++) b = `${prefix}-b${i}`;
  created.push(a, b);
  console.log(
    `MSET ${a} ${b} (slot ${slotFor(a)} và ${slotFor(b)}) ->`,
    await node.mset(a, "1", b, "2").catch((e: Error) => e.message),
  );
  console.log("Nhận xét: server từ chối vì các key không cùng slot.");

  console.log("--- Cùng kiểu key nhưng chung hash tag, qua cluster client ---");
  const tag = uniqueName("demo:tag");
  const tagged = [`{${tag}}:pending`, `{${tag}}:processing`];
  created.push(...tagged);
  console.log(`MSET ${tagged.join(" ")} ->`, await cluster.mset(tagged[0]!, "1", tagged[1]!, "2"));
  console.log("MGET ->", await cluster.mget(...tagged));
  console.log(
    "Nhận xét: chung hash tag nên cùng slot, lệnh multi-key chạy được (cách làm cho reliable queue BLMOVE).",
  );
} finally {
  for (const key of created) await cluster.del(key).catch(() => undefined);
  cluster.disconnect();
  node.disconnect();
}
