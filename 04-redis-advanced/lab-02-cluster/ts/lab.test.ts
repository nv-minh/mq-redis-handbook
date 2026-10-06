import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { Redis, type Cluster } from "ioredis";
import { uniqueName, waitForPort } from "@handbook/testkit";
import { CLUSTER_PORTS, connectCluster, hostAddress, slotFor, slotOwner } from "./lab.js";

const HINT =
  'Redis Cluster chưa chạy. Hãy bật cả hai topology bằng lệnh: make up PROFILE="sentinel cluster"';

// Fail fast: nếu profile cluster chưa chạy thì dừng ngay và nói rõ cần bật gì, thay vì treo tới timeout.
beforeAll(async () => {
  try {
    for (const port of CLUSTER_PORTS) await waitForPort("127.0.0.1", port, 3_000);
  } catch (error) {
    throw new Error(`${error instanceof Error ? error.message : String(error)}\n${HINT}`, {
      cause: error,
    });
  }
}, 30_000);

// Connection trực tiếp tới MỘT node, không đi qua cluster client nên không tự theo MOVED.
// protocol 2 giữ reply của CLUSTER SLOTS ở dạng mảng lồng nhau thuần.
const direct = (port: number) => new Redis({ host: "127.0.0.1", port, protocol: 2 });

const nodes: Redis[] = [];
const clusters: Cluster[] = [];
const createdKeys: string[] = [];

async function cleanup(): Promise<void> {
  // DEL nhiều key khác slot sẽ lỗi CROSSSLOT, nên xóa từng key một qua cluster client (nó tự tìm đúng master).
  if (createdKeys.length > 0) {
    const cluster = connectCluster();
    try {
      for (const key of createdKeys.splice(0)) await cluster.del(key);
    } finally {
      cluster.disconnect();
    }
  }
  for (const c of clusters.splice(0)) c.disconnect();
  for (const n of nodes.splice(0)) n.disconnect();
}

afterEach(cleanup);
afterAll(cleanup);

describe("lab-02 cluster: hash slots", () => {
  it("slot_for_matches_cluster_keyslot", async () => {
    const node = direct(7001);
    nodes.push(node);
    const keys = [
      "foo",
      "bar",
      "123456789",
      "user:1000",
      "",
      "a",
      // quy tắc hash tag theo cluster spec
      "{user1000}.following",
      "{user1000}.followers",
      "foo{}{bar}", // tag rỗng: băm cả key
      "foo{{bar}}zap", // tag là "{bar"
      "foo{bar}{zap}", // tag đầu tiên thắng: "bar"
      "{}foo", // key bắt đầu bằng {} thì băm cả key
      "foo{bar", // không có dấu } đóng: băm cả key
      "foo}bar{", // dấu } đứng trước dấu {
      "{",
      "}",
      "{}",
      "{{}}", // tag là "{"
      "{a}",
      "a{b}c{d}e",
      // không phải ASCII: hash chạy trên byte UTF-8, không phải trên đơn vị UTF-16 của chuỗi JavaScript
      "khóa",
      "日本語",
      "{日本}語",
      "café:{ünï}",
      "🔑",
      "k🔑{🔑}",
    ];
    for (const key of keys) {
      const expected = Number(await node.call("CLUSTER", "KEYSLOT", key));
      expect(slotFor(key), `slotFor(${JSON.stringify(key)})`).toBe(expected);
    }
    // Check value của CRC16/XMODEM theo cluster spec: CRC16("123456789") = 0x31C3.
    expect(slotFor("123456789")).toBe(0x31c3 % 16384);
  });

  it("hash_tag_rules_follow_the_cluster_spec", () => {
    expect(slotFor("{user1000}.following")).toBe(slotFor("{user1000}.followers"));
    expect(slotFor("{user1000}.following")).toBe(slotFor("user1000"));
    expect(slotFor("foo{bar}{zap}")).toBe(slotFor("bar"));
    expect(slotFor("foo{{bar}}zap")).toBe(slotFor("{bar"));
    // Tag rỗng, tag không đóng và key bắt đầu bằng {} đều băm cả key, nên slot khác slot của phần trong ngoặc.
    expect(slotFor("foo{}{bar}")).not.toBe(slotFor("bar"));
    expect(slotFor("foo{bar")).not.toBe(slotFor("bar"));
    expect(slotFor("{}foo")).not.toBe(slotFor("foo"));
    expect(slotFor("x")).toBeGreaterThanOrEqual(0);
    expect(slotFor("x")).toBeLessThan(16384);
  });

  it("keys_with_same_hash_tag_share_a_slot", async () => {
    const node = direct(7001);
    nodes.push(node);
    const tag = uniqueName("lab04-tag");
    const sameTag = [`{${tag}}:pending`, `{${tag}}:processing`, `{${tag}}:dead`];
    for (const key of sameTag) {
      expect(Number(await node.call("CLUSTER", "KEYSLOT", key))).toBe(slotFor(`{${tag}}`));
      expect(slotFor(key)).toBe(slotFor(`{${tag}}`));
    }
    // Không có hash tag thì các key cùng hậu tố vẫn rải ra nhiều slot (tên cố định, đã biết là khác nhau).
    expect(slotFor("{alpha}:pending")).not.toBe(slotFor("{beta}:pending"));
    expect(slotFor("alpha:pending")).not.toBe(slotFor("alpha:processing"));

    // Lệnh multi-key trên các key cùng một slot chạy được qua cluster client.
    const cluster = connectCluster();
    clusters.push(cluster);
    createdKeys.push(...sameTag);
    expect(await cluster.mset(sameTag[0]!, "1", sameTag[1]!, "2", sameTag[2]!, "3")).toBe("OK");
    expect(await cluster.mget(...sameTag)).toEqual(["1", "2", "3"]);
  });

  it("multi_key_command_across_slots_fails_with_crossslot", async () => {
    // Hai key khác slot (tăng hậu tố tới khi slot khác nhau).
    const prefix = uniqueName("lab04-cross");
    const a = `${prefix}-a`;
    let b = `${prefix}-b`;
    for (let i = 0; slotFor(a) === slotFor(b); i++) b = `${prefix}-b${i}`;
    expect(slotFor(a)).not.toBe(slotFor(b));
    createdKeys.push(a, b);

    // Gửi MSET tới MỘT node qua connection trực tiếp: server kiểm tra CROSSSLOT trước MOVED,
    // nên lỗi này đến từ bất kỳ node nào kể cả node không sở hữu slot của key.
    const node = direct(7001);
    nodes.push(node);
    const error = await node.mset(a, "1", b, "2").then(
      () => undefined,
      (e: unknown) => e as Error,
    );
    expect(error).toBeInstanceOf(Error);
    expect(error?.message.startsWith("CROSSSLOT")).toBe(true);

    // Cùng kiểu key nhưng chung một hash tag thì thành công trên node sở hữu slot đó.
    const tag = uniqueName("lab04-tagged");
    const tagged = [`{${tag}}:a`, `{${tag}}:b`];
    createdKeys.push(...tagged);
    const slot = slotFor(tagged[0]!);
    const owner = await slotOwner(slot);
    const ownerNode = direct(hostAddress(owner).port);
    nodes.push(ownerNode);
    expect(await ownerNode.mset(tagged[0]!, "1", tagged[1]!, "2")).toBe("OK");
    expect(await ownerNode.mget(...tagged)).toEqual(["1", "2"]);

    // Node khác (master của shard khác hoặc replica) trả MOVED cho lệnh ghi vào slot này,
    // kèm địa chỉ Docker DNS của node sở hữu.
    const otherPort = CLUSTER_PORTS.find((p) => p !== hostAddress(owner).port)!;
    const stranger = direct(otherPort);
    nodes.push(stranger);
    const moved = await stranger.mset(tagged[0]!, "1", tagged[1]!, "2").then(
      () => undefined,
      (e: unknown) => e as Error,
    );
    expect(moved?.message.startsWith("MOVED")).toBe(true);
  });

  it("cluster_client_writes_keys_spread_over_all_masters", async () => {
    // Chứng minh natMap: sau CLUSTER SLOTS client dial redis-cluster-N:700N, tên này phải được đổi về localhost.
    // 60 key rải trên cả 3 master nên client buộc phải nối tới đủ ba node.
    const cluster = connectCluster();
    clusters.push(cluster);
    const prefix = uniqueName("lab04-spread");
    const owners = new Set<string>();
    for (let i = 0; i < 60; i++) {
      const key = `${prefix}:${i}`;
      createdKeys.push(key);
      expect(await cluster.set(key, String(i))).toBe("OK");
      owners.add((await slotOwner(slotFor(key))).host);
    }
    for (let i = 0; i < 60; i++) expect(await cluster.get(`${prefix}:${i}`)).toBe(String(i));
    expect(owners.size).toBe(3);
  });
});
