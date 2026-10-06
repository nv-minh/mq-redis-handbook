import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { Redis, type Cluster } from "ioredis";
import { uniqueName, waitForPort } from "@handbook/testkit";
import { CLUSTER_PORTS, connectCluster, hostAddress, slotFor, slotOwner } from "./lab.js";

const HINT =
  'Redis Cluster is not up. Start both topologies with: make up PROFILE="sentinel cluster"';

// Fail fast with a clear message when the cluster profile is not running.
beforeAll(async () => {
  try {
    for (const port of CLUSTER_PORTS) await waitForPort("127.0.0.1", port, 3_000);
  } catch (error) {
    throw new Error(`${error instanceof Error ? error.message : String(error)}\n${HINT}`, {
      cause: error,
    });
  }
}, 30_000);

// Direct connection to ONE node. protocol 2 keeps CLUSTER SLOTS replies as plain nested arrays.
const direct = (port: number) => new Redis({ host: "127.0.0.1", port, protocol: 2 });

const nodes: Redis[] = [];
const clusters: Cluster[] = [];
const createdKeys: string[] = [];

async function cleanup(): Promise<void> {
  // DEL on a cluster is single-slot per command, so delete key by key through a cluster client.
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
      // hash tag rules from the cluster spec
      "{user1000}.following",
      "{user1000}.followers",
      "foo{}{bar}", // empty tag: the whole key is hashed
      "foo{{bar}}zap", // the tag is "{bar"
      "foo{bar}{zap}", // first tag wins: "bar"
      "{}foo", // key starting with {} hashes the whole key
      "foo{bar", // no closing brace: the whole key is hashed
      "foo}bar{", // closing brace before opening brace
      "{",
      "}",
      "{}",
      "{{}}", // tag is "{"
      "{a}",
      "a{b}c{d}e",
      // non-ASCII: the hash runs over the UTF-8 bytes, not over UTF-16 code units
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
    // CRC16/XMODEM check value from the cluster spec: CRC16("123456789") = 0x31C3.
    expect(slotFor("123456789")).toBe(0x31c3 % 16384);
  });

  it("hash_tag_rules_follow_the_cluster_spec", () => {
    expect(slotFor("{user1000}.following")).toBe(slotFor("{user1000}.followers"));
    expect(slotFor("{user1000}.following")).toBe(slotFor("user1000"));
    expect(slotFor("foo{bar}{zap}")).toBe(slotFor("bar"));
    expect(slotFor("foo{{bar}}zap")).toBe(slotFor("{bar"));
    // Empty tag, unclosed tag and a leading {} hash the whole key.
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
    // Without a tag the same suffixes spread over slots (fixed names, known to differ).
    expect(slotFor("{alpha}:pending")).not.toBe(slotFor("{beta}:pending"));
    expect(slotFor("alpha:pending")).not.toBe(slotFor("alpha:processing"));

    // A multi-key command on keys of one slot works through the cluster client.
    const cluster = connectCluster();
    clusters.push(cluster);
    createdKeys.push(...sameTag);
    expect(await cluster.mset(sameTag[0]!, "1", sameTag[1]!, "2", sameTag[2]!, "3")).toBe("OK");
    expect(await cluster.mget(...sameTag)).toEqual(["1", "2", "3"]);
  });

  it("multi_key_command_across_slots_fails_with_crossslot", async () => {
    // Two keys with different slots.
    const prefix = uniqueName("lab04-cross");
    const a = `${prefix}-a`;
    let b = `${prefix}-b`;
    for (let i = 0; slotFor(a) === slotFor(b); i++) b = `${prefix}-b${i}`;
    expect(slotFor(a)).not.toBe(slotFor(b));
    createdKeys.push(a, b);

    // Send MSET to ONE node over a direct connection: CROSSSLOT is checked before MOVED.
    const node = direct(7001);
    nodes.push(node);
    const error = await node.mset(a, "1", b, "2").then(
      () => undefined,
      (e: unknown) => e as Error,
    );
    expect(error).toBeInstanceOf(Error);
    expect(error?.message.startsWith("CROSSSLOT")).toBe(true);

    // The same keys under one hash tag succeed on the node that owns the slot.
    const tag = uniqueName("lab04-tagged");
    const tagged = [`{${tag}}:a`, `{${tag}}:b`];
    createdKeys.push(...tagged);
    const slot = slotFor(tagged[0]!);
    const owner = await slotOwner(slot);
    const ownerNode = direct(hostAddress(owner).port);
    nodes.push(ownerNode);
    expect(await ownerNode.mset(tagged[0]!, "1", tagged[1]!, "2")).toBe("OK");
    expect(await ownerNode.mget(...tagged)).toEqual(["1", "2"]);

    // Any other node (a master of another shard or a replica) answers MOVED for a write to this slot.
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
    // Proves natMap: after CLUSTER SLOTS the client dials redis-cluster-N:700N, which must map to localhost.
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
