import { afterAll, afterEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { uniqueName } from "@handbook/testkit";
import { Leaderboard } from "./lab.js";

const redis = new Redis(process.env.REDIS_URL ?? "redis://127.0.0.1:6379");
const createdKeys: string[] = [];

/** A leaderboard on a unique key, removed again in afterEach. */
function newBoard(): Leaderboard {
  const key = uniqueName("lab02-leaderboard");
  createdKeys.push(key);
  return new Leaderboard(redis, key);
}

afterEach(async () => {
  if (createdKeys.length > 0) await redis.del(...createdKeys.splice(0));
});

afterAll(() => {
  redis.disconnect();
});

describe("lab-01 datatypes: leaderboard on a sorted set", () => {
  it("top3_returns_highest_scores_in_order", async () => {
    const board = newBoard();
    await board.add("alice", 50);
    await board.add("bob", 90);
    await board.add("carol", 70);
    await board.add("dave", 10);
    await board.add("erin", 80);

    // Highest score first, and only the three requested entries.
    expect(await board.top(3)).toEqual(["bob", "erin", "carol"]);
  });

  it("top_on_empty_board_returns_empty_list", async () => {
    const board = newBoard();
    expect(await board.top(3)).toEqual([]);
  });

  it("top_returns_fewer_names_when_the_board_is_smaller_than_n", async () => {
    const board = newBoard();
    await board.add("alice", 1);
    await board.add("bob", 2);
    expect(await board.top(10)).toEqual(["bob", "alice"]);
  });

  it("top_with_non_positive_n_returns_empty_list", async () => {
    // ZRANGE key 0 -1 REV returns EVERYTHING, so n = 0 must not be turned into stop = -1.
    const board = newBoard();
    await board.add("alice", 1);
    expect(await board.top(0)).toEqual([]);
  });

  it("adding_an_existing_name_updates_its_score_instead_of_duplicating", async () => {
    const board = newBoard();
    await board.add("alice", 10);
    await board.add("bob", 20);
    await board.add("alice", 30);
    expect(await board.top(10)).toEqual(["alice", "bob"]);
  });

  it("top_with_scores_parses_the_reply_shape_of_the_client", async () => {
    const board = newBoard();
    await board.add("alice", 10.5);
    await board.add("bob", 20);
    expect(await board.topWithScores(2)).toEqual([
      { name: "bob", score: 20 },
      { name: "alice", score: 10.5 },
    ]);
  });

  it("resp3_reply_shape_of_zrange_withscores_is_measured_not_assumed", async () => {
    // ioredis 6 speaks RESP3 by default. Pin the real shapes so a client upgrade cannot change them silently.
    const key = uniqueName("lab02-shape");
    createdKeys.push(key);
    await redis.zadd(key, 10, "a", 20.5, "b");

    expect(await redis.client("INFO")).toContain("resp=3");
    // Flat array of strings: member, score, member, score (scores are NOT numbers and NOT nested).
    expect(await redis.zrange(key, 0, "-1", "REV", "WITHSCORES")).toEqual(["b", "20.5", "a", "10"]);
    expect(await redis.zscore(key, "b")).toBe("20.5");
  });
});
