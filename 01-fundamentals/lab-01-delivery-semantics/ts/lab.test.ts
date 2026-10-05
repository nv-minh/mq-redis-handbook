import { describe, expect, it } from "vitest";
import { uniqueName } from "@handbook/testkit";
import { createIdempotentHandler, createQueue } from "./lab.js";

/** A rand() that replays a fixed script, so a "random" loss is decided by the test. */
function scripted(values: number[]): () => number {
  let i = 0;
  return () => {
    const value = values[i++];
    if (value === undefined) throw new Error("scripted rand exhausted");
    return value;
  };
}

describe("lab-01 delivery semantics", () => {
  it("at_most_once_loses_messages_when_consumer_crashes", async () => {
    const [m1, m2, m3] = [uniqueName("m1"), uniqueName("m2"), uniqueName("m3")];
    const queue = createQueue({ mode: "at-most-once", lossRate: 0 });
    const attempts: string[] = [];
    const processed: string[] = [];
    queue.consume((id) => {
      attempts.push(id);
      // The consumer crashes while handling m2: the handler throws before finishing.
      if (id === m2) throw new Error("consumer crashed");
      processed.push(id);
    });

    for (const id of [m1, m2, m3]) queue.publish(id);
    await queue.drain();

    // m2 was handed over once and never redelivered, so its work is lost for good.
    expect(attempts).toEqual([m1, m2, m3]);
    expect(processed).toEqual([m1, m3]);
    expect(queue.deadLetters()).toEqual([]);
  });

  it("at_most_once_drops_every_delivery_when_loss_rate_is_one", async () => {
    const queue = createQueue({ mode: "at-most-once", lossRate: 1 });
    const seen: string[] = [];
    queue.consume((id) => seen.push(id));
    for (const id of ["a", "b", "c"]) queue.publish(id);
    await queue.drain();
    expect(seen).toEqual([]);
  });

  it("at_least_once_redelivers_when_consumer_crashes", async () => {
    const id = uniqueName("m");
    const queue = createQueue({ mode: "at-least-once", lossRate: 0 });
    let attempts = 0;
    const processed: string[] = [];
    queue.consume((got) => {
      attempts++;
      if (attempts === 1) throw new Error("consumer crashed"); // no ack, so the broker redelivers
      processed.push(got);
    });
    queue.publish(id);
    await queue.drain();
    expect(attempts).toBe(2);
    expect(processed).toEqual([id]);
  });

  it("at_least_once_duplicates_when_ack_is_lost", async () => {
    // lossRate = 1: every ack is lost, so every delivery is retried until maxDeliveries.
    const queue = createQueue({ mode: "at-least-once", lossRate: 1, maxDeliveries: 3 });
    const [a, b] = [uniqueName("a"), uniqueName("b")];
    const seen: string[] = [];
    queue.consume((id) => seen.push(id));
    queue.publish(a);
    queue.publish(b);
    await queue.drain();

    expect(seen.filter((id) => id === a)).toHaveLength(3);
    expect(seen.filter((id) => id === b)).toHaveLength(3);
    // The handler did run to completion each time; only the ack went missing.
    // After maxDeliveries the broker gives up and parks the message as dead.
    expect(queue.deadLetters().sort()).toEqual([a, b].sort());
  });

  it("at_least_once_stops_redelivering_once_an_ack_gets_through", async () => {
    // First ack lost (0 < 0.5), second ack arrives (0.9 >= 0.5): exactly one duplicate.
    const queue = createQueue({
      mode: "at-least-once",
      lossRate: 0.5,
      rand: scripted([0, 0.9]),
    });
    const id = uniqueName("m");
    const seen: string[] = [];
    queue.consume((got) => seen.push(got));
    queue.publish(id);
    await queue.drain();
    expect(seen).toEqual([id, id]);
    expect(queue.deadLetters()).toEqual([]);
  });

  it("idempotent_consumer_applies_each_id_once", async () => {
    const queue = createQueue({ mode: "at-least-once", lossRate: 1, maxDeliveries: 4 });
    const ids = [uniqueName("a"), uniqueName("b"), uniqueName("c")];
    const applied: string[] = [];
    let deliveries = 0;
    const handler = createIdempotentHandler((id) => applied.push(id));
    queue.consume((id) => {
      deliveries++;
      handler(id);
    });
    for (const id of ids) queue.publish(id);
    await queue.drain();

    expect(deliveries).toBe(ids.length * 4); // duplicates really arrived
    expect(applied.sort()).toEqual([...ids].sort()); // yet each id was applied once
  });

  it("idempotent_consumer_retries_an_id_whose_apply_failed", () => {
    let calls = 0;
    const applied: string[] = [];
    const handler = createIdempotentHandler((id) => {
      calls++;
      if (calls === 1) throw new Error("side effect failed");
      applied.push(id);
    });
    expect(() => handler("x")).toThrow("side effect failed");
    handler("x"); // not treated as a duplicate: the first attempt never completed
    handler("x"); // now it is a duplicate
    expect(applied).toEqual(["x"]);
    expect(calls).toBe(2);
  });
});
