import { afterAll, afterEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { eventually, uniqueName } from "@handbook/testkit";
import { numSubscribers, publish, subscribe, type Subscription } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
// One connection for normal commands (PUBLISH, PUBSUB NUMSUB). Every subscriber gets its own.
const publisher = new Redis(url);
const opened: Redis[] = [];
const subscriptions: Subscription[] = [];

/** A subscriber on a dedicated connection, closed again in afterEach. */
async function newSubscriber(channel: string): Promise<Subscription> {
  const connection = new Redis(url);
  opened.push(connection);
  const subscription = await subscribe(connection, channel);
  subscriptions.push(subscription);
  return subscription;
}

afterEach(async () => {
  for (const subscription of subscriptions.splice(0)) await subscription.close();
  for (const connection of opened.splice(0)) connection.disconnect();
});

afterAll(() => {
  publisher.disconnect();
});

describe("lab-01 pubsub: fire and forget", () => {
  it("subscriber_misses_messages_published_while_offline", async () => {
    const channel = uniqueName("lab03-offline");

    // Nobody is subscribed: PUBLISH replies with the number of receivers, which is 0,
    // and Redis does not keep the message anywhere.
    expect(await publish(publisher, channel, "offline-1")).toBe(0);
    expect(await publish(publisher, channel, "offline-2")).toBe(0);
    expect(await publish(publisher, channel, "offline-3")).toBe(0);

    const subscriber = await newSubscriber(channel);
    // The subscribe confirmation is already in, and the server agrees: one subscriber.
    await eventually(async () => (await numSubscribers(publisher, channel)) === 1, {
      timeoutMs: 5000,
    });

    // Now there is exactly one receiver, so this one is delivered.
    expect(await publish(publisher, channel, "online-1")).toBe(1);
    await eventually(async () => subscriber.messages().length >= 1, { timeoutMs: 5000 });

    // The three old messages never arrive: Pub/Sub has no history, the late subscriber only
    // sees what is published after it subscribed. Order on one connection means a stray old
    // message would have arrived before "online-1".
    expect(subscriber.messages()).toEqual(["online-1"]);
  });

  it("all_subscribers_receive_each_message", async () => {
    const channel = uniqueName("lab03-fanout");
    const total = 20;
    const subscribers = [
      await newSubscriber(channel),
      await newSubscriber(channel),
      await newSubscriber(channel),
    ];
    await eventually(async () => (await numSubscribers(publisher, channel)) === 3, {
      timeoutMs: 5000,
    });

    const expected = Array.from({ length: total }, (_, i) => `msg-${i}`);
    for (const message of expected) {
      // Fan-out: every PUBLISH reaches all 3 subscribers.
      expect(await publish(publisher, channel, message)).toBe(3);
    }

    for (const subscriber of subscribers) {
      await eventually(async () => subscriber.messages().length >= total, { timeoutMs: 5000 });
      // Each subscriber got every message, in publish order, exactly once.
      expect(subscriber.messages()).toEqual(expected);
    }
  });
});
