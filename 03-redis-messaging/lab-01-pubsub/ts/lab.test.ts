import { afterAll, afterEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { eventually, uniqueName } from "@handbook/testkit";
import { numSubscribers, publish, subscribe, type Subscription } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
// Một connection cho các lệnh thường (PUBLISH, PUBSUB NUMSUB). Mỗi subscriber có connection riêng.
const publisher = new Redis(url);
const opened: Redis[] = [];
const subscriptions: Subscription[] = [];

/** Một subscriber trên connection riêng, được đóng lại trong afterEach. */
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

    // Chưa ai subscribe: PUBLISH trả về số receiver, tức là 0,
    // và Redis không giữ message ở đâu cả.
    expect(await publish(publisher, channel, "offline-1")).toBe(0);
    expect(await publish(publisher, channel, "offline-2")).toBe(0);
    expect(await publish(publisher, channel, "offline-3")).toBe(0);

    const subscriber = await newSubscriber(channel);
    // Xác nhận subscribe đã về, và server cũng đồng ý: có một subscriber.
    await eventually(async () => (await numSubscribers(publisher, channel)) === 1, {
      timeoutMs: 5000,
    });

    // Giờ có đúng một receiver, nên message này được giao.
    expect(await publish(publisher, channel, "online-1")).toBe(1);
    await eventually(async () => subscriber.messages().length >= 1, { timeoutMs: 5000 });

    // Ba message cũ không bao giờ tới: Pub/Sub không có lịch sử, subscriber vào muộn chỉ
    // thấy những gì được publish sau khi nó subscribe. Thứ tự trên một connection nghĩa là một
    // message cũ lạc tới sẽ đến trước "online-1".
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
      // Fan-out: mỗi PUBLISH tới cả 3 subscriber.
      expect(await publish(publisher, channel, message)).toBe(3);
    }

    for (const subscriber of subscribers) {
      await eventually(async () => subscriber.messages().length >= total, { timeoutMs: 5000 });
      // Mỗi subscriber nhận đủ mọi message, theo thứ tự publish, đúng một lần.
      expect(subscriber.messages()).toEqual(expected);
    }
  });
});
