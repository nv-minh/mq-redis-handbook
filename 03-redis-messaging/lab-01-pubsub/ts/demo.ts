// Demo: Pub/Sub has no history (late subscriber misses messages) and fans out to every subscriber.
import { Redis } from "ioredis";
import { eventually, uniqueName } from "@handbook/testkit";
import { numSubscribers, publish, subscribe } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const publisher = new Redis(url);
const connections: Redis[] = [];

async function newSubscriber(channel: string) {
  const connection = new Redis(url);
  connections.push(connection);
  return subscribe(connection, channel);
}

try {
  const channel = uniqueName("demo:pubsub");

  console.log("== offline: nobody is subscribed ==");
  for (const message of ["offline-1", "offline-2", "offline-3"]) {
    console.log(`PUBLISH ${message} -> receivers = ${await publish(publisher, channel, message)}`);
  }

  console.log("== a late subscriber joins ==");
  const late = await newSubscriber(channel);
  console.log("PUBSUB NUMSUB =", await numSubscribers(publisher, channel));
  console.log(`PUBLISH online-1 -> receivers = ${await publish(publisher, channel, "online-1")}`);
  await eventually(async () => late.messages().length >= 1, { timeoutMs: 5000 });
  console.log(
    "late subscriber received:",
    late.messages().join(" "),
    "(the 3 offline messages are gone)",
  );

  console.log("== fan-out: 2 more subscribers, 5 messages ==");
  const others = [await newSubscriber(channel), await newSubscriber(channel)];
  await eventually(async () => (await numSubscribers(publisher, channel)) === 3, {
    timeoutMs: 5000,
  });
  for (let i = 0; i < 5; i++) {
    const receivers = await publish(publisher, channel, `fanout-${i}`);
    if (i === 0) console.log(`each PUBLISH reaches receivers = ${receivers}`);
  }
  for (const [i, subscriber] of [late, ...others].entries()) {
    // The late subscriber also holds "online-1", so wait for 5 fanout messages after it.
    const want = subscriber === late ? 6 : 5;
    await eventually(async () => subscriber.messages().length >= want, { timeoutMs: 5000 });
    console.log(`subscriber ${i + 1} received:`, subscriber.messages().join(" "));
  }
} finally {
  for (const connection of connections) connection.disconnect();
  publisher.disconnect();
}
