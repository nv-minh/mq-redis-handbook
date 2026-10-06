// Demo: Pub/Sub không có lịch sử (subscriber vào muộn bỏ lỡ message) và fan-out tới mọi subscriber.
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

  console.log("== offline: chưa ai subscribe ==");
  for (const message of ["offline-1", "offline-2", "offline-3"]) {
    console.log(
      `PUBLISH ${message} -> số receiver = ${await publish(publisher, channel, message)}`,
    );
  }

  console.log("== một subscriber vào muộn ==");
  const late = await newSubscriber(channel);
  console.log("PUBSUB NUMSUB =", await numSubscribers(publisher, channel));
  console.log(`PUBLISH online-1 -> số receiver = ${await publish(publisher, channel, "online-1")}`);
  await eventually(async () => late.messages().length >= 1, { timeoutMs: 5000 });
  console.log(
    "subscriber vào muộn đã nhận:",
    late.messages().join(" "),
    "(3 message offline đã mất)",
  );

  console.log("== fan-out: thêm 2 subscriber, 5 message ==");
  const others = [await newSubscriber(channel), await newSubscriber(channel)];
  await eventually(async () => (await numSubscribers(publisher, channel)) === 3, {
    timeoutMs: 5000,
  });
  for (let i = 0; i < 5; i++) {
    const receivers = await publish(publisher, channel, `fanout-${i}`);
    if (i === 0) console.log(`mỗi PUBLISH tới số receiver = ${receivers}`);
  }
  for (const [i, subscriber] of [late, ...others].entries()) {
    // Subscriber vào muộn còn giữ "online-1", nên chờ thêm 5 message fanout sau nó.
    const want = subscriber === late ? 6 : 5;
    await eventually(async () => subscriber.messages().length >= want, { timeoutMs: 5000 });
    console.log(`subscriber ${i + 1} đã nhận:`, subscriber.messages().join(" "));
  }
} finally {
  for (const connection of connections) connection.disconnect();
  publisher.disconnect();
}
