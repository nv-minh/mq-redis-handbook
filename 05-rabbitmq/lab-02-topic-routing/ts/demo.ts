// Demo: với mỗi topic pattern, publish cùng một bộ routing key và in ra key nào vào được queue.
// Sau đó cho thấy message không route được bị bỏ lặng lẽ khi không có mandatory, và bị trả về (312 NO_ROUTE) khi có mandatory.
import { eventually, uniqueName } from "@handbook/testkit";
import {
  ConfirmedPublisher,
  declareTopicRouting,
  openConnection,
  receiveRoutingKeys,
} from "./lab.js";

const connection = await openConnection();
const exchanges: string[] = [];
const queues: string[] = [];
const keys = [
  "orders",
  "orders.created",
  "orders.eu.created",
  "orders.eu.vn.created",
  "payments.created",
];

try {
  const channel = await connection.createConfirmChannel();
  const publisher = new ConfirmedPublisher(channel);
  console.log(`routing key được publish: ${keys.join(", ")}`);

  // Số key khớp mong đợi của mỗi pattern, để biết khi nào queue đã nhận đủ.
  const expected: Record<string, number> = {
    "orders.*.created": 1,
    "orders.#": 4,
    "orders.eu.created": 1,
  };
  for (const [pattern, matched] of Object.entries(expected)) {
    const name = uniqueName("demo-topic");
    exchanges.push(name);
    queues.push(`${name}.queue`);
    const { exchange, queue } = await declareTopicRouting(channel, name, pattern);
    for (const key of keys) await publisher.publish(exchange, key, key);
    // Confirm đã về, nhưng queue được cập nhật bất đồng bộ: poll tới khi queue có đủ số message mong đợi.
    await eventually(
      async () => (await channel.checkQueue(queue)).messageCount === matched || undefined,
      { timeoutMs: 10_000 },
    );
    const received = await receiveRoutingKeys(channel, queue);
    console.log(`binding ${pattern.padEnd(18)} nhận: ${received.join(", ") || "(không có)"}`);
  }
  console.log(
    "Kết luận: `*` khớp đúng một từ, `#` khớp không hoặc nhiều từ, binding không có wildcard khớp chính xác.",
  );

  const name = uniqueName("demo-unrouted");
  exchanges.push(name);
  queues.push(`${name}.queue`);
  const { exchange, queue } = await declareTopicRouting(channel, name, "orders.eu.created");
  console.log("--- publish payments.refund, không có binding nào khớp ---");
  const silent = await publisher.publish(exchange, "payments.refund", "x");
  console.log(
    `không mandatory: broker vẫn confirm, return=${silent === null ? "không có" : "có"}, queue có ${(await channel.checkQueue(queue)).messageCount} message`,
  );
  const returned = await publisher.publish(exchange, "payments.refund", "x", { mandatory: true });
  console.log(
    `mandatory=true : return ${returned?.replyCode} ${returned?.replyText} cho routing key ${returned?.routingKey}, đến trước confirm`,
  );
  console.log(
    "Kết luận: confirm không chứng minh message vào queue, muốn biết phải dùng mandatory và nghe basic.return.",
  );
} finally {
  const channel = await connection.createChannel();
  for (const queue of queues) await channel.deleteQueue(queue);
  for (const exchange of exchanges) await channel.deleteExchange(exchange);
  await connection.close();
}
