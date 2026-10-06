import { afterEach, describe, expect, it } from "vitest";
import type { ChannelModel, ConfirmChannel } from "amqplib";
import { eventually, uniqueName } from "@handbook/testkit";
import {
  ConfirmedPublisher,
  declareTopicRouting,
  openConnection,
  receiveRoutingKeys,
  type TopicRouting,
} from "./lab.js";

const connections: ChannelModel[] = [];
const exchanges: string[] = [];
const queues: string[] = [];

async function open(): Promise<ChannelModel> {
  const connection = await openConnection();
  connections.push(connection);
  return connection;
}

/** Dựng topic exchange và queue với một binding, đăng ký tên để afterEach xóa dù test fail. */
async function routing(channel: ConfirmChannel, pattern: string): Promise<TopicRouting> {
  const name = uniqueName("lab02-orders");
  // Đăng ký tên trước khi declare: xóa một exchange hoặc queue chưa tồn tại không phải lỗi.
  exchanges.push(name);
  queues.push(`${name}.queue`);
  return declareTopicRouting(channel, name, pattern);
}

/** Số message đang nằm trong queue. */
async function messageCount(connection: ChannelModel, queue: string): Promise<number> {
  const channel = await connection.createChannel();
  try {
    return (await channel.checkQueue(queue)).messageCount;
  } finally {
    await channel.close().catch(() => undefined);
  }
}

afterEach(async () => {
  await Promise.all(connections.splice(0).map((c) => c.close().catch(() => undefined)));
  if (exchanges.length === 0 && queues.length === 0) return;
  const cleaner = await openConnection();
  try {
    const channel = await cleaner.createChannel();
    for (const queue of queues.splice(0)) await channel.deleteQueue(queue);
    for (const exchange of exchanges.splice(0)) await channel.deleteExchange(exchange);
  } finally {
    await cleaner.close().catch(() => undefined);
  }
});

describe("lab-02 topic routing: wildcard và unroutable message", () => {
  it("orders_star_created_matches_one_word_only", async () => {
    const connection = await open();
    const channel = await connection.createConfirmChannel();
    const { exchange, queue } = await routing(channel, "orders.*.created");
    const publisher = new ConfirmedPublisher(channel);

    // `*` thay thế đúng một từ: chỉ key đầu tiên khớp.
    await publisher.publish(exchange, "orders.eu.created", "khớp: đúng một từ ở giữa");
    await publisher.publish(exchange, "orders.created", "không khớp: thiếu từ ở giữa");
    await publisher.publish(exchange, "orders.eu.vn.created", "không khớp: hai từ ở giữa");

    // Confirm chỉ cho biết broker đã xử lý, queue được cập nhật gần như đồng thời nên chờ bằng eventually.
    await eventually(async () => (await messageCount(connection, queue)) === 1 || undefined, {
      timeoutMs: 10_000,
    });
    expect(await receiveRoutingKeys(channel, queue)).toEqual(["orders.eu.created"]);
  });

  it("orders_hash_matches_zero_or_more_words", async () => {
    const connection = await open();
    const channel = await connection.createConfirmChannel();
    const { exchange, queue } = await routing(channel, "orders.#");
    const publisher = new ConfirmedPublisher(channel);

    // `#` thay thế không hoặc nhiều từ: khớp cả `orders` trơn, nhưng không khớp prefix khác.
    for (const key of ["orders", "orders.eu", "orders.eu.vn.created", "payments.created"]) {
      await publisher.publish(exchange, key, key);
    }

    await eventually(async () => (await messageCount(connection, queue)) === 3 || undefined, {
      timeoutMs: 10_000,
    });
    expect(await receiveRoutingKeys(channel, queue)).toEqual([
      "orders",
      "orders.eu",
      "orders.eu.vn.created",
    ]);
  });

  it("unrouted_message_is_dropped_without_mandatory_flag", async () => {
    const connection = await open();
    const channel = await connection.createConfirmChannel();
    const { exchange, queue } = await routing(channel, "orders.eu.created");
    const publisher = new ConfirmedPublisher(channel);

    // Không có binding nào khớp `payments.refund`, và không đặt mandatory.
    // Broker vẫn confirm (publish() chỉ resolve khi nhận confirm), nhưng message bị bỏ lặng lẽ.
    const silent = await publisher.publish(exchange, "payments.refund", "mất mà không ai biết");
    expect(silent).toBeNull();
    expect(publisher.returned).toHaveLength(0);
    // Confirm đã về nghĩa là broker đã route xong, nên queue chắc chắn không nhận được gì.
    expect(await messageCount(connection, queue)).toBe(0);

    // Tương phản: cùng một routing key nhưng mandatory=true thì broker trả message về trước khi confirm.
    const returned = await publisher.publish(exchange, "payments.refund", "được trả về", {
      mandatory: true,
    });
    expect(returned).not.toBeNull();
    expect(returned?.replyCode).toBe(312);
    expect(returned?.replyText).toBe("NO_ROUTE");
    expect(returned?.routingKey).toBe("payments.refund");
    expect(publisher.returned).toHaveLength(1);
    expect(await messageCount(connection, queue)).toBe(0);

    // mandatory=true mà route được thì không có return.
    expect(
      await publisher.publish(exchange, "orders.eu.created", "route được", { mandatory: true }),
    ).toBeNull();
    await eventually(async () => (await messageCount(connection, queue)) === 1 || undefined, {
      timeoutMs: 10_000,
    });
  });
});
